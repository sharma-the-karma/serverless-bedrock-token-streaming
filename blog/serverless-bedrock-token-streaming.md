---
title: "Real-Time Token Streaming with Amazon Bedrock and AWS Lambda: Architecture, Tradeoffs, and Security"
published: true
description: "A technical guide to streaming Bedrock tokens using Lambda Function URLs in RESPONSE_STREAM mode, addressing API Gateway tradeoffs, Python asyncio concurrency, and production security."
tags: aws, serverless, bedrock, architecture
cover_image: https://raw.githubusercontent.com/sharma-the-karma/serverless-bedrock-token-streaming/main/assets/cover.png
canonical_url: https://dev.to/sharmavarun/solving-aws-reposts-1-genai-headache-real-time-token-streaming-with-amazon-bedrock-aws-lambda-37kh
---

When building interactive generative AI applications on AWS, managing Time-To-First-Token (TTFT) is critical. While Amazon Bedrock natively supports streaming token responses via its `ConverseStream` API, exposing this stream to client browsers over serverless infrastructure requires choosing the right architectural path.

In this guide, we examine the mechanics of serverless token streaming on AWS, compare **API Gateway REST APIs** with **Lambda Function URLs in `RESPONSE_STREAM` mode**, resolve a common **Python asyncio event loop blocking bug**, and address the **security and billing safeguards** needed before exposing model endpoints.

---

## 1. The Architectural Landscape: Function URLs vs. API Gateway

When serving streaming LLM responses, AWS offers two primary serverless patterns:

### Option A: Amazon API Gateway REST APIs (Response Transfer Mode: `STREAM`)
AWS introduced native response streaming for API Gateway REST APIs, allowing integrations to stream response payloads without buffering.
* **Pros:** Full access to API Gateway's mature operational features (API keys, usage plans, request validation, Cognito/Lambda authorizers).
* **Cons:** Requires configuring method response transfer modes (`STREAM`), incurs API Gateway invocation charges ($3.50 per million requests), and does not apply to API Gateway HTTP APIs (which still buffer responses).

### Option B: AWS Lambda Function URLs (`InvokeMode: RESPONSE_STREAM`)
Lambda Function URLs provide a direct HTTPS endpoint backed by HTTP chunked transfer encoding.
* **Pros:** Zero API Gateway provisioning overhead, zero per-request proxy fees, native chunked streaming, and direct support up to Lambda's 15-minute execution limit.
* **Cons:** Fewer built-in API management features; authentication must be managed via IAM (SigV4), CloudFront + WAF, or an application-layer secret.

```mermaid
flowchart TD
    Client[Web Browser: fetch ReadableStream] -->|SSE Stream| CF[Amazon CloudFront CDN]
    CF -->|Chunked HTTP Transfer| FURL[Lambda Function URL: RESPONSE_STREAM]
    FURL -->|EventStream| Bedrock[Amazon Bedrock ConverseStream]
```

---

## 2. Latency & TTFT: Measured Behavior

To illustrate the user experience impact, consider a model generating a 500-token analytical response:

* **Buffered Response:** The client receives zero bytes until generation is complete. The Time-To-First-Token equals the full generation duration (typically 5 to 9 seconds depending on model throughput).
* **Streaming Response:** The client receives the initial token chunk as soon as the model finishes prefill processing and begins generation (typically 250ms to 400ms). The user can start reading immediately while subsequent tokens stream in over Server-Sent Events (SSE).

---

## 3. Node.js 20+ Implementation

AWS Lambda supports native response streaming in Node.js via `awslambda.streamifyResponse()` and `awslambda.HttpResponseStream`.

```javascript
// lambda/index.mjs
import {
  BedrockRuntimeClient,
  ConverseStreamCommand,
} from "@aws-sdk/client-bedrock-runtime";

const bedrock = new BedrockRuntimeClient({
  region: process.env.AWS_REGION || "us-east-1",
});

const DEFAULT_MODEL_ID = process.env.BEDROCK_MODEL_ID || "us.anthropic.claude-3-7-sonnet-20250219-v1:0";
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || "https://yourdomain.com";

export const handler = awslambda.streamifyResponse(
  async (event, responseStream, context) => {
    const headers = {
      "Content-Type": "text/event-stream; charset=utf-8",
      "Cache-Control": "no-cache, no-transform",
      "Connection": "keep-alive",
      "X-Accel-Buffering": "no",
      "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
      "Access-Control-Allow-Methods": "POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization, x-api-key",
    };

    // Pre-flight CORS handling
    if (event.requestContext?.http?.method === "OPTIONS") {
      const corsResponse = awslambda.HttpResponseStream.from(responseStream, {
        statusCode: 204,
        headers,
      });
      corsResponse.end();
      return;
    }

    const stream = awslambda.HttpResponseStream.from(responseStream, {
      statusCode: 200,
      headers,
    });

    const sendSSE = (data, eventType = "message") => {
      const payload = typeof data === "string" ? data : JSON.stringify(data);
      stream.write(`event: ${eventType}\ndata: ${payload}\n\n`);
    };

    try {
      const body = event.body ? JSON.parse(event.body) : {};
      const prompt = body.prompt || "Explain distributed consensus in two sentences.";
      const modelId = body.modelId || DEFAULT_MODEL_ID;

      sendSSE({ status: "connected", modelId }, "init");

      const command = new ConverseStreamCommand({
        modelId,
        messages: [{ role: "user", content: [{ text: prompt }] }],
        inferenceConfig: { maxTokens: 2048, temperature: 0.7 },
      });

      const response = await bedrock.send(command);

      for await (const chunk of response.stream) {
        if (chunk.contentBlockDelta?.delta?.text) {
          sendSSE({ text: chunk.contentBlockDelta.delta.text }, "delta");
        }
        if (chunk.messageStop) {
          sendSSE({ stopReason: chunk.messageStop.stopReason }, "done");
        }
      }
    } catch (err) {
      console.error("Bedrock stream error:", err);
      sendSSE({ error: true, message: err.message }, "error");
    } finally {
      stream.end();
    }
  }
);
```

---

## 4. Python Concurrency Gotcha: Non-Blocking Stream Consumption

A common pitfall when building Python streaming services with FastAPI and Boto3 is how the Bedrock stream is consumed:

```python
# WARNING: Flawed Pattern
response = await loop.run_in_executor(None, lambda: bedrock.converse_stream(...))
for event in response.get("stream"):  # BLOCKS the asyncio event loop!
    await asyncio.sleep(0)            # Does NOT fix synchronous socket reads
    yield event
```

`response.get("stream")` is a `botocore.eventstream.EventStream`. Its iterator performs synchronous, blocking network socket reads. Iterating over it directly inside an `async def` generator blocks the entire asyncio event loop thread while waiting for the next token, causing concurrent requests to stall.

### The Correct Pattern: Producer-Consumer via `asyncio.Queue`

Offload the synchronous stream iteration to a background worker thread, pushing events into an `asyncio.Queue`:

```python
# lambda/python_adapter/main.py
import os
import json
import asyncio
import threading
from typing import AsyncGenerator
import boto3
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

app = FastAPI()
bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")
_STREAM_END = object()

class ChatRequest(BaseModel):
    prompt: str
    modelId: str = "us.anthropic.claude-3-7-sonnet-20250219-v1:0"

async def stream_bedrock_events(request: ChatRequest) -> AsyncGenerator[str, None]:
    queue = asyncio.Queue()
    loop = asyncio.get_running_loop()

    def worker():
        try:
            response = bedrock.converse_stream(
                modelId=request.modelId,
                messages=[{"role": "user", "content": [{"text": request.prompt}]}],
            )
            for event in response.get("stream"):
                loop.call_soon_threadsafe(queue.put_nowait, event)
            loop.call_soon_threadsafe(queue.put_nowait, _STREAM_END)
        except Exception as e:
            loop.call_soon_threadsafe(queue.put_nowait, e)

    # Run blocking I/O on a dedicated thread
    threading.Thread(target=worker, daemon=True).start()

    yield f"event: init\ndata: {json.dumps({'status': 'connected'})}\n\n"

    while True:
        item = await queue.get()
        if item is _STREAM_END:
            break
        if isinstance(item, Exception):
            yield f"event: error\ndata: {json.dumps({'error': str(item)})}\n\n"
            break

        if "contentBlockDelta" in item:
            text = item["contentBlockDelta"]["delta"]["text"]
            yield f"event: delta\ndata: {json.dumps({'text': text})}\n\n"
        elif "messageStop" in item:
            yield f"event: done\ndata: {json.dumps({'status': 'complete'})}\n\n"

@app.post("/stream")
async def chat_endpoint(req: ChatRequest):
    return StreamingResponse(
        stream_bedrock_events(req),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
```

Pair this with the **AWS Lambda Web Adapter** (`AWS_LWA_INVOKE_MODE=response_stream`) in a container image to run standard FastAPI on Lambda Function URLs.

---

## 5. Security & Billing Safeguards: Critical Checklist

Exposing a Lambda Function URL that calls Amazon Bedrock requires explicit access control. Deploying with `AuthType: NONE`, wildcard CORS (`*`), and unrestricted IAM permissions creates severe financial exposure—anyone who discovers the URL can invoke the model and drive up AWS charges.

### Production Hardening Steps:

1. **Use `AuthType: AWS_IAM`:** Require clients to sign requests using AWS Signature Version 4 (SigV4). If frontends cannot sign requests directly, route through CloudFront with Lambda@Edge / CloudFront Functions to sign requests or validate JSON Web Tokens (JWTs).
2. **Scope IAM Execution Roles:** Never grant `Resource: "*"`. Restrict the Lambda execution policy to the specific foundation model ARNs and regional inference profile ARNs required:
   ```yaml
   - Effect: Allow
     Action:
       - bedrock:InvokeModelWithResponseStream
       - bedrock:ConverseStream
     Resource:
       - "arn:aws:bedrock:*::foundation-model/*"
       - !Sub "arn:aws:bedrock:${AWS::Region}:${AWS::AccountId}:inference-profile/*"
   ```
3. **Restrict CORS:** Explicitly whitelist your domain in `AllowOrigins`. Wildcard CORS (`*`) allows unauthorized web origins to make cross-site calls to your endpoint.
4. **Deploy CloudFront with AWS WAF:** Place AWS WAF in front of CloudFront to enforce rate limits, geo-restrictions, and bot control. Validate a shared secret header (`X-Origin-Verify`) at the Lambda layer so requests cannot bypass CloudFront.
5. **Enforce a Server-Side Model Allowlist:** Never allow clients to pass arbitrary `modelId` values. Validate incoming model IDs against a strict server-side allowlist to prevent callers from invoking unexpected or high-cost models.
6. **Input Validation & Payload Guards:** Validate prompt character lengths (e.g., max 4,000 characters) and request body sizes (e.g., max 50 KB) before dispatching to Bedrock.

### Response Streaming Bandwidth and Cost Notes
* **Bandwidth Behavior:** AWS Lambda response streaming delivers an initial 6 MB unthrottled burst, after which subsequent throughput is capped at 2 MB/s (16 Mbps), up to a maximum payload size of 200 MB. For text-based LLM token streaming, this bandwidth ceiling is far higher than the generation throughput of current foundation models.
* **Billing Mechanics:** While Function URLs avoid API Gateway's $3.50 per million request charge, standard Lambda execution duration (billed in 1ms increments), memory allocation, and AWS Data Transfer Out still apply.

---

## 6. CloudFront CDN Configuration

When routing CloudFront to a streaming Lambda Function URL, two AWS-managed policies are required:

1. **`CachePolicyId: 4135ea2d-6df8-44a3-9df3-44ca84e08fad` (CachingDisabled):** Ensures CloudFront forwards chunks immediately without buffering.
2. **`OriginRequestPolicyId: b689b0a8-53d0-40ab-baf2-68738e2966ac` (AllViewerExceptHostHeader):** Strips the client's `Host` header and replaces it with the Lambda Function URL domain. Without this policy, the Lambda service rejects incoming requests because the `Host` header does not match the origin.

---

## 7. Client-Side Consumption via `fetch()`

Instead of `EventSource` (which only supports GET requests), consume the SSE stream using modern `fetch()` with `ReadableStreamDefaultReader`:

```javascript
async function streamChat(prompt, onToken) {
  const response = await fetch("https://your-lambda-url.on.aws/", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ prompt }),
  });

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop(); // Retain incomplete line

    for (const line of lines) {
      if (line.startsWith("data:")) {
        try {
          const payload = json.parse(line.replace("data:", "").trim());
          if (payload.text) {
            onToken(payload.text);
          }
        } catch {
          // Ignore heartbeats or partial JSON
        }
      }
    }
  }
}
```

---

## Conclusion & Code Repository

Serverless token streaming with Amazon Bedrock provides an excellent balance between low latency and cost efficiency. Whether you choose API Gateway REST APIs in `STREAM` mode or direct Lambda Function URLs, ensuring non-blocking event loop iteration in Python and locking down authentication and CORS are essential for production readiness.

The complete code, SAM template, and CDK stack are open-source and available here:

🔗 **GitHub Repository:** [sharma-the-karma/serverless-bedrock-token-streaming](https://github.com/sharma-the-karma/serverless-bedrock-token-streaming)
