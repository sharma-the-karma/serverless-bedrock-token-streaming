import {
  BedrockRuntimeClient,
  ConverseStreamCommand,
} from "@aws-sdk/client-bedrock-runtime";

// Initialize Bedrock client with connection reuse & default region
const bedrock = new BedrockRuntimeClient({
  region: process.env.AWS_REGION || "us-east-1",
});

const DEFAULT_MODEL_ID = process.env.BEDROCK_MODEL_ID || "anthropic.claude-3-5-sonnet-20241022-v2:0";

/**
 * AWS Lambda Response Streaming Handler using awslambda.streamifyResponse
 *
 * Requirements for streaming to work:
 * 1. Lambda Function URL configured with InvokeMode: RESPONSE_STREAM
 * 2. Node.js 18.x+ or 20.x+ runtime
 * 3. Headers: Content-Type: text/event-stream, Cache-Control: no-cache
 */
export const handler = awslambda.streamifyResponse(
  async (event, responseStream, context) => {
    // 1. Set SSE (Server-Sent Events) and anti-buffering response headers
    const metadata = {
      statusCode: 200,
      headers: {
        "Content-Type": "text/event-stream; charset=utf-8",
        "Cache-Control": "no-cache, no-transform",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no", // Disables Nginx/proxy buffering
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization",
      },
    };

    // Pre-flight CORS handling
    if (event.requestContext?.http?.method === "OPTIONS" || event.httpMethod === "OPTIONS") {
      const corsResponse = awslambda.HttpResponseStream.from(responseStream, {
        statusCode: 204,
        headers: metadata.headers,
      });
      corsResponse.end();
      return;
    }

    const stream = awslambda.HttpResponseStream.from(responseStream, metadata);

    // Helper to send formatted SSE messages
    const sendSSE = (eventData, eventType = "message") => {
      const payload = typeof eventData === "string" ? eventData : JSON.stringify(eventData);
      stream.write(`event: ${eventType}\ndata: ${payload}\n\n`);
    };

    try {
      // 2. Parse request payload (supports POST JSON or GET query string)
      let prompt = "Explain quantum computing in 3 simple sentences.";
      let systemPrompt = "You are a helpful, concise AI technical assistant.";
      let modelId = DEFAULT_MODEL_ID;
      let conversationHistory = [];

      if (event.body) {
        const body = typeof event.body === "string" ? JSON.parse(event.body) : event.body;
        if (body.prompt) prompt = body.prompt;
        if (body.system) systemPrompt = body.system;
        if (body.modelId) modelId = body.modelId;
        if (Array.isArray(body.messages)) conversationHistory = body.messages;
      } else if (event.queryStringParameters?.prompt) {
        prompt = event.queryStringParameters.prompt;
        if (event.queryStringParameters.modelId) modelId = event.queryStringParameters.modelId;
      }

      // Build messages array conforming to Bedrock Converse API standard
      const messages = conversationHistory.length > 0
        ? conversationHistory
        : [{ role: "user", content: [{ text: prompt }] }];

      sendSSE({ status: "connected", modelId }, "init");

      // 3. Invoke Amazon Bedrock ConverseStream Command
      const command = new ConverseStreamCommand({
        modelId,
        messages,
        system: systemPrompt ? [{ text: systemPrompt }] : undefined,
        inferenceConfig: {
          maxTokens: 2048,
          temperature: 0.7,
          topP: 0.9,
        },
      });

      const bedrockResponse = await bedrock.send(command);

      // 4. Stream response chunks token-by-token
      let fullResponseText = "";
      let tokenUsage = null;
      const startTime = Date.now();
      let firstTokenTime = null;

      for await (const chunk of bedrockResponse.stream) {
        // Text delta streaming chunk
        if (chunk.contentBlockDelta?.delta?.text) {
          if (!firstTokenTime) {
            firstTokenTime = Date.now();
            sendSSE({ ttftMs: firstTokenTime - startTime }, "metric_ttft");
          }

          const textChunk = chunk.contentBlockDelta.delta.text;
          fullResponseText += textChunk;

          sendSSE({
            text: textChunk,
            index: chunk.contentBlockDelta.contentBlockIndex,
          }, "delta");
        }

        // Metrics & token usage metadata emitted at conclusion of stream
        if (chunk.metadata?.usage) {
          tokenUsage = chunk.metadata.usage;
        }

        // Message stop / finish reason
        if (chunk.messageStop) {
          sendSSE({
            stopReason: chunk.messageStop.stopReason,
            metrics: {
              totalDurationMs: Date.now() - startTime,
              ttftMs: firstTokenTime ? firstTokenTime - startTime : null,
              usage: tokenUsage,
            },
          }, "done");
        }
      }
    } catch (err) {
      console.error("Streaming execution failed:", err);
      sendSSE(
        {
          error: true,
          message: err.message || "Internal Bedrock streaming error occurred.",
          name: err.name,
        },
        "error"
      );
    } finally {
      // Gracefully terminate the HTTP response stream
      stream.end();
    }
  }
);
