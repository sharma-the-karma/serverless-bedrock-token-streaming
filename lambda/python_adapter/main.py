"""
Production Python Streaming API with Amazon Bedrock + FastAPI + AWS Lambda Web Adapter.

Why this solves the AWS re:Post dilemma:
AWS Lambda doesn't have native streamifyResponse() for Python, but AWS Lambda Web Adapter
(LWA) translates HTTP streaming directly into Lambda Function URL Response Streaming!
"""

import os
import json
import asyncio
from typing import AsyncGenerator, Optional, List
import boto3
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

app = FastAPI(title="Bedrock Serverless Python Streamer")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Bedrock client
bedrock_runtime = boto3.client(
    service_name="bedrock-runtime",
    region_name=os.getenv("AWS_REGION", "us-east-1")
)

DEFAULT_MODEL_ID = os.getenv("BEDROCK_MODEL_ID", "anthropic.claude-3-5-sonnet-20241022-v2:0")


class ChatMessage(BaseModel):
    role: str
    content: List[dict]


class ChatRequest(BaseModel):
    prompt: Optional[str] = None
    system: Optional[str] = "You are a fast, concise AI assistant."
    modelId: Optional[str] = DEFAULT_MODEL_ID
    messages: Optional[List[ChatMessage]] = None
    temperature: Optional[float] = 0.7
    maxTokens: Optional[int] = 2048


async def stream_bedrock_events(request: ChatRequest) -> AsyncGenerator[str, None]:
    """
    Generator yielding Server-Sent Events (SSE) formatted text chunks
    from Bedrock converse_stream API.
    """
    # Build converse messages format
    if request.messages and len(request.messages) > 0:
        messages = [m.model_dump() for m in request.messages]
    else:
        prompt_text = request.prompt or "Hello from AWS re:Post community!"
        messages = [{"role": "user", "content": [{"text": prompt_text}]}]

    system_content = [{"text": request.system}] if request.system else None

    # Initial SSE event
    yield f"event: init\ndata: {json.dumps({'status': 'connected', 'modelId': request.modelId})}\n\n"

    try:
        # Run blocking boto3 Bedrock call in thread pool to preserve async loop responsiveness
        loop = asyncio.get_event_loop()

        def invoke():
            kwargs = {
                "modelId": request.modelId,
                "messages": messages,
                "inferenceConfig": {
                    "maxTokens": request.maxTokens,
                    "temperature": request.temperature,
                }
            }
            if system_content:
                kwargs["system"] = system_content
            return bedrock_runtime.converse_stream(**kwargs)

        response = await loop.run_in_executor(None, invoke)
        stream = response.get("stream")

        for event in stream:
            # Yield control to event loop
            await asyncio.sleep(0)

            if "contentBlockDelta" in event:
                delta = event["contentBlockDelta"]["delta"]
                if "text" in delta:
                    payload = json.dumps({"text": delta["text"]})
                    yield f"event: delta\ndata: {payload}\n\n"

            elif "messageStop" in event:
                stop_info = event["messageStop"]
                payload = json.dumps({"stopReason": stop_info.get("stopReason")})
                yield f"event: done\ndata: {payload}\n\n"

            elif "metadata" in event:
                usage = event["metadata"].get("usage", {})
                metrics = event["metadata"].get("metrics", {})
                payload = json.dumps({"usage": usage, "metrics": metrics})
                yield f"event: metadata\ndata: {payload}\n\n"

    except Exception as e:
        error_payload = json.dumps({"error": True, "message": str(e)})
        yield f"event: error\ndata: {error_payload}\n\n"


@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "Bedrock Python Streaming API"}


@app.post("/stream")
async def chat_stream(request: ChatRequest):
    """
    Main endpoint for token-by-token streaming.
    Streams Server-Sent Events directly over HTTP.
    """
    return StreamingResponse(
        stream_bedrock_events(request),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)
