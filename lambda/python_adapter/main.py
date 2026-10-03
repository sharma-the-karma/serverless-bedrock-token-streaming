"""
Python Streaming API with Amazon Bedrock + FastAPI + AWS Lambda Web Adapter.

Uses an asyncio.Queue with a background worker thread to consume the blocking
boto3 EventStream without starving the FastAPI asyncio event loop under concurrency.
"""

import os
import json
import asyncio
import threading
from typing import AsyncGenerator, Optional, List
import boto3
from botocore.config import Config
from fastapi import FastAPI, HTTPException, Security, Depends
from fastapi.security.api_key import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

app = FastAPI(title="Bedrock Serverless Python Streamer")

# Restrict CORS to allowed origins (configured via environment variable in production)
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# Optional API key protection for production use
API_KEY_HEADER = APIKeyHeader(name="x-api-key", auto_error=False)
EXPECTED_API_KEY = os.getenv("APP_API_KEY")

def verify_api_key(api_key: Optional[str] = Depends(API_KEY_HEADER)):
    if EXPECTED_API_KEY and api_key != EXPECTED_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return api_key

# Configure Boto3 Bedrock Runtime Client with retries and connection reuse
boto_config = Config(
    retries={"max_attempts": 3, "mode": "standard"},
    connect_timeout=10,
    read_timeout=300,
)

bedrock_runtime = boto3.client(
    service_name="bedrock-runtime",
    region_name=os.getenv("AWS_REGION", "us-east-1"),
    config=boto_config,
)

# Use current cross-region inference profiles or Foundation Model IDs
DEFAULT_MODEL_ID = os.getenv(
    "BEDROCK_MODEL_ID",
    "us.anthropic.claude-3-7-sonnet-20250219-v1:0"
)

_STREAM_END = object()


class ChatMessage(BaseModel):
    role: str
    content: List[dict]


class ChatRequest(BaseModel):
    prompt: Optional[str] = None
    system: Optional[str] = "You are a concise, accurate AI assistant."
    modelId: Optional[str] = DEFAULT_MODEL_ID
    messages: Optional[List[ChatMessage]] = None
    temperature: Optional[float] = 0.7
    maxTokens: Optional[int] = 2048


async def stream_bedrock_events(request: ChatRequest) -> AsyncGenerator[str, None]:
    """
    Consumes the blocking boto3 EventStream on a background thread and yields SSE
    events asynchronously via an asyncio.Queue, avoiding event-loop starvation.
    """
    if request.messages and len(request.messages) > 0:
        messages = [m.model_dump() for m in request.messages]
    else:
        prompt_text = request.prompt or "Hello from Amazon Bedrock!"
        messages = [{"role": "user", "content": [{"text": prompt_text}]}]

    system_content = [{"text": request.system}] if request.system else None

    # Initial SSE event
    yield f"event: init\ndata: {json.dumps({'status': 'connected', 'modelId': request.modelId})}\n\n"

    queue: asyncio.Queue = asyncio.Queue()
    loop = asyncio.get_running_loop()

    def worker():
        try:
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

            response = bedrock_runtime.converse_stream(**kwargs)
            stream = response.get("stream")

            for event in stream:
                loop.call_soon_threadsafe(queue.put_nowait, event)

            loop.call_soon_threadsafe(queue.put_nowait, _STREAM_END)
        except Exception as e:
            loop.call_soon_threadsafe(queue.put_nowait, e)

    # Start stream consumer on background thread
    thread = threading.Thread(target=worker, daemon=True)
    thread.start()

    try:
        while True:
            item = await queue.get()
            if item is _STREAM_END:
                break
            if isinstance(item, Exception):
                error_payload = json.dumps({"error": True, "message": str(item)})
                yield f"event: error\ndata: {error_payload}\n\n"
                break

            event = item
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

    finally:
        pass


@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "Bedrock Python Streaming API"}


@app.post("/stream")
async def chat_stream(request: ChatRequest, _auth: Optional[str] = Depends(verify_api_key)):
    """
    Streams Server-Sent Events over HTTP.
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
