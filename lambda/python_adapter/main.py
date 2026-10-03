"""
Python Streaming API with Amazon Bedrock + FastAPI + AWS Lambda Web Adapter.

Uses an asyncio.Queue with a background worker thread to consume the blocking
boto3 EventStream without starving the FastAPI asyncio event loop under concurrency.
Includes server-side model allowlist, origin verification, and input size caps.
"""

import os
import json
import asyncio
import threading
from typing import AsyncGenerator, Optional, List
import boto3
from botocore.config import Config
from fastapi import FastAPI, HTTPException, Security, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

app = FastAPI(title="Bedrock Serverless Python Streamer")

ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "https://yourdomain.com").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

EXPECTED_ORIGIN_VERIFY = os.getenv("ORIGIN_VERIFY_SECRET")
EXPECTED_API_KEY = os.getenv("APP_API_KEY")

def verify_request_access(
    x_origin_verify: Optional[str] = Header(None, alias="x-origin-verify"),
    x_api_key: Optional[str] = Header(None, alias="x-api-key"),
):
    if EXPECTED_ORIGIN_VERIFY and x_origin_verify != EXPECTED_ORIGIN_VERIFY:
        raise HTTPException(status_code=403, detail="Forbidden: Direct Function URL access is blocked.")
    if EXPECTED_API_KEY and x_api_key != EXPECTED_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid x-api-key.")

# Server-side model allowlist
ALLOWED_MODELS = {
    "amazon.nova-pro-v1:0",
    "amazon.nova-lite-v1:0",
    "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
    "us.anthropic.claude-3-5-haiku-20241022-v1:0",
}

DEFAULT_MODEL_ID = os.getenv(
    "BEDROCK_MODEL_ID",
    "amazon.nova-pro-v1:0"
)

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

_STREAM_END = object()

class ChatMessage(BaseModel):
    role: str
    content: List[dict]

class ChatRequest(BaseModel):
    prompt: Optional[str] = Field(None, max_length=4000)
    system: Optional[str] = Field("You are a concise, accurate AI assistant.", max_length=1000)
    modelId: Optional[str] = DEFAULT_MODEL_ID
    messages: Optional[List[ChatMessage]] = None
    temperature: Optional[float] = Field(0.7, ge=0.0, le=1.0)
    maxTokens: Optional[int] = Field(2048, ge=1, le=4096)

async def stream_bedrock_events(request: ChatRequest) -> AsyncGenerator[str, None]:
    if request.modelId not in ALLOWED_MODELS:
        yield f"event: error\ndata: {json.dumps({'error': f'Model {request.modelId} not allowed.'})}\n\n"
        return

    if request.messages and len(request.messages) > 0:
        messages = [m.model_dump() for m in request.messages]
    else:
        prompt_text = request.prompt or "Explain distributed consensus in two sentences."
        messages = [{"role": "user", "content": [{"text": prompt_text}]}]

    system_content = [{"text": request.system}] if request.system else None

    yield f"event: init\ndata: {json.dumps({'status': 'connected', 'modelId': request.modelId})}\n\n"

    queue: asyncio.Queue = asyncio.Queue()
    loop = asyncio.get_running_loop()

    # Worker thread consumes blocking Boto3 socket stream to prevent asyncio loop starvation
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
            for event in response.get("stream"):
                loop.call_soon_threadsafe(queue.put_nowait, event)
            loop.call_soon_threadsafe(queue.put_nowait, _STREAM_END)
        except Exception as e:
            loop.call_soon_threadsafe(queue.put_nowait, e)

    threading.Thread(target=worker, daemon=True).start()

    while True:
        item = await queue.get()
        if item is _STREAM_END:
            break
        if isinstance(item, Exception):
            yield f"event: error\ndata: {json.dumps({'error': True, 'message': str(item)})}\n\n"
            break

        event = item
        if "contentBlockDelta" in event:
            delta = event["contentBlockDelta"]["delta"]
            if "text" in delta:
                yield f"event: delta\ndata: {json.dumps({'text': delta['text']})}\n\n"
        elif "messageStop" in event:
            yield f"event: done\ndata: {json.dumps({'stopReason': event['messageStop'].get('stopReason')})}\n\n"
        elif "metadata" in event:
            yield f"event: metadata\ndata: {json.dumps(event['metadata'])}\n\n"

@app.get("/health")
def health_check():
    return {"status": "healthy"}

@app.post("/stream")
async def chat_stream(request: ChatRequest, _auth: None = Depends(verify_request_access)):
    if request.modelId not in ALLOWED_MODELS:
        raise HTTPException(
            status_code=400,
            detail=f"Model '{request.modelId}' not in allowlist. Allowed: {list(ALLOWED_MODELS)}"
        )
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
    uvicorn.run("main:app", host="0.0.0.0", port=8080)
