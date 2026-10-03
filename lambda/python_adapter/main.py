"""
Python Streaming API with Amazon Bedrock + FastAPI + AWS Lambda Web Adapter.

Uses an asyncio.Queue with a background worker thread to consume the blocking
boto3 EventStream without starving the FastAPI asyncio event loop under concurrency.
Handles client disconnects cleanly via threading.Event to prevent unneeded token drain.
"""

import os
import json
import asyncio
import secrets
import threading
from typing import AsyncGenerator, Optional, List
import boto3
from botocore.config import Config
from fastapi import FastAPI, HTTPException, Request, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

app = FastAPI(title="Bedrock Serverless Python Streamer")

# Support both singular ALLOWED_ORIGIN and plural ALLOWED_ORIGINS
raw_origin = os.getenv("ALLOWED_ORIGIN") or os.getenv("ALLOWED_ORIGINS") or "https://yourdomain.com"
ALLOWED_ORIGINS = [o.strip() for o in raw_origin.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

EXPECTED_ORIGIN_VERIFY = os.getenv("ORIGIN_VERIFY_SECRET")
EXPECTED_API_KEY = os.getenv("APP_API_KEY")

def safe_compare(val: Optional[str], expected: Optional[str]) -> bool:
    if not val or not expected:
        return False
    return secrets.compare_digest(val.strip(), expected.strip())

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

async def stream_bedrock_events(request: ChatRequest, http_req: Request) -> AsyncGenerator[str, None]:
    if request.modelId not in ALLOWED_MODELS:
        yield f"event: error\ndata: {json.dumps({'error': f'Model {request.modelId} not allowed.'})}\n\n"
        return

    # Validate total character count across messages array or prompt
    total_chars = 0
    if request.messages and len(request.messages) > 0:
        messages = [m.model_dump() for m in request.messages]
        for m in messages:
            for part in m.get("content", []):
                if "text" in part:
                    total_chars += len(str(part["text"]))
    else:
        prompt_text = request.prompt or "Explain distributed consensus in two sentences."
        messages = [{"role": "user", "content": [{"text": prompt_text}]}]
        total_chars = len(prompt_text)

    if total_chars > 4000:
        yield f"event: error\ndata: {json.dumps({'error': f'Total input text ({total_chars} chars) exceeds 4,000 character limit.'})}\n\n"
        return

    if request.system and len(request.system) > 1000:
        yield f"event: error\ndata: {json.dumps({'error': 'System prompt exceeds maximum allowed length of 1,000 characters.'})}\n\n"
        return

    system_content = [{"text": request.system}] if request.system else None

    yield f"event: init\ndata: {json.dumps({'status': 'connected', 'modelId': request.modelId})}\n\n"

    queue: asyncio.Queue = asyncio.Queue()
    loop = asyncio.get_running_loop()
    stop_event = threading.Event()

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
                if stop_event.is_set():
                    break
                loop.call_soon_threadsafe(queue.put_nowait, event)
            loop.call_soon_threadsafe(queue.put_nowait, _STREAM_END)
        except Exception as e:
            loop.call_soon_threadsafe(queue.put_nowait, e)

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()

    stop_reason = None
    token_usage = None

    try:
        while True:
            # Check client disconnection periodically
            if await http_req.is_disconnected():
                stop_event.set()
                break

            try:
                item = await asyncio.wait_for(queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue

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
                stop_reason = event["messageStop"].get("stopReason")
            elif "metadata" in event:
                token_usage = event["metadata"].get("usage")

        if not stop_event.is_set():
            yield f"event: done\ndata: {json.dumps({'stopReason': stop_reason, 'usage': token_usage})}\n\n"
    finally:
        # Signal worker thread to stop draining stream if client disconnects early
        stop_event.set()


@app.get("/health")
def health_check():
    return {"status": "healthy"}


@app.post("/stream")
async def chat_stream(
    request: ChatRequest,
    http_req: Request,
    x_origin_verify: Optional[str] = Header(None, alias="x-origin-verify"),
    x_api_key: Optional[str] = Header(None, alias="x-api-key"),
):
    if EXPECTED_ORIGIN_VERIFY and not safe_compare(x_origin_verify, EXPECTED_ORIGIN_VERIFY):
        raise HTTPException(status_code=403, detail="Forbidden: Direct Function URL access is blocked.")
    if EXPECTED_API_KEY and not safe_compare(x_api_key, EXPECTED_API_KEY):
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid x-api-key.")

    if request.modelId not in ALLOWED_MODELS:
        raise HTTPException(
            status_code=400,
            detail=f"Model '{request.modelId}' not in allowlist. Allowed: {list(ALLOWED_MODELS)}"
        )

    return StreamingResponse(
        stream_bedrock_events(request, http_req),
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
