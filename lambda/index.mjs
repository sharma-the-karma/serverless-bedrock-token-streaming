import crypto from "node:crypto";
import {
  BedrockRuntimeClient,
  ConverseStreamCommand,
} from "@aws-sdk/client-bedrock-runtime";

const bedrock = new BedrockRuntimeClient({
  region: process.env.AWS_REGION || "us-east-1",
});

const ALLOWED_MODELS = new Set([
  "amazon.nova-pro-v1:0",
  "amazon.nova-lite-v1:0",
  "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
  "us.anthropic.claude-3-5-haiku-20241022-v1:0",
]);

const DEFAULT_MODEL_ID = process.env.BEDROCK_MODEL_ID || "amazon.nova-pro-v1:0";
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || "https://yourdomain.com";
const EXPECTED_ORIGIN_VERIFY = process.env.ORIGIN_VERIFY_SECRET;
const EXPECTED_API_KEY = process.env.APP_API_KEY;

const MAX_TOTAL_PROMPT_CHARS = 4000;
const MAX_SYSTEM_CHARS = 1000;
const MAX_BODY_BYTES = 50 * 1024; // 50 KB

function safeCompare(a, b) {
  if (typeof a !== "string" || typeof b !== "string") return false;
  const bufA = Buffer.from(a);
  const bufB = Buffer.from(b);
  if (bufA.length !== bufB.length) return false;
  return crypto.timingSafeEqual(bufA, bufB);
}

export const handler = awslambda.streamifyResponse(
  async (event, responseStream, context) => {
    const headers = {
      "Content-Type": "text/event-stream; charset=utf-8",
      "Cache-Control": "no-cache, no-transform",
      "Connection": "keep-alive",
      "X-Accel-Buffering": "no",
      "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
      "Access-Control-Allow-Methods": "POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization, x-api-key, x-origin-verify",
    };

    // Pre-flight CORS handling
    const isOptions = event.requestContext?.http?.method === "OPTIONS" || event.httpMethod === "OPTIONS";
    if (isOptions) {
      const corsResponse = awslambda.HttpResponseStream.from(responseStream, {
        statusCode: 204,
        headers,
      });
      corsResponse.end();
      return;
    }

    // Origin verification guard: Ensures requests route through CloudFront
    if (EXPECTED_ORIGIN_VERIFY) {
      const originHeader = event.headers?.["x-origin-verify"] || event.headers?.["X-Origin-Verify"];
      if (!safeCompare(originHeader, EXPECTED_ORIGIN_VERIFY)) {
        const forbiddenResponse = awslambda.HttpResponseStream.from(responseStream, {
          statusCode: 403,
          headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": ALLOWED_ORIGIN },
        });
        forbiddenResponse.write(JSON.stringify({ error: "Forbidden: Direct Function URL access is blocked." }));
        forbiddenResponse.end();
        return;
      }
    }

    // Constant-time application-layer API key validation when configured
    if (EXPECTED_API_KEY) {
      const requestApiKey = event.headers?.["x-api-key"] || event.headers?.["X-Api-Key"];
      if (!safeCompare(requestApiKey, EXPECTED_API_KEY)) {
        const unauthorizedResponse = awslambda.HttpResponseStream.from(responseStream, {
          statusCode: 401,
          headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": ALLOWED_ORIGIN },
        });
        unauthorizedResponse.write(JSON.stringify({ error: "Unauthorized: Invalid x-api-key." }));
        unauthorizedResponse.end();
        return;
      }
    }

    // Payload size guard
    const rawBody = event.body || "";
    if (Buffer.byteLength(rawBody, "utf8") > MAX_BODY_BYTES) {
      const sizeErrResponse = awslambda.HttpResponseStream.from(responseStream, {
        statusCode: 413,
        headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": ALLOWED_ORIGIN },
      });
      sizeErrResponse.write(JSON.stringify({ error: "Payload too large. Maximum allowed is 50 KB." }));
      sizeErrResponse.end();
      return;
    }

    const stream = awslambda.HttpResponseStream.from(responseStream, {
      statusCode: 200,
      headers,
    });

    const sendSSE = (eventData, eventType = "message") => {
      const payload = typeof eventData === "string" ? eventData : JSON.stringify(eventData);
      stream.write(`event: ${eventType}\ndata: ${payload}\n\n`);
    };

    try {
      let prompt = "Explain distributed consensus in two sentences.";
      let systemPrompt = "You are a concise technical assistant.";
      let modelId = DEFAULT_MODEL_ID;
      let conversationHistory = [];

      if (event.body) {
        let body;
        try {
          body = typeof event.body === "string" ? JSON.parse(event.body) : event.body;
        } catch {
          sendSSE({ error: true, message: "Invalid JSON request body." }, "error");
          stream.end();
          return;
        }

        if (body.prompt) prompt = String(body.prompt);
        if (body.system) systemPrompt = String(body.system);
        if (body.modelId) modelId = String(body.modelId);
        if (Array.isArray(body.messages)) conversationHistory = body.messages;
      } else if (event.queryStringParameters?.prompt) {
        prompt = String(event.queryStringParameters.prompt);
        if (event.queryStringParameters.modelId) modelId = String(event.queryStringParameters.modelId);
      }

      // Input validation: System prompt length cap
      if (systemPrompt.length > MAX_SYSTEM_CHARS) {
        sendSSE(
          { error: true, message: `System prompt exceeds maximum allowed length of ${MAX_SYSTEM_CHARS} characters.` },
          "error"
        );
        stream.end();
        return;
      }

      // Input validation: Calculate total characters across prompt or messages array
      let totalInputChars = 0;
      if (conversationHistory.length > 0) {
        for (const msg of conversationHistory) {
          if (Array.isArray(msg.content)) {
            for (const part of msg.content) {
              if (part.text) totalInputChars += String(part.text).length;
            }
          }
        }
      } else {
        totalInputChars = prompt.length;
      }

      if (totalInputChars > MAX_TOTAL_PROMPT_CHARS) {
        sendSSE(
          { error: true, message: `Total input text (${totalInputChars} chars) exceeds maximum allowed length of ${MAX_TOTAL_PROMPT_CHARS} characters.` },
          "error"
        );
        stream.end();
        return;
      }

      // Security check: Server-side model allowlist
      if (!ALLOWED_MODELS.has(modelId)) {
        sendSSE(
          {
            error: true,
            message: `Model '${modelId}' is not permitted. Allowed models: ${Array.from(ALLOWED_MODELS).join(", ")}`,
          },
          "error"
        );
        stream.end();
        return;
      }

      const messages = conversationHistory.length > 0
        ? conversationHistory
        : [{ role: "user", content: [{ text: prompt }] }];

      sendSSE({ status: "connected", modelId }, "init");

      const command = new ConverseStreamCommand({
        modelId,
        messages,
        system: systemPrompt ? [{ text: systemPrompt }] : undefined,
        inferenceConfig: {
          maxTokens: 2048,
          temperature: 0.7,
        },
      });

      const bedrockResponse = await bedrock.send(command);

      let stopReason = null;
      let tokenUsage = null;

      for await (const chunk of bedrockResponse.stream) {
        if (chunk.contentBlockDelta?.delta?.text) {
          sendSSE({
            text: chunk.contentBlockDelta.delta.text,
            index: chunk.contentBlockDelta.contentBlockIndex,
          }, "delta");
        }

        if (chunk.messageStop) {
          stopReason = chunk.messageStop.stopReason;
        }

        if (chunk.metadata?.usage) {
          tokenUsage = chunk.metadata.usage;
        }
      }

      // Send completion event with both stopReason and usage metrics populated
      sendSSE({
        stopReason,
        usage: tokenUsage,
      }, "done");
    } catch (err) {
      console.error("Bedrock stream error:", err);
      sendSSE(
        {
          error: true,
          message: err.message || "Internal Bedrock streaming error.",
          name: err.name,
        },
        "error"
      );
    } finally {
      stream.end();
    }
  }
);
