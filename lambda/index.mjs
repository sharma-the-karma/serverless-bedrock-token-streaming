import {
  BedrockRuntimeClient,
  ConverseStreamCommand,
} from "@aws-sdk/client-bedrock-runtime";

const bedrock = new BedrockRuntimeClient({
  region: process.env.AWS_REGION || "us-east-1",
});

const DEFAULT_MODEL_ID = process.env.BEDROCK_MODEL_ID || "us.anthropic.claude-3-7-sonnet-20250219-v1:0";
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || "*";
const EXPECTED_API_KEY = process.env.APP_API_KEY;

export const handler = awslambda.streamifyResponse(
  async (event, responseStream, context) => {
    const headers = {
      "Content-Type": "text/event-stream; charset=utf-8",
      "Cache-Control": "no-cache, no-transform",
      "Connection": "keep-alive",
      "X-Accel-Buffering": "no",
      "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization, x-api-key",
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

    // Optional API key validation when configured
    const requestApiKey = event.headers?.["x-api-key"] || event.headers?.["X-Api-Key"];
    if (EXPECTED_API_KEY && requestApiKey !== EXPECTED_API_KEY) {
      const unauthorizedResponse = awslambda.HttpResponseStream.from(responseStream, {
        statusCode: 401,
        headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": ALLOWED_ORIGIN },
      });
      unauthorizedResponse.write(JSON.stringify({ error: "Unauthorized: Invalid x-api-key" }));
      unauthorizedResponse.end();
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
      let prompt = "Explain quantum computing in 3 simple sentences.";
      let systemPrompt = "You are a concise, helpful AI technical assistant.";
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

      let fullResponseText = "";
      let tokenUsage = null;
      const startTime = Date.now();
      let firstTokenTime = null;

      for await (const chunk of bedrockResponse.stream) {
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

        if (chunk.metadata?.usage) {
          tokenUsage = chunk.metadata.usage;
        }

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
