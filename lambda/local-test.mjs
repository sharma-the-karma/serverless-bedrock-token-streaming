/**
 * Local simulation runner for testing the Bedrock Streaming Handler
 * Run with: node local-test.mjs
 */
import { Writable } from "node:stream";

// Mock awslambda global for local testing environment
globalThis.awslambda = {
  streamifyResponse: (fn) => fn,
  HttpResponseStream: {
    from: (stream, metadata) => {
      console.log("\n[HTTP Response Stream Headers]:", JSON.stringify(metadata, null, 2));
      return {
        write: (chunk) => stream.write(chunk),
        end: () => stream.end(),
      };
    },
  },
};

// Import our Lambda handler
const { handler } = await import("./index.mjs");

// Create standard output writable stream
const outputStream = new Writable({
  write(chunk, encoding, callback) {
    process.stdout.write(chunk.toString());
    callback();
  },
});

outputStream.on("finish", () => {
  console.log("\n\n Stream completed successfully.");
});

const mockEvent = {
  body: JSON.stringify({
    prompt: "Write a haiku about serverless response streaming.",
    modelId: "anthropic.claude-3-haiku-20240307-v1:0",
  }),
};

console.log("=== Testing Bedrock Streaming Handler Locally ===");
console.log("Payload:", mockEvent.body);
console.log("--- Starting Stream ---");

try {
  await handler(mockEvent, outputStream, {});
} catch (err) {
  console.error("Local invocation failed:", err);
}
