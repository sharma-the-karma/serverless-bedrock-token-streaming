/**
 * Bedrock Real-Time Streaming Playground
 * Handles token-by-token SSE streaming, live telemetry, and mock simulation mode.
 */

// DOM Elements
const endpointInput = document.getElementById("endpoint-url");
const mockModeToggle = document.getElementById("mock-mode-toggle");
const modelSelector = document.getElementById("model-selector");
const systemPromptInput = document.getElementById("system-prompt");
const messagesContainer = document.getElementById("messages-container");
const chatForm = document.getElementById("chat-form");
const promptInput = document.getElementById("prompt-input");
const sendButton = document.getElementById("send-button");
const connectionStatus = document.getElementById("connection-status");
const suggestionChips = document.getElementById("suggestion-chips");

// Telemetry DOM Elements
const metricTtft = document.getElementById("metric-ttft");
const metricTokens = document.getElementById("metric-tokens");
const metricSpeed = document.getElementById("metric-speed");
const metricDuration = document.getElementById("metric-duration");

// App State
let isStreaming = false;
let conversationHistory = [];

// Auto-resize textarea
promptInput.addEventListener("input", () => {
  promptInput.style.height = "auto";
  promptInput.style.height = Math.min(promptInput.scrollHeight, 120) + "px";
});

promptInput.addEventListener("keydown", (e) => {
  if (e.key === "Enter" && !e.shiftKey) {
    e.preventDefault();
    chatForm.dispatchEvent(new Event("submit"));
  }
});

// Suggestion Chips
suggestionChips.addEventListener("click", (e) => {
  const chip = e.target.closest(".chip");
  if (chip && !isStreaming) {
    promptInput.value = chip.dataset.prompt;
    promptInput.focus();
  }
});

// Append Message Row
function appendMessage(role, text = "") {
  const row = document.createElement("div");
  row.className = `message-row ${role}`;

  const avatar = document.createElement("div");
  avatar.className = `avatar ${role === "assistant" ? "bedrock-avatar" : "user-avatar"}`;
  avatar.textContent = role === "assistant" ? "AWS" : "YOU";

  const content = document.createElement("div");
  content.className = "message-content";

  const bubble = document.createElement("div");
  bubble.className = "message-bubble";
  bubble.innerHTML = formatMarkdown(text);

  content.appendChild(bubble);
  row.appendChild(avatar);
  row.appendChild(content);
  messagesContainer.appendChild(row);
  scrollToBottom();

  return bubble;
}

function scrollToBottom() {
  messagesContainer.scrollTop = messagesContainer.scrollHeight;
}

// Simple Markdown Formatter
function formatMarkdown(text) {
  if (!text) return "";
  let formatted = text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");

  // Code blocks ```code```
  formatted = formatted.replace(/```([a-z]*)\n([\s\S]*?)```/g, (_, lang, code) => {
    return `<pre><code>${code.trim()}</code></pre>`;
  });

  // Inline code `code`
  formatted = formatted.replace(/`([^`]+)`/g, "<code>$1</code>");

  // Bold **text**
  formatted = formatted.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");

  // Line breaks
  formatted = formatted.replace(/\n\n/g, "</p><p>").replace(/\n/g, "<br/>");

  return `<p>${formatted}</p>`;
}

// UI State Management
function setStreamingState(streaming) {
  isStreaming = streaming;
  sendButton.disabled = streaming;
  promptInput.disabled = streaming;

  const dot = connectionStatus.querySelector(".status-dot");
  const text = connectionStatus.querySelector(".status-text");

  if (streaming) {
    dot.className = "status-dot streaming";
    text.textContent = "Streaming...";
  } else {
    dot.className = "status-dot";
    text.textContent = "Ready";
  }
}

// Submit Handler
chatForm.addEventListener("submit", async (e) => {
  e.preventDefault();
  const prompt = promptInput.value.trim();
  if (!prompt || isStreaming) return;

  // Add user message
  appendMessage("user", prompt);
  promptInput.value = "";
  promptInput.style.height = "auto";

  // Create empty assistant bubble with blinking cursor
  const assistantBubble = appendMessage("assistant", "");
  const cursor = document.createElement("span");
  cursor.className = "streaming-cursor";
  assistantBubble.appendChild(cursor);

  setStreamingState(true);

  // Reset Telemetry
  metricTtft.textContent = "-- ms";
  metricTokens.textContent = "0";
  metricSpeed.textContent = "-- tps";
  metricDuration.textContent = "-- s";

  const startTime = performance.now();
  let firstTokenTime = null;
  let accumulatedText = "";
  let tokenCount = 0;

  const endpoint = endpointInput.value.trim();
  const isMock = mockModeToggle.checked || !endpoint;

  if (isMock) {
    // Run Mock Simulator
    await simulateStreaming(prompt, assistantBubble, cursor, {
      startTime,
      onToken: (chunk) => {
        if (!firstTokenTime) {
          firstTokenTime = performance.now();
          const ttft = Math.round(firstTokenTime - startTime);
          metricTtft.textContent = `${ttft} ms`;
        }

        accumulatedText += chunk;
        tokenCount += 1;
        assistantBubble.innerHTML = formatMarkdown(accumulatedText);
        assistantBubble.appendChild(cursor);
        scrollToBottom();

        // Update live metrics
        const elapsedSec = (performance.now() - startTime) / 1000;
        metricTokens.textContent = tokenCount.toString();
        metricDuration.textContent = `${elapsedSec.toFixed(1)} s`;
        metricSpeed.textContent = `${Math.round(tokenCount / Math.max(elapsedSec, 0.1))} tps`;
      },
    });

    cursor.remove();
    setStreamingState(false);
    return;
  }

  // Real HTTP SSE Streaming Reader
  try {
    const payload = {
      prompt,
      modelId: modelSelector.value,
      system: systemPromptInput.value,
    };

    const response = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      throw new Error(`HTTP Error: ${response.status} ${response.statusText}`);
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop(); // Keep partial line in buffer

      let currentEvent = "message";

      for (const line of lines) {
        if (line.startsWith("event:")) {
          currentEvent = line.replace("event:", "").trim();
        } else if (line.startsWith("data:")) {
          const rawData = line.replace("data:", "").trim();
          try {
            const data = JSON.parse(rawData);

            if (currentEvent === "delta" && data.text) {
              if (!firstTokenTime) {
                firstTokenTime = performance.now();
                metricTtft.textContent = `${Math.round(firstTokenTime - startTime)} ms`;
              }

              accumulatedText += data.text;
              tokenCount += 1;
              assistantBubble.innerHTML = formatMarkdown(accumulatedText);
              assistantBubble.appendChild(cursor);
              scrollToBottom();

              const elapsedSec = (performance.now() - startTime) / 1000;
              metricTokens.textContent = tokenCount.toString();
              metricDuration.textContent = `${elapsedSec.toFixed(1)} s`;
              metricSpeed.textContent = `${Math.round(tokenCount / Math.max(elapsedSec, 0.1))} tps`;
            } else if (currentEvent === "metric_ttft" && data.ttftMs) {
              metricTtft.textContent = `${data.ttftMs} ms`;
            } else if (currentEvent === "error") {
              assistantBubble.innerHTML += `<br/><strong style="color:var(--accent-red)">Error: ${data.message}</strong>`;
            }
          } catch {
            // Raw text fallback
            accumulatedText += rawData;
            assistantBubble.innerHTML = formatMarkdown(accumulatedText);
            assistantBubble.appendChild(cursor);
          }
        }
      }
    }
  } catch (err) {
    assistantBubble.innerHTML += `<p style="color: var(--accent-red); margin-top: 10px;">
      ⚠️ <strong>Streaming Error:</strong> ${err.message}<br/>
      <small>Check Function URL CORS, Bedrock model access in your AWS region, or enable 'Simulate Local Stream'.</small>
    </p>`;
  } finally {
    cursor.remove();
    setStreamingState(false);
  }
});

// Mock Streaming Simulation Generator
async function simulateStreaming(prompt, bubble, cursor, { startTime, onToken }) {
  const simulatedAnswers = {
    default: [
      "Token-by-token streaming ",
      "transforms the user experience ",
      "by cutting perceived latency ",
      "from **8.5 seconds** down to **sub-300ms** Time To First Token (TTFT).\n\n",
      "### Why API Gateway Breaks Streaming:\n",
      "1. **29-Second Hard Limit:** API Gateway abruptly cuts off long generations.\n",
      "2. **Response Buffering:** Both REST and HTTP APIs accumulate all chunks before forwarding.\n\n",
      "### The Architectural Fix:\n",
      "Deploy an **AWS Lambda Function URL** with `InvokeMode: RESPONSE_STREAM`.\n",
      "Combine it with `@aws-sdk/client-bedrock-runtime`'s `ConverseStreamCommand`.\n\n",
      "```javascript\n",
      "export const handler = awslambda.streamifyResponse(\n",
      "  async (event, responseStream, context) => {\n",
      "    const stream = awslambda.HttpResponseStream.from(responseStream, headers);\n",
      "    for await (const chunk of bedrockResponse.stream) {\n",
      "      stream.write(`data: ${JSON.stringify(chunk)}\\n\\n`);\n",
      "    }\n",
      "    stream.end();\n",
      "  }\n",
      ");\n",
      "```\n\n",
      "The browser streams tokens immediately via standard `ReadableStream`!",
    ],
  };

  const tokens = simulatedAnswers.default;

  // Simulate network handshake latency
  await new Promise((r) => setTimeout(r, 220));

  for (const token of tokens) {
    onToken(token);
    // Dynamic typing delay (15ms - 45ms per chunk)
    await new Promise((r) => setTimeout(r, Math.floor(Math.random() * 30) + 15));
  }
}
