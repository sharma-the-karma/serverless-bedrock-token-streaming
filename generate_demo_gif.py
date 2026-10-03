"""
Generates an animated GIF demonstration of the Bedrock token streaming playground
for LinkedIn posts and GitHub READMEs.
"""

import os
from PIL import Image, ImageDraw, ImageFont

ASSETS_DIR = os.path.join(os.path.dirname(__file__), "assets")
os.makedirs(ASSETS_DIR, exist_ok=True)
OUTPUT_GIF = os.path.join(ASSETS_DIR, "streaming-demo.gif")

# Resolution: 960 x 540 (16:9 ratio, perfect for LinkedIn feed)
WIDTH = 960
HEIGHT = 540

# Color palette
BG_DARK = (10, 13, 20)
PANEL_BG = (17, 23, 38)
PANEL_BORDER = (40, 50, 75)
TEXT_WHITE = (243, 244, 246)
TEXT_MUTED = (156, 163, 175)
TEXT_DIM = (107, 114, 128)
AWS_ORANGE = (255, 153, 0)
CYAN_GLOW = (6, 182, 212)
PURPLE_ACCENT = (139, 92, 246)
USER_BUBBLE_BG = (79, 70, 229)
ASSISTANT_BUBBLE_BG = (26, 36, 56)

# Fonts
try:
    font_bold_lg = ImageFont.truetype("arialbd.ttf", 16)
    font_bold_md = ImageFont.truetype("arialbd.ttf", 14)
    font_regular = ImageFont.truetype("arial.ttf", 13)
    font_mono = ImageFont.truetype("consola.ttf", 14)
    font_mono_bold = ImageFont.truetype("consolab.ttf", 18)
    font_small = ImageFont.truetype("arial.ttf", 11)
except Exception:
    font_bold_lg = ImageFont.load_default()
    font_bold_md = ImageFont.load_default()
    font_regular = ImageFont.load_default()
    font_mono = ImageFont.load_default()
    font_mono_bold = ImageFont.load_default()
    font_small = ImageFont.load_default()

prompt_text = "Why does API Gateway buffer SSE responses and how does Lambda solve it?"

tokens = [
    "Token-by-token streaming ",
    "slashes perceived latency ",
    "from 8.5s down to 260ms TTFT.\n\n",
    "1. API Gateway buffers all HTTP responses ",
    "in memory until completion (29s limit).\n",
    "2. Lambda Function URLs with ",
    "InvokeMode: RESPONSE_STREAM ",
    "stream chunks directly to clients!\n\n",
    "Sub-300ms Time To First Token achieved."
]

def draw_base_ui(draw: ImageDraw.ImageDraw, ttft: str, token_count: int, speed: str, duration: str, is_streaming: bool):
    # Background
    draw.rectangle([0, 0, WIDTH, HEIGHT], fill=BG_DARK)

    # Top Header
    draw.rectangle([0, 0, WIDTH, 54], fill=(13, 17, 28), outline=PANEL_BORDER, width=1)
    
    # AWS Bedrock Badge
    draw.rounded_rectangle([20, 14, 150, 40], radius=12, fill=(40, 25, 10), outline=AWS_ORANGE, width=1)
    draw.ellipse([28, 23, 36, 31], fill=AWS_ORANGE)
    draw.text((44, 20), "AWS BEDROCK", fill=AWS_ORANGE, font=font_bold_md)
    draw.text((165, 19), "Token Streaming Playground", fill=TEXT_WHITE, font=font_bold_lg)

    # Live Status Badge
    status_color = CYAN_GLOW if is_streaming else (16, 185, 129)
    status_label = "Streaming Live..." if is_streaming else "Stream Complete"
    draw.rounded_rectangle([WIDTH - 170, 14, WIDTH - 20, 40], radius=12, fill=(15, 25, 35), outline=status_color, width=1)
    draw.ellipse([WIDTH - 158, 23, WIDTH - 150, 31], fill=status_color)
    draw.text((WIDTH - 142, 20), status_label, fill=status_color, font=font_bold_md)

    # Left Sidebar (Telemetry HUD)
    draw.rounded_rectangle([20, 68, 290, HEIGHT - 20], radius=12, fill=PANEL_BG, outline=PANEL_BORDER, width=1)
    draw.text((36, 82), "STREAMING TELEMETRY HUD", fill=TEXT_MUTED, font=font_bold_md)

    # Metric Cards
    metrics = [
        ("TTFT (FIRST TOKEN)", ttft, CYAN_GLOW),
        ("TOKENS EMITTED", str(token_count), PURPLE_ACCENT),
        ("STREAMING SPEED", speed, AWS_ORANGE),
        ("TOTAL DURATION", duration, TEXT_WHITE),
    ]

    card_y = 110
    for label, val, color in metrics:
        draw.rounded_rectangle([32, card_y, 278, card_y + 60], radius=8, fill=(11, 15, 26), outline=PANEL_BORDER, width=1)
        draw.text((42, card_y + 8), label, fill=TEXT_DIM, font=font_small)
        draw.text((42, card_y + 26), val, fill=color, font=font_mono_bold)
        card_y += 70

    # Architecture Blueprint Box
    draw.rounded_rectangle([32, card_y + 10, 278, HEIGHT - 32], radius=8, fill=(14, 20, 34), outline=PANEL_BORDER, width=1)
    draw.text((42, card_y + 20), "ARCHITECTURE BLUEPRINT", fill=TEXT_MUTED, font=font_small)
    draw.text((42, card_y + 38), "Lambda Function URL", fill=TEXT_WHITE, font=font_bold_md)
    draw.text((42, card_y + 56), "InvokeMode: RESPONSE_STREAM", fill=AWS_ORANGE, font=font_mono)
    draw.text((42, card_y + 76), "Bypasses API Gateway 29s timeout", fill=TEXT_DIM, font=font_small)

    # Main Chat Area
    draw.rounded_rectangle([306, 68, WIDTH - 20, HEIGHT - 20], radius=12, fill=PANEL_BG, outline=PANEL_BORDER, width=1)

    # User Message Bubble
    draw.ellipse([324, 88, 356, 120], fill=(55, 65, 81))
    draw.text((333, 98), "YOU", fill=TEXT_WHITE, font=font_small)
    draw.rounded_rectangle([366, 84, WIDTH - 36, 126], radius=10, fill=USER_BUBBLE_BG)
    draw.text((378, 96), prompt_text, fill=TEXT_WHITE, font=font_regular)

    # Assistant Avatar
    draw.ellipse([324, 144, 356, 176], fill=PURPLE_ACCENT)
    draw.text((333, 154), "AWS", fill=TEXT_WHITE, font=font_small)


def create_frame(text_so_far: str, cursor_on: bool, ttft: str, token_count: int, speed: str, duration: str, is_streaming: bool) -> Image.Image:
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_DARK)
    draw = ImageDraw.Draw(img)

    draw_base_ui(draw, ttft, token_count, speed, duration, is_streaming)

    # Assistant Response Bubble
    draw.rounded_rectangle([366, 140, WIDTH - 36, HEIGHT - 36], radius=10, fill=ASSISTANT_BUBBLE_BG, outline=PANEL_BORDER, width=1)

    # Render accumulated text with line wrapping
    lines = text_so_far.split("\n")
    y_offset = 156
    last_line_x = 380

    for i, line in enumerate(lines):
        if line:
            # Highlight key terms
            draw.text((380, y_offset), line, fill=TEXT_WHITE, font=font_regular)
            last_line_x = 380 + int(draw.textlength(line, font=font_regular))
        else:
            last_line_x = 380
        y_offset += 20

    # Draw blinking cursor
    if cursor_on and is_streaming:
        cursor_y = y_offset - 20
        draw.rectangle([last_line_x + 3, cursor_y + 2, last_line_x + 8, cursor_y + 16], fill=CYAN_GLOW)

    return img


def build_gif():
    frames = []
    durations = []

    # Initial state (user sent prompt, awaiting first token)
    for _ in range(3):
        frames.append(create_frame("", True, "-- ms", 0, "-- tps", "0.1 s", True))
        durations.append(150)

    accumulated = ""
    token_counter = 0

    for idx, token_chunk in enumerate(tokens):
        accumulated += token_chunk
        token_counter += len(token_chunk.split())
        elapsed_sec = 0.26 + (idx * 0.18)
        speed_val = int(token_counter / max(elapsed_sec - 0.26, 0.1))

        # 2 frames per token (cursor blink)
        f1 = create_frame(accumulated, True, "248 ms", token_counter, f"{speed_val} tps", f"{elapsed_sec:.1f} s", True)
        f2 = create_frame(accumulated, False, "248 ms", token_counter, f"{speed_val} tps", f"{elapsed_sec:.1f} s", True)

        frames.extend([f1, f2])
        durations.extend([90, 90])

    # Final completed state (hold for 2.5 seconds so viewers can examine the HUD)
    final_frame = create_frame(accumulated, False, "248 ms", token_counter, "46 tps", "2.1 s", False)
    for _ in range(8):
        frames.append(final_frame)
        durations.append(300)

    # Save animated GIF
    print(f"Saving animated GIF with {len(frames)} frames...")
    frames[0].save(
        OUTPUT_GIF,
        save_all=True,
        append_images=frames[1:],
        duration=durations,
        loop=0,
        optimize=True,
    )

    size_mb = os.path.getsize(OUTPUT_GIF) / (1024 * 1024)
    print(f"GIF generated successfully: {OUTPUT_GIF} ({size_mb:.2f} MB)")


if __name__ == "__main__":
    build_gif()
