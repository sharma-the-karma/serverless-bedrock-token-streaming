#!/usr/bin/env python3
"""
Publishes or updates devto-blog-post.md to Dev.to using the Dev.to REST API.

Usage:
  python publish_to_devto.py --api-key YOUR_DEVTO_API_KEY [--article-id ID] [--draft]
"""

import os
import sys
import json
import argparse
import urllib.request
import urllib.error

# Ensure UTF-8 output on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

DEFAULT_BLOG_FILE = os.path.join(
    os.path.dirname(__file__), "blog", "serverless-bedrock-token-streaming.md"
)

def parse_frontmatter(content: str):
    frontmatter = {}
    body = content
    if content.startswith("---"):
        parts = content.split("---", 2)
        if len(parts) >= 3:
            raw_fm = parts[1]
            body = parts[2].lstrip()
            for line in raw_fm.strip().splitlines():
                if ":" in line:
                    key, val = line.split(":", 1)
                    key = key.strip()
                    val = val.strip().strip('"').strip("'")
                    if key == "tags":
                        tags = [t.strip() for t in val.split(",")]
                        frontmatter["tags"] = tags
                    elif key == "published":
                        frontmatter["published"] = val.lower() in ("true", "1", "yes")
                    else:
                        frontmatter[key] = val
    return frontmatter, body

def publish_or_update_article(api_key: str, file_path: str = DEFAULT_BLOG_FILE, article_id: str = None, draft: bool = False):
    if not os.path.exists(file_path):
        print(f"[error] {file_path} not found!")
        sys.exit(1)

    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    fm, body = parse_frontmatter(content)

    title = fm.get("title", "Real-Time Token Streaming with Amazon Bedrock & AWS Lambda")
    description = fm.get("description", "")
    tags = fm.get("tags", ["aws", "serverless", "bedrock", "ai"])
    is_published = False if draft else fm.get("published", True)

    payload = {
        "article": {
            "title": title,
            "body_markdown": body,
            "published": is_published,
            "tags": tags,
            "description": description,
        }
    }

    if "cover_image" in fm and fm["cover_image"].startswith("http"):
        payload["article"]["main_image"] = fm["cover_image"]

    req_data = json.dumps(payload).encode("utf-8")

    if article_id:
        url = f"https://dev.to/api/articles/{article_id}"
        method = "PUT"
    else:
        url = "https://dev.to/api/articles"
        method = "POST"

    req = urllib.request.Request(
        url,
        data=req_data,
        method=method,
        headers={
            "api-key": api_key,
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }
    )

    try:
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            action = "Updated" if article_id else "Published"
            print(f"[success] {action} article on Dev.to!")
            print(f"URL: {data.get('url')}")
            print(f"Status: {'Published' if is_published else 'Draft'}")
            print(f"Article ID: {data.get('id')}")
            return data
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8")
        print(f"[error] Failed to send article to Dev.to (HTTP {e.code}):")
        try:
            err_json = json.loads(error_body)
            print(json.dumps(err_json, indent=2))
        except Exception:
            print(error_body)
        sys.exit(1)
    except Exception as e:
        print(f"[error] Unexpected failure: {e}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Publish or update markdown article to Dev.to")
    parser.add_argument("--api-key", "-k", help="Dev.to API key", default=os.getenv("DEVTO_API_KEY"))
    parser.add_argument("--file", "-f", help="Markdown article file path", default=DEFAULT_BLOG_FILE)
    parser.add_argument("--article-id", "-i", help="Existing Dev.to article ID to update", default=None)
    parser.add_argument("--draft", action="store_true", help="Publish as draft")

    args = parser.parse_args()

    api_key = args.api_key
    if not api_key:
        api_key = input("Enter your Dev.to API key: ").strip()

    if not api_key:
        print("[error] Dev.to API key is required. Get one at: https://dev.to/settings/extensions")
        sys.exit(1)

    publish_or_update_article(api_key, file_path=args.file, article_id=args.article_id, draft=args.draft)
