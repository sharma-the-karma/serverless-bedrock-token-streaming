#!/usr/bin/env python3
"""
Publishes devto-blog-post.md to Dev.to using the Dev.to REST API.

Usage:
  python publish_to_devto.py --api-key YOUR_DEVTO_API_KEY [--draft]
Or set environment variable:
  $env:DEVTO_API_KEY="your_api_key"
  python publish_to_devto.py
"""

import os
import sys
import re
import json
import argparse
import urllib.request
import urllib.error

BLOG_FILE = os.path.join(os.path.dirname(__file__), "devto-blog-post.md")

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

def publish_article(api_key: str, draft: bool = False):
    if not os.path.exists(BLOG_FILE):
        print(f"Error: {BLOG_FILE} not found!")
        sys.exit(1)

    with open(BLOG_FILE, "r", encoding="utf-8") as f:
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

    req = urllib.request.Request(
        "https://dev.to/api/articles",
        data=req_data,
        headers={
            "api-key": api_key,
            "Content-Type": "application/json",
            "User-Agent": "AntigravityDevToPublisher/1.0"
        }
    )

    try:
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            print("\n🎉 Successfully published to Dev.to!")
            print(f"Article URL: {data.get('url')}")
            print(f"Status: {'Published' if is_published else 'Draft'}")
            print(f"ID: {data.get('id')}")
            return data
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8")
        print(f"\n❌ Failed to publish to Dev.to (HTTP {e.code}):")
        try:
            err_json = json.loads(error_body)
            print(json.dumps(err_json, indent=2))
        except Exception:
            print(error_body)
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Publish markdown article to Dev.to")
    parser.add_argument("--api-key", "-k", help="Dev.to API key", default=os.getenv("DEVTO_API_KEY"))
    parser.add_argument("--draft", action="store_true", help="Publish as draft")

    args = parser.parse_args()

    api_key = args.api_key
    if not api_key:
        api_key = input("Enter your Dev.to API key: ").strip()

    if not api_key:
        print("Error: Dev.to API key is required. Get one at: https://dev.to/settings/extensions")
        sys.exit(1)

    publish_article(api_key, draft=args.draft)
