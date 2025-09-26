# -*- coding: utf-8 -*-
"""
Common utilities shared across ADP NHL modules
"""

import os
import re
import time
import requests
from datetime import datetime

# ---------------------------- Normalization ----------------------------
def norm_name(s: str) -> str:
    """
    Normalize player names for consistent matching across sources.
    - Strips accents, punctuation, and casing
    - Converts Last, First -> First Last
    """
    if not isinstance(s, str):
        return ""
    s = re.sub(r"[\u2013\u2014\u2019]", "-", str(s))
    s = re.sub(r"[^A-Za-z0-9\-\' ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip().upper()
    if "," in s:
        parts = [p.strip() for p in s.split(",")]
        if len(parts) == 2:
            s = f"{parts[1]} {parts[0]}".strip()
    return s

# ---------------------------- HTTP Cache ----------------------------
def http_get_cached(url, tag, cache_dir="data/raw", sleep=2, retries=5, headers=None):
    """
    Fetch HTML/JSON from URL with local caching.
    - Caches to data/raw/{tag}_{YYYYMMDD}.html
    - Retries on rate limiting (429)
    """
    headers = headers or {"User-Agent": "Mozilla/5.0 (ADP Free Model)"}
    today = datetime.today().strftime("%Y%m%d")
    cache_file = os.path.join(cache_dir, f"{tag}_{today}.html")

    # Return from cache if exists
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return f.read()

    tries = 0
    while tries < retries:
        try:
            r = requests.get(url, headers=headers, timeout=60)
            if r.status_code == 429:
                print("⚠️ Rate limited. Sleeping 60s...")
                time.sleep(60)
                tries += 1
                continue
            r.raise_for_status()
            html = r.text
            os.makedirs(cache_dir, exist_ok=True)
            with open(cache_file, "w", encoding="utf-8") as f:
                f.write(html)
            time.sleep(sleep)
            return html
        except Exception as e:
            print(f"❌ Fetch error for {url} ({tag}): {e}")
            time.sleep(10)
            tries += 1
    return None
