import requests
from pathlib import Path
from datetime import datetime
import json
from typing import Optional, Dict, Any

ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSED = ROOT / "data" / "processed"
CACHE_DIR = DATA_PROCESSED / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://vhd27npae1.execute-api.us-east-1.amazonaws.com/lineups"

def _etag_file(team: Optional[str]) -> Path:
    key = "ALL" if team is None else team.upper()
    return CACHE_DIR / f"lineups_{key}_etag.txt"

def _last_file(team: Optional[str]) -> Path:
    key = "ALL" if team is None else team.upper()
    return CACHE_DIR / f"lineups_{key}_last.txt"

def _read_text(path: Path) -> Optional[str]:
    try:
        return path.read_text().strip()
    except FileNotFoundError:
        return None

def _write_text(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)

def fetch_lineups(team: Optional[str] = None, use_cache: bool = True) -> Dict[str, Any]:
    """
    Fetch daily lineups (all teams or one team).
    - Respects ETag to avoid re-downloading unchanged data.
    - Safe to run once each morning.
    """
    url = BASE_URL if team is None else f"{BASE_URL}/{team.upper()}"
    headers = {}

    if use_cache:
        etag = _read_text(_etag_file(team))
        if etag:
            headers["If-None-Match"] = etag

    resp = requests.get(url, timeout=20, headers=headers)

    if resp.status_code == 304:
        last = _read_text(_last_file(team)) or "unknown"
        return {"status": "not_modified", "url": url, "last_fetch": last}

    resp.raise_for_status()
    data = resp.json()

    # Save cache headers
    etag = resp.headers.get("ETag")
    if etag:
        _write_text(_etag_file(team), etag)
    _write_text(_last_file(team), datetime.utcnow().isoformat())

    # Save raw JSON for reference
    stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_file = DATA_PROCESSED / f"lineups_{team or 'ALL'}_{stamp}.json"
    out_file.write_text(json.dumps(data, indent=2))

    return {
        "status": "ok",
        "url": url,
        "count": len(data) if isinstance(data, list) else 1,
        "data": data,
    }
