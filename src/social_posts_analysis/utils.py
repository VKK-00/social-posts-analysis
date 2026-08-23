from __future__ import annotations

import hashlib
import json
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def make_run_id() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def stable_id(*parts: str) -> str:
    joined = "::".join(parts)
    return hashlib.sha1(joined.encode("utf-8")).hexdigest()


def pseudonymize_author_id(author_id: str | None) -> str | None:
    """Return a stable, non-reversible pseudonym for a third-party author id."""
    if not author_id:
        return None
    return f"anon-{hashlib.sha256(author_id.encode('utf-8')).hexdigest()[:16]}"


def handle_rate_limit_response(
    response: Any,
    *,
    default_seconds: float = 2.0,
    max_seconds: float = 30.0,
) -> float | None:
    """Sleep when an API responds with HTTP 429, honouring ``Retry-After``.

    Callers keep their existing tenacity retry loops; this only adds the
    polite wait the rate-limiting server asked for before the next attempt.
    Returns the delay slept (or ``None`` when the response was not a 429).
    """
    status_code = getattr(response, "status_code", None)
    if status_code != 429:
        return None
    headers = getattr(response, "headers", None) or {}
    raw_delay = headers.get("Retry-After") or headers.get("retry-after")
    try:
        delay = float(str(raw_delay))
    except (TypeError, ValueError):
        delay = default_seconds
    delay = max(0.0, min(delay, max_seconds))
    if delay > 0:
        time.sleep(delay)
    return delay


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip()).strip("-")
    return slug.lower() or "item"


def parse_compact_number(value: str | None) -> int:
    if not value:
        return 0
    normalized = value.strip().replace(",", "").replace("\u202f", "").replace("\xa0", "")
    normalized = normalized.replace(" views", "").replace(" view", "")
    normalized = normalized.replace(" followers", "").replace(" follower", "")
    normalized = normalized.replace(" likes", "").replace(" like", "")
    normalized = normalized.replace(" reposts", "").replace(" repost", "")
    normalized = normalized.replace(" replies", "").replace(" reply", "")
    normalized = normalized.replace(" bookmarks", "").replace(" bookmark", "")
    normalized = normalized.strip()
    match = re.match(r"^(\d+(?:\.\d+)?)([KMB])?$", normalized, flags=re.IGNORECASE)
    if not match:
        digits = re.sub(r"[^\d]", "", normalized)
        return int(digits) if digits else 0
    base_value = float(match.group(1))
    suffix = (match.group(2) or "").upper()
    multiplier = {"": 1, "K": 1_000, "M": 1_000_000, "B": 1_000_000_000}[suffix]
    return int(base_value * multiplier)

