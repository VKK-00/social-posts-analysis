from __future__ import annotations

import hashlib
import json
import re
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

