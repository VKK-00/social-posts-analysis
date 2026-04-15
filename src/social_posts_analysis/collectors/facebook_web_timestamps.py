from __future__ import annotations

import re
import unicodedata
from datetime import UTC, datetime, timedelta


def normalize_mobile_text(text: str) -> str:
    cleaned = "".join(ch for ch in text.replace("\xa0", " ") if unicodedata.category(ch) != "Co")
    cleaned = cleaned.replace("ô¸", " ").replace("ô¤¦", " ").replace("ôŒ«", " ")
    cleaned = re.sub(r"[ \t]+", " ", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def parse_post_timestamp(raw_hint: str) -> str | None:
    hint = normalize_mobile_text(raw_hint or "").replace("\u202f", " ").replace("\xa0", " ").strip()
    if not hint:
        return None
    parsed = parse_timestamp_token(hint)
    if parsed:
        return parsed
    extracted_hint = extract_supported_date_hint_safe(hint)
    if extracted_hint and extracted_hint != hint:
        return parse_timestamp_token(extracted_hint)
    return None


def parse_timestamp_token(hint: str) -> str | None:
    now = datetime.now(tz=UTC).replace(microsecond=0)
    relative_patterns = [
        (r"^(\d+)\s*m(?:in)?s?$", "minutes"),
        (r"^(\d+)\s*h(?:r|rs)?s?$", "hours"),
        (r"^(\d+)\s*d(?:ay|ays)?s?$", "days"),
        (r"^(\d+)\s*w(?:eek|eeks)?s?$", "weeks"),
    ]
    lowered = hint.lower()
    for pattern, unit in relative_patterns:
        match = re.match(pattern, lowered)
        if match:
            delta_value = int(match.group(1))
            delta = timedelta(**{unit: delta_value})
            return (now - delta).isoformat()

    if lowered == "yesterday":
        return (now - timedelta(days=1)).isoformat()
    if lowered.startswith("yesterday at "):
        try:
            parsed_time = datetime.strptime(lowered.replace("yesterday at ", ""), "%I:%M %p").time()
            return datetime.combine((now - timedelta(days=1)).date(), parsed_time, tzinfo=UTC).isoformat()
        except ValueError:
            return (now - timedelta(days=1)).isoformat()
    if lowered in {"вчора", "вчера"}:
        return (now - timedelta(days=1)).isoformat()
    for prefix in ("вчора о ", "вчера в "):
        if lowered.startswith(prefix):
            try:
                parsed_time = datetime.strptime(lowered.replace(prefix, ""), "%H:%M").time()
                return datetime.combine((now - timedelta(days=1)).date(), parsed_time, tzinfo=UTC).isoformat()
            except ValueError:
                return (now - timedelta(days=1)).isoformat()

    formats = [
        ("%B %d at %I:%M %p", False),
        ("%b %d at %I:%M %p", False),
        ("%B %d, %Y at %I:%M %p", True),
        ("%b %d, %Y at %I:%M %p", True),
        ("%B %d", False),
        ("%b %d", False),
        ("%B %d, %Y", True),
        ("%b %d, %Y", True),
    ]
    for fmt, has_explicit_year in formats:
        try:
            if has_explicit_year:
                parsed = datetime.strptime(hint, fmt)
            else:
                parse_hint = f"{hint}, {now.year}"
                parse_fmt = f"{fmt}, %Y"
                parsed = datetime.strptime(parse_hint, parse_fmt)
            final_dt = parsed.replace(tzinfo=UTC)
            return final_dt.isoformat()
        except ValueError:
            continue
    return parse_localized_absolute_timestamp_safe(hint, now)


def extract_supported_date_hint_safe(text: str) -> str:
    normalized = normalize_mobile_text(text).strip(" .,;:|()[]")
    if not normalized:
        return ""
    localized_suffix = r"(?:\s*(?:\u0440\u043e\u043a\u0443|\u0433\u043e\u0434\u0430|\u0440\.?))?"
    patterns = [
        r"\b\d+\s*(?:m(?:in)?s?|h(?:r|rs)?s?|d(?:ay|ays)?s?|w(?:eek|eeks)?s?)\b",
        r"\byesterday(?:\s+at\s+\d{1,2}:\d{2}\s*[ap]m)?\b",
        r"\bвчора(?:\s+о\s+\d{1,2}:\d{2})?\b",
        r"\bвчера(?:\s+в\s+\d{1,2}:\d{2})?\b",
        r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\s+\d{1,2}(?:,\s*\d{4})?(?:\s+at\s+\d{1,2}:\d{2}\s*[ap]m)?\b",
        rf"\b\d{{1,2}}\s+[A-Za-z\u0400-\u04FF]+(?:\s+\d{{4}})?{localized_suffix}\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, normalized, flags=re.IGNORECASE)
        if match:
            return match.group(0).strip(" .,;:|()[]")
    return ""


def parse_localized_absolute_timestamp_safe(hint: str, now: datetime) -> str | None:
    month_map = {
        "january": 1,
        "jan": 1,
        "february": 2,
        "feb": 2,
        "march": 3,
        "mar": 3,
        "april": 4,
        "apr": 4,
        "may": 5,
        "june": 6,
        "jun": 6,
        "july": 7,
        "jul": 7,
        "august": 8,
        "aug": 8,
        "september": 9,
        "sep": 9,
        "sept": 9,
        "october": 10,
        "oct": 10,
        "november": 11,
        "nov": 11,
        "december": 12,
        "dec": 12,
        "\u0441\u0456\u0447\u043d\u044f": 1,
        "\u043b\u044e\u0442\u043e\u0433\u043e": 2,
        "\u0431\u0435\u0440\u0435\u0437\u043d\u044f": 3,
        "\u043a\u0432\u0456\u0442\u043d\u044f": 4,
        "\u0442\u0440\u0430\u0432\u043d\u044f": 5,
        "\u0447\u0435\u0440\u0432\u043d\u044f": 6,
        "\u043b\u0438\u043f\u043d\u044f": 7,
        "\u0441\u0435\u0440\u043f\u043d\u044f": 8,
        "\u0432\u0435\u0440\u0435\u0441\u043d\u044f": 9,
        "\u0436\u043e\u0432\u0442\u043d\u044f": 10,
        "\u043b\u0438\u0441\u0442\u043e\u043f\u0430\u0434\u0430": 11,
        "\u0433\u0440\u0443\u0434\u043d\u044f": 12,
        "\u044f\u043d\u0432\u0430\u0440\u044f": 1,
        "\u0444\u0435\u0432\u0440\u0430\u043b\u044f": 2,
        "\u043c\u0430\u0440\u0442\u0430": 3,
        "\u0430\u043f\u0440\u0435\u043b\u044f": 4,
        "\u043c\u0430\u044f": 5,
        "\u0438\u044e\u043d\u044f": 6,
        "\u0438\u044e\u043b\u044f": 7,
        "\u0430\u0432\u0433\u0443\u0441\u0442\u0430": 8,
        "\u0441\u0435\u043d\u0442\u044f\u0431\u0440\u044f": 9,
        "\u043e\u043a\u0442\u044f\u0431\u0440\u044f": 10,
        "\u043d\u043e\u044f\u0431\u0440\u044f": 11,
        "\u0434\u0435\u043a\u0430\u0431\u0440\u044f": 12,
    }
    match = re.search(
        r"\b(\d{1,2})\s+([A-Za-z\u0400-\u04FF]+)(?:\s+(\d{4}))?(?:\s*(?:\u0440\u043e\u043a\u0443|\u0433\u043e\u0434\u0430|\u0440\.?))?\b",
        hint,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    day = int(match.group(1))
    month = month_map.get(match.group(2).lower())
    if not month:
        return None
    year = int(match.group(3)) if match.group(3) else now.year
    try:
        return datetime(year, month, day, tzinfo=UTC).isoformat()
    except ValueError:
        return None


def extract_numeric_media_id(value: str) -> str | None:
    if not value:
        return None
    for pattern in (
        r"/reel/(\d+)",
        r"/videos/(\d+)",
        r"[?&]fbid=(\d+)",
        r"[?&]story_fbid=(\d+)",
    ):
        match = re.search(pattern, value)
        if match:
            return match.group(1)
    return None


def epoch_seconds_to_iso(raw_value: str) -> str | None:
    try:
        epoch_value = int(raw_value)
    except (TypeError, ValueError):
        return None
    earliest_epoch = int(datetime(2010, 1, 1, tzinfo=UTC).timestamp())
    latest_epoch = int((datetime.now(tz=UTC) + timedelta(days=7)).timestamp())
    if epoch_value < earliest_epoch or epoch_value > latest_epoch:
        return None
    return datetime.fromtimestamp(epoch_value, tz=UTC).replace(microsecond=0).isoformat()


def extract_embedded_published_at(
    html: str,
    *,
    detail_url: str,
    post_permalink: str | None,
) -> str | None:
    if not html:
        return None

    patterns: list[str] = []
    for value in (detail_url, post_permalink or ""):
        media_id = extract_numeric_media_id(value)
        if not media_id:
            continue
        escaped_media_id = re.escape(media_id)
        patterns.extend(
            [
                rf'"id":"{escaped_media_id}".{{0,2500}}?"creation_time":(\d{{9,}})',
                rf'"creation_time":(\d{{9,}}).{{0,2500}}?"id":"{escaped_media_id}"',
                rf'"id":"{escaped_media_id}".{{0,2500}}?"publish_time":(\d{{9,}})',
                rf'"publish_time":(\d{{9,}}).{{0,2500}}?"id":"{escaped_media_id}"',
                rf'"story_fbid":\["{escaped_media_id}"\].{{0,1200}}?"publish_time":(\d{{9,}})',
            ]
        )

    patterns.extend(
        [
            r'"post_id":"[^"]+","creation_time":(\d{9,}),"unpublished_content_type"',
            r'"publish_time":(\d{9,}),"story_name"',
            r'"creation_time":(\d{9,})',
            r'"publish_time":(\d{9,})',
        ]
    )

    for pattern in patterns:
        match = re.search(pattern, html, flags=re.DOTALL)
        if not match:
            continue
        timestamp = epoch_seconds_to_iso(match.group(1))
        if timestamp:
            return timestamp
    return None
