from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime


def parse_configured_datetime(raw_value: str | None, *, end_of_day: bool) -> datetime | None:
    if not raw_value:
        return None
    try:
        if "T" in raw_value:
            parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        else:
            parsed = datetime.fromisoformat(f"{raw_value}T23:59:59+00:00" if end_of_day else f"{raw_value}T00:00:00+00:00")
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def normalize_datetime_value(raw_value: datetime | str | None) -> datetime | None:
    if raw_value is None:
        return None
    if isinstance(raw_value, datetime):
        parsed = raw_value
    else:
        try:
            parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class RangeFilter:
    start: datetime | None
    end: datetime | None

    @classmethod
    def from_strings(cls, start: str | None, end: str | None) -> "RangeFilter":
        return cls(
            start=parse_configured_datetime(start, end_of_day=False),
            end=parse_configured_datetime(end, end_of_day=True),
        )

    def contains(self, raw_value: datetime | str | None, *, allow_missing: bool = False) -> bool:
        current = normalize_datetime_value(raw_value)
        if current is None:
            return allow_missing
        if self.start and current < self.start:
            return False
        if self.end and current > self.end:
            return False
        return True
