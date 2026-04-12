from __future__ import annotations

import os


def env_value(name: str) -> str | None:
    value = os.getenv(name)
    return value if value else None


def env_int(name: str) -> int | None:
    value = env_value(name)
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None
