from __future__ import annotations

import os
import warnings


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
        # A malformed integer used to silently become ``None`` and surface
        # later as a confusing "missing credentials" error; warn instead.
        warnings.warn(
            f"Environment variable {name}={value!r} is not a valid integer; ignoring it.",
            stacklevel=2,
        )
        return None
