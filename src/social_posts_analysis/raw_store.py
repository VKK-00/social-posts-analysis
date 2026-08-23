from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


class RawSnapshotStore:
    def __init__(self, run_dir: Path) -> None:
        self.run_dir = run_dir
        self.run_dir.mkdir(parents=True, exist_ok=True)

    def write_json(self, category: str, stem: str, payload: Any) -> Path:
        category_dir = self.run_dir / category
        category_dir.mkdir(parents=True, exist_ok=True)
        target_path = category_dir / f"{stem}.json"
        content = json.dumps(payload, ensure_ascii=False, indent=2)
        # Write to a temporary file in the same directory, then atomically
        # replace the target so a crash mid-write can never leave a torn or
        # partially written snapshot behind.
        temp_path = target_path.with_name(f"{target_path.name}.tmp-{os.getpid()}")
        try:
            temp_path.write_text(content, encoding="utf-8")
            os.replace(temp_path, target_path)
        finally:
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)
        return target_path

    def write_manifest(self, payload: dict[str, Any]) -> Path:
        return self.write_json("", "manifest", payload)
