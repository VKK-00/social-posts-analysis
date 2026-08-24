from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class LanguagePrediction:
    language: str
    confidence: float
    method: str


class LanguageDetector:
    """Language detection with a configurable backend and heuristic fallback.

    Backends:
    - ``fasttext``: Facebook's lid.176 model via the optional
      ``fasttext-langdetect`` package (accurate on short texts; downloads the
      model on first use). Install with ``pip install fasttext-langdetect``.
    - ``langdetect`` (default): the deterministic seeded port used historically.

    Both fall back to script/lexicon heuristics so detection never hard-fails
    a run.
    """

    def __init__(self, allowed_languages: list[str], method: str = "langdetect") -> None:
        self.allowed_languages = allowed_languages
        self.method = method if method in {"langdetect", "fasttext"} else "langdetect"

    def detect(self, text: str | None) -> LanguagePrediction:
        normalized = (text or "").strip()
        if not normalized:
            return LanguagePrediction(language="unknown", confidence=0.0, method="empty")

        if self.method == "fasttext":
            fast_prediction = self._detect_fasttext(normalized)
            if fast_prediction is not None:
                return fast_prediction

        return self._detect_langdetect(normalized)

    def _detect_fasttext(self, normalized: str) -> LanguagePrediction | None:
        try:
            from fast_langdetect import detect

            try:
                result: Any = detect(normalized, low_memory=False)
            except TypeError:
                # Older/newer package versions expose different signatures.
                result = detect(normalized)
        except Exception:
            # Package missing or model unavailable: fall back quietly.
            return None

        if isinstance(result, list):
            payload = max(result, key=lambda item: item.get("score", 0.0)) if result else {}
        elif isinstance(result, dict):
            payload = result
        else:
            return None

        language = str(payload.get("lang") or "").lower()
        if not language:
            return None
        confidence = float(payload.get("score") or 0.0)
        if language not in self.allowed_languages:
            # A confident out-of-scope result is still more informative than
            # the cyrillic heuristics, which would guess ru for Ukrainian.
            return LanguagePrediction(language=language, confidence=confidence, method="fasttext")
        return LanguagePrediction(language=language, confidence=confidence, method="fasttext")

    def _detect_langdetect(self, normalized: str) -> LanguagePrediction:
        try:
            from langdetect import DetectorFactory, detect_langs

            DetectorFactory.seed = 0
            predictions = detect_langs(normalized)
            if predictions:
                best = max(predictions, key=lambda item: item.prob)
                language = best.lang
                if language in self.allowed_languages:
                    return LanguagePrediction(language=language, confidence=float(best.prob), method="langdetect")
        except Exception:
            pass

        lowered = normalized.lower()
        if re.search(r"[іїєґ]", lowered):
            return LanguagePrediction(language="uk", confidence=0.72, method="heuristic-script")
        if re.search(r"[ыэёъ]", lowered):
            return LanguagePrediction(language="ru", confidence=0.72, method="heuristic-script")
        if re.search(r"[а-я]", lowered):
            return LanguagePrediction(language="ru", confidence=0.58, method="heuristic-cyrillic")

        english_markers = {"the", "and", "with", "that", "this", "from", "will"}
        tokens = set(re.findall(r"[a-zA-Z']+", lowered))
        if tokens & english_markers:
            return LanguagePrediction(language="en", confidence=0.6, method="heuristic-lexicon")
        if re.search(r"[a-z]", lowered):
            return LanguagePrediction(language="en", confidence=0.45, method="heuristic-latin")
        return LanguagePrediction(language="unknown", confidence=0.2, method="heuristic-fallback")
