from __future__ import annotations

import re
from hashlib import sha256
from itertools import combinations
from typing import Any

import polars as pl

NEAR_DUPLICATES_SCHEMA: dict[str, Any] = {
    "item_type_a": pl.String,
    "item_id_a": pl.String,
    "item_type_b": pl.String,
    "item_id_b": pl.String,
    "similarity": pl.Float64,
    "run_id": pl.String,
}

_NUM_PERM = 128
_SHINGLE_SIZE = 3
# 16 bands x 8 rows: with 128 permutations this keeps random pairs below the
# threshold out of candidate sets while catching pairs at ~0.8+ similarity.
_BANDS = 16

_WORD_RE = re.compile(r"[\w']+", re.UNICODE)


def _shingles(text: str, size: int = _SHINGLE_SIZE) -> list[str]:
    tokens = _WORD_RE.findall((text or "").lower())
    if len(tokens) < size:
        return [" ".join(tokens)] if tokens else []
    return [" ".join(tokens[index : index + size]) for index in range(len(tokens) - size + 1)]


def minhash_signature(text: str, num_perm: int = _NUM_PERM) -> list[int]:
    """Deterministic MinHash signature over word shingles.

    Uses SHA-256 digests instead of Python's salted ``hash()``, so signatures
    are stable across processes and machines.
    """
    signature = [2**64 - 1] * num_perm
    for shingle in _shingles(text):
        digest = sha256(shingle.encode("utf-8")).digest()
        # Two independent 64-bit values per shingle: hash and a fixed odd multiplier.
        value = int.from_bytes(digest[:8], "big")
        salted = int.from_bytes(sha256(b"\x01" + shingle.encode("utf-8")).digest()[:8], "big")
        for index in range(num_perm):
            # Simple affine mixing of the two base hashes gives num_perm variants.
            mixed = ((value * (2 * index + 1)) + salted * (index + 3)) % (2**64)
            if mixed < signature[index]:
                signature[index] = mixed
    return signature


def estimated_jaccard(first: list[int], second: list[int]) -> float:
    if not first or not second:
        return 0.0
    matches = sum(1 for a, b in zip(first, second, strict=False) if a == b)
    return matches / len(first)


def find_near_duplicates(
    texts: dict[tuple[str, str], str],
    *,
    threshold: float = 0.8,
    run_id: str,
) -> pl.DataFrame:
    """Find near-duplicate text pairs among items via MinHash LSH banding.

    ``texts`` maps ``(item_type, item_id)`` to its text. Candidate pairs are
    generated with banding LSH over 128-perm MinHash signatures, then verified
    by estimated Jaccard similarity against ``threshold``. Output is sorted by
    descending similarity and deduplicated.
    """
    items = {key: text for key, text in texts.items() if (text or "").strip()}
    if len(items) < 2:
        return pl.DataFrame(schema=NEAR_DUPLICATES_SCHEMA)

    signatures = {key: minhash_signature(text) for key, text in items.items()}

    rows_per_band = max(_NUM_PERM // _BANDS, 1)
    buckets: dict[tuple[int, tuple[int, ...]], set[tuple[str, str]]] = {}
    for key, signature in signatures.items():
        for band in range(_BANDS):
            start = band * rows_per_band
            bucket_key = (band, tuple(signature[start : start + rows_per_band]))
            buckets.setdefault(bucket_key, set()).add(key)

    candidates: set[frozenset[tuple[str, str]]] = set()
    for members in buckets.values():
        if len(members) > 1:
            for first, second in combinations(sorted(members), 2):
                candidates.add(frozenset({first, second}))

    rows: list[dict[str, Any]] = []
    seen: set[frozenset[tuple[str, str]]] = set()
    for pair in candidates:
        if pair in seen:
            continue
        seen.add(pair)
        first_key, second_key = sorted(pair)
        similarity = estimated_jaccard(signatures[first_key], signatures[second_key])
        if similarity >= threshold:
            (type_a, id_a), (type_b, id_b) = first_key, second_key
            rows.append(
                {
                    "item_type_a": type_a,
                    "item_id_a": id_a,
                    "item_type_b": type_b,
                    "item_id_b": id_b,
                    "similarity": similarity,
                    "run_id": run_id,
                }
            )

    if not rows:
        return pl.DataFrame(schema=NEAR_DUPLICATES_SCHEMA)
    return pl.DataFrame(rows, schema=NEAR_DUPLICATES_SCHEMA).sort("similarity", descending=True)


def near_duplicate_texts(items: list[dict[str, Any]], threshold: float, run_id: str) -> pl.DataFrame:
    """Adapter from analysis item dicts ({item_type, item_id, text}) to LSH."""
    texts = {
        (str(item.get("item_type") or "post"), str(item["item_id"])): str(item.get("text") or "") for item in items
    }
    return find_near_duplicates(texts, threshold=threshold, run_id=run_id)
