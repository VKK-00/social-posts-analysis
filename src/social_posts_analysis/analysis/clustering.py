from __future__ import annotations

import math
import re
from collections import Counter
from typing import Any

import numpy as np

from .providers import LLMProvider

# Multilingual function words for ru/uk/en that must never surface as cluster
# keywords. The c-TF-IDF weighting below already downweights terms shared
# across clusters; this list removes the residual glue vocabulary.
STOPWORDS = frozenset(
    {
        # English
        "the",
        "and",
        "that",
        "with",
        "this",
        "from",
        "they",
        "have",
        "was",
        "were",
        "are",
        "for",
        "not",
        "but",
        "all",
        "his",
        "her",
        "their",
        "about",
        "into",
        "over",
        "after",
        "under",
        "between",
        "will",
        "would",
        "there",
        "than",
        "then",
        "when",
        "what",
        "which",
        "who",
        "how",
        "why",
        "you",
        "your",
        "our",
        "out",
        "more",
        "also",
        "just",
        "being",
        # Ukrainian
        "для",
        "який",
        "яка",
        "які",
        "це",
        "цього",
        "цієї",
        "цей",
        "ця",
        "ще",
        "після",
        "також",
        "тому",
        "про",
        "але",
        "або",
        "якщо",
        "коли",
        "був",
        "була",
        "було",
        "були",
        "буде",
        "всі",
        "всіх",
        "його",
        "її",
        "їх",
        "щоб",
        "що",
        "того",
        "теж",
        "дуже",
        "свої",
        "свою",
        "ними",
        "нам",
        "нас",
        "вас",
        "може",
        "можна",
        "тільки",
        "понад",
        "між",
        "під",
        "над",
        "без",
        "мали",
        "має",
        # Russian
        "это",
        "как",
        "что",
        "чтобы",
        "которые",
        "который",
        "которая",
        "было",
        "были",
        "будет",
        "после",
        "также",
        "потому",
        "если",
        "когда",
        "все",
        "всех",
        "его",
        "её",
        "их",
        "очень",
        "можно",
        "нужно",
        "только",
        "более",
        "менее",
        "между",
        "под",
        "свои",
        "них",
        "есть",
    }
)

_KEYWORD_COUNT = 8


def _tokenize(text: str) -> list[str]:
    return [token for token in re.findall(r"[\w']+", text.lower()) if len(token) > 2 and token not in STOPWORDS]


def cluster_keywords(grouped_texts: dict[str, list[str]]) -> dict[str, list[str]]:
    """Class-based TF-IDF keywords per cluster (the c-TF-IDF idea from BERTopic).

    All texts of one cluster are treated as a single document:
    ``tf`` is the term count inside the cluster document and
    ``idf = ln(1 + avg_document_length / term_frequency_across_all_documents)``
    downweights vocabulary shared by many clusters, so discriminative terms
    outrank generic ones even when they are frequent overall.
    """
    counters = {
        cluster_id: Counter(token for text in texts for token in _tokenize(text))
        for cluster_id, texts in grouped_texts.items()
    }
    total_tokens = {cid: sum(counter.values()) for cid, counter in counters.items()}
    average_length = (sum(total_tokens.values()) / len(counters)) if counters else 0.0

    global_frequency: Counter[str] = Counter()
    for counter in counters.values():
        global_frequency.update(counter.keys())

    result: dict[str, list[str]] = {}
    for cluster_id, counter in counters.items():
        scores: dict[str, float] = {}
        for term, tf in counter.items():
            idf = math.log(1 + average_length / global_frequency[term]) if global_frequency[term] else 0.0
            scores[term] = tf * idf
        ranked = sorted(scores.items(), key=lambda entry: (-entry[1], entry[0]))
        result[cluster_id] = [term for term, _ in ranked[:_KEYWORD_COUNT]]
    return result


class NarrativeClusterer:
    def __init__(self, llm_provider: LLMProvider, exemplar_count: int, min_cluster_size: int, min_samples: int) -> None:
        self.llm_provider = llm_provider
        self.exemplar_count = exemplar_count
        self.min_cluster_size = min_cluster_size
        self.min_samples = min_samples

    def cluster_items(
        self,
        item_type: str,
        items: list[dict[str, Any]],
        embeddings: np.ndarray,
        run_id: str,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        if not items:
            return [], []

        labels = self._cluster_labels(embeddings)
        if len(labels) != len(items):
            labels = [0] * len(items)

        memberships: list[dict[str, Any]] = []
        grouped: dict[str, list[dict[str, Any]]] = {}
        for item, label in zip(items, labels, strict=False):
            cluster_key = f"{item_type}-{'noise' if label == -1 else label}"
            memberships.append(
                {
                    "item_type": item_type,
                    "item_id": item["item_id"],
                    "cluster_id": cluster_key,
                    "run_id": run_id,
                }
            )
            grouped.setdefault(cluster_key, []).append(item)

        summaries: list[dict[str, Any]] = []
        keywords_by_cluster = cluster_keywords(
            {cluster_id: [member["text"] for member in members] for cluster_id, members in grouped.items()}
        )
        for cluster_id, members in grouped.items():
            keywords = keywords_by_cluster.get(cluster_id, [])
            sorted_members = sorted(members, key=lambda entry: len(entry["text"]), reverse=True)
            exemplars = [item["item_id"] for item in sorted_members[: self.exemplar_count]]
            llm_summary = self.llm_provider.summarize_cluster(
                item_type=item_type,
                keywords=keywords,
                texts=[member["text"] for member in members[:3]],
            )
            summaries.append(
                {
                    "item_type": item_type,
                    "cluster_id": cluster_id,
                    "label": llm_summary.get("label") or cluster_id,
                    "description": llm_summary.get("description") or "",
                    "top_keywords": keywords,
                    "exemplar_ids": exemplars,
                    "run_id": run_id,
                }
            )
        return summaries, memberships

    def _cluster_labels(self, embeddings: np.ndarray) -> list[int]:
        if len(embeddings) == 0:
            return []
        if len(embeddings) < max(2, self.min_cluster_size):
            return [0] * len(embeddings)
        try:
            import hdbscan

            clusterer = hdbscan.HDBSCAN(
                min_cluster_size=min(self.min_cluster_size, len(embeddings)),
                min_samples=min(self.min_samples, max(len(embeddings) - 1, 1)),
            )
            labels = clusterer.fit_predict(embeddings)
            if len(set(labels)) == 1 and list(set(labels))[0] == -1:
                return [0] * len(embeddings)
            return list(map(int, labels))
        except Exception:
            return [0] * len(embeddings)
