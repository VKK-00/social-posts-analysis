from __future__ import annotations

import numpy as np

from social_posts_analysis.analysis.providers import ProvidersBundle
from social_posts_analysis.analysis.service import AnalysisService
from social_posts_analysis.normalize import NormalizationService


def test_analysis_service_reuses_embedding_and_stance_cache(project_config, project_paths, monkeypatch) -> None:
    run_id = "20260402T120000Z"
    project_config.providers.embeddings.dimension = 8
    NormalizationService(project_config, project_paths).run(run_id=run_id)

    class CountingEmbeddings:
        name = "counting_embedding"

        def __init__(self) -> None:
            self.calls = 0

        def embed_texts(self, texts: list[str]) -> np.ndarray:
            self.calls += len(texts)
            vectors = []
            for index, _text in enumerate(texts):
                vectors.append(np.full(project_config.providers.embeddings.dimension, float(index + 1)))
            return np.array(vectors, dtype=float)

    class CountingLLM:
        name = "counting_llm"

        def __init__(self) -> None:
            self.classify_calls = 0

        def summarize_cluster(self, item_type: str, keywords: list[str], texts: list[str]) -> dict[str, str]:
            return {"label": f"{item_type} cluster", "description": ", ".join(keywords[:3])}

        def classify_stance(self, text: str, side) -> dict[str, float | str]:  # noqa: ANN001
            self.classify_calls += 1
            return {"label": "support" if side.name.lower() in text.lower() else "neutral", "confidence": 0.9}

    embedding_provider = CountingEmbeddings()
    llm_provider = CountingLLM()
    bundle = ProvidersBundle(embeddings=embedding_provider, llm=llm_provider)

    monkeypatch.setattr("social_posts_analysis.analysis.service.build_providers", lambda *_args, **_kwargs: bundle)

    service = AnalysisService(project_config, project_paths)
    service.run(run_id=run_id)

    first_embedding_calls = embedding_provider.calls
    first_classify_calls = llm_provider.classify_calls

    assert first_embedding_calls > 0
    assert first_classify_calls > 0
    assert (project_paths.processed_root / "embedding_cache.parquet").exists()
    assert (project_paths.processed_root / "stance_cache.parquet").exists()

    service.run(run_id=run_id)

    assert embedding_provider.calls == first_embedding_calls
    assert llm_provider.classify_calls == first_classify_calls
