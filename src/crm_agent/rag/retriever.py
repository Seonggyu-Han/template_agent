from __future__ import annotations

import os
from collections import defaultdict
from typing import Any, Dict, Optional, List

from dotenv import load_dotenv
from openai import OpenAI
from pinecone import Pinecone


class RagRetriever:
    def __init__(self):
        load_dotenv(override=True)

        self.pinecone_key = os.getenv("PINECONE_API_KEY", "")
        self.index_name = os.getenv("PINECONE_INDEX", "pinecone-first")
        self.namespace = os.getenv("PINECONE_NAMESPACE", "amore_crm_agent")

        self.openai_key = os.getenv("OPENAI_API_KEY", "")
        self.embed_model = os.getenv("OPENAI_EMBED_MODEL", "text-embedding-3-small")

        if not self.pinecone_key:
            raise RuntimeError("PINECONE_API_KEY가 없습니다 (.env 확인).")
        if not self.openai_key:
            raise RuntimeError("OPENAI_API_KEY가 없습니다 (.env 확인).")

        self.pc = Pinecone(api_key=self.pinecone_key)
        self.idx = self.pc.Index(self.index_name)
        self.oa = OpenAI(api_key=self.openai_key)

    def retrieve(
        self,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
        top_k: int = 10,
    ) -> Dict[str, Any]:
        q_emb = self.oa.embeddings.create(model=self.embed_model, input=query).data[0].embedding

        res = self.idx.query(
            vector=q_emb,
            top_k=top_k,
            namespace=self.namespace,
            include_metadata=True,
            filter=filters or None,
        )

        matches = []
        for m in (getattr(res, "matches", []) or []):
            md = getattr(m, "metadata", {}) or {}
            matches.append(
                {
                    "id": getattr(m, "id", ""),
                    "score": float(getattr(m, "score", 0.0)),
                    "metadata": md,
                }
            )

        return {
            "query": query,
            "top_k": top_k,
            "namespace": self.namespace,
            "matches": matches,
        }


def build_context_text(retrieved: Dict[str, Any], max_each: int = 3) -> str:
    """
    source별 최대 max_each개 chunk까지만 컨텍스트에 포함.
    ingest.py에서 metadata["text"]를 넣어야 이게 의미 있게 동작함.
    """
    matches = retrieved.get("matches", []) or []

    per_source = defaultdict(int)
    blocks: List[str] = []

    for m in matches:
        md = (m.get("metadata") or {})
        source = md.get("source", "UNKNOWN")
        section = md.get("section", "")
        chunk_id = md.get("chunk_id", "")
        text = (md.get("text") or "").strip()
        if not text:
            continue

        if per_source[source] >= max_each:
            continue
        per_source[source] += 1

        score = float(m.get("score", 0.0))
        header = f"[{source} | {section} | chunk={chunk_id} | score={score:.3f}]"
        blocks.append(header + "\n" + text)

    return "\n\n".join(blocks)
