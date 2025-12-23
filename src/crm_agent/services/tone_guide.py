from __future__ import annotations

from pathlib import Path
from typing import Dict


# RAG 코퍼스와 동일 위치의 md를 "브랜드 톤 가이드"로도 사용
_CORPUS_DIR = Path(__file__).resolve().parents[1] / "rag" / "corpus"

# tone_id -> md filename
_TONE_GUIDE_FILES: Dict[str, str] = {
    "amoremall": "amoremall.md",
    "innisfree": "innisfree.md",
}


def list_tone_ids() -> list[str]:
    return sorted(_TONE_GUIDE_FILES.keys())


def load_tone_guide(tone_id: str) -> str:
    """
    tone_id(=브랜드)별 md 가이드를 로드해서 문자열로 반환.
    파일이 없으면 빈 문자열 반환(LLM은 RAG/기본 가이드로라도 동작).
    """
    key = (tone_id or "").strip().lower()
    fname = _TONE_GUIDE_FILES.get(key)
    if not fname:
        return ""

    fp = _CORPUS_DIR / fname
    if not fp.exists():
        return ""

    try:
        return fp.read_text(encoding="utf-8").strip()
    except Exception:
        return ""
