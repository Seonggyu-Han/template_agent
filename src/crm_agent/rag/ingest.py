from __future__ import annotations

import os
import re
import hashlib
import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Optional

from dotenv import load_dotenv
from openai import OpenAI
from pinecone import Pinecone


CORPUS_DIR = Path(__file__).parent / "corpus"


@dataclass
class Chunk:
    id: str
    text: str
    metadata: Dict[str, str]


def _clean_text(s: str) -> str:
    s = s.replace("\r\n", "\n").strip()
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s


def _split_markdown_into_sections(md: str) -> List[Tuple[str, str]]:
    """
    (section_title, section_text) 리스트로 분할.
    간단히 #~###### heading을 기준으로 섹션을 나눔.
    """
    lines = md.splitlines()
    sections: List[Tuple[str, List[str]]] = []
    current_title = "ROOT"
    current_buf: List[str] = []

    heading_re = re.compile(r"^(#{1,6})\s+(.*)\s*$")

    for line in lines:
        m = heading_re.match(line)
        if m:
            if current_buf:
                sections.append((current_title, current_buf))
            current_title = (m.group(2) or "").strip() or "UNTITLED"
            current_buf = [line]
        else:
            current_buf.append(line)

    if current_buf:
        sections.append((current_title, current_buf))

    return [(title, _clean_text("\n".join(buf))) for title, buf in sections]


def _chunk_text(text: str, max_chars: int = 1200, overlap: int = 150) -> List[str]:
    """
    문자 기반 chunking + overlap.
    """
    text = _clean_text(text)
    if not text:
        return []

    if len(text) <= max_chars:
        return [text]

    chunks: List[str] = []
    start = 0
    while start < len(text):
        end = min(start + max_chars, len(text))
        part = text[start:end].strip()
        if part:
            chunks.append(part)
        if end == len(text):
            break
        start = max(0, end - overlap)
    return chunks


def _stable_id(source: str, idx: int, text: str) -> str:
    h = hashlib.sha1(f"{source}|{idx}|{text}".encode("utf-8")).hexdigest()[:16]
    return f"{Path(source).stem}_{idx:03d}_{h}"


def load_corpus(only_files: Optional[List[str]] = None) -> List[Tuple[str, str]]:
    """
    Returns list of (filename, markdown_text) where markdown_text is non-empty.

    only_files:
      - None: CORPUS_DIR/*.md 전체 로드
      - ["amoremall.md", "innisfree.md"]: 해당 파일만 로드 (존재/확장자 검증)
    """
    if not CORPUS_DIR.exists():
        raise FileNotFoundError(f"CORPUS_DIR not found: {CORPUS_DIR}")

    if only_files:
        # 입력값 정규화
        normalized = []
        for f in only_files:
            name = (f or "").strip()
            if not name:
                continue
            if not name.endswith(".md"):
                name += ".md"
            normalized.append(name)

        # 중복 제거(순서 유지)
        seen = set()
        only_files = []
        for n in normalized:
            if n not in seen:
                seen.add(n)
                only_files.append(n)

        items: List[Tuple[str, str]] = []
        missing = []
        for name in only_files:
            fp = CORPUS_DIR / name
            if not fp.exists():
                missing.append(name)
                continue
            raw = fp.read_text(encoding="utf-8")
            text = raw.strip()
            if text:
                items.append((fp.name, text))
        if missing:
            raise FileNotFoundError(
                f"요청한 md 파일을 찾을 수 없습니다: {missing}\n"
                f"경로: {CORPUS_DIR}"
            )
        return items

    files = sorted(CORPUS_DIR.glob("*.md"))
    items: List[Tuple[str, str]] = []
    for fp in files:
        raw = fp.read_text(encoding="utf-8")
        text = raw.strip()
        if text:
            items.append((fp.name, text))
    return items


def build_chunks(corpus: List[Tuple[str, str]]) -> List[Chunk]:
    all_chunks: List[Chunk] = []

    for source, md in corpus:
        sections = _split_markdown_into_sections(md)
        chunk_idx = 0

        for title, sec_text in sections:
            parts = _chunk_text(sec_text, max_chars=1200, overlap=150)
            for part in parts:
                cid = _stable_id(source, chunk_idx, part)
                meta = {
                    "source": source,
                    "section": title,
                    "chunk_id": str(chunk_idx),
                }
                all_chunks.append(Chunk(id=cid, text=part, metadata=meta))
                chunk_idx += 1

    return all_chunks


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest md corpus into Pinecone (RAG).")
    p.add_argument(
        "--files",
        nargs="*",
        default=None,
        help="업서트할 md 파일명만 지정 (예: --files amoremall.md innisfree.md). "
             "미지정 시 corpus/*.md 전체 업서트",
    )
    return p.parse_args()


def main():
    args = parse_args()
    load_dotenv(override=True)

    pinecone_key = os.getenv("PINECONE_API_KEY", "")
    index_name = os.getenv("PINECONE_INDEX", "pinecone-first")
    namespace = os.getenv("PINECONE_NAMESPACE", "amore_crm_agent")

    openai_key = os.getenv("OPENAI_API_KEY", "")
    embed_model = os.getenv("OPENAI_EMBED_MODEL", "text-embedding-3-small")

    if not pinecone_key:
        raise RuntimeError("PINECONE_API_KEY가 없습니다 (.env 확인).")
    if not openai_key:
        raise RuntimeError("OPENAI_API_KEY가 없습니다 (.env 확인).")

    corpus = load_corpus(only_files=args.files)
    if not corpus:
        raise RuntimeError(
            f"코퍼스가 비어있습니다. md 파일 내용이 0바이트/빈 문자열입니다.\n"
            f"경로: {CORPUS_DIR}\n"
            f"→ md 파일에 내용을 채운 뒤 다시 실행하세요."
        )

    chunks = build_chunks(corpus)
    if not chunks:
        raise RuntimeError("chunk 결과가 0개입니다. 코퍼스 내용을 확인하세요.")

    # Pinecone index 존재만 확인 (생성은 하지 않음)
    pc = Pinecone(api_key=pinecone_key)
    existing = [i["name"] for i in pc.list_indexes()]
    if index_name not in existing:
        raise RuntimeError(
            f"Pinecone index '{index_name}' 가 없습니다.\n"
            f"→ .env의 PINECONE_INDEX를 '존재하는 인덱스 이름'으로 바꾸세요."
        )

    idx = pc.Index(index_name)

    oa = OpenAI(api_key=openai_key)

    # embeddings 생성
    texts = [c.text for c in chunks]
    vectors = []

    BATCH = 96
    for b in range(0, len(texts), BATCH):
        batch_texts = texts[b: b + BATCH]
        emb_res = oa.embeddings.create(model=embed_model, input=batch_texts).data

        for j, e in enumerate(emb_res):
            c = chunks[b + j]
            meta = dict(c.metadata)
            # ✅ 추적 핵심: 원문 chunk를 metadata에 저장
            meta["text"] = c.text

            vectors.append(
                {
                    "id": c.id,
                    "values": e.embedding,
                    "metadata": meta,
                }
            )

    # upsert
    UPSERT_BATCH = 200
    for b in range(0, len(vectors), UPSERT_BATCH):
        idx.upsert(vectors=vectors[b: b + UPSERT_BATCH], namespace=namespace)

    stats = idx.describe_index_stats()

    print("✅ RAG ingest done")
    print(f"- index: {index_name}")
    print(f"- namespace: {namespace}")
    print(f"- files: {[name for name, _ in corpus]}")
    print(f"- chunks: {len(chunks)}")
    print(stats)


if __name__ == "__main__":
    main()
