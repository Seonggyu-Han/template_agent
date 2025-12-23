from __future__ import annotations

import os
from collections import Counter
from dotenv import load_dotenv
from pinecone import Pinecone


def _get(obj, key, default=None):
    # FetchResponse가 dict처럼도/속성처럼도 올 수 있어서 방어
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def main():
    load_dotenv()

    api_key = os.getenv("PINECONE_API_KEY", "")
    index_name = os.getenv("PINECONE_INDEX", "pinecone-first")
    namespace = os.getenv("PINECONE_NAMESPACE")
    # ✅ 지금 통계 기준: default namespace('')에 41개가 있음
    # namespace를 명시하지 않거나 빈 값이면 default로 검사
    ns = "" if namespace is None or namespace == "" else namespace

    if not api_key:
        raise RuntimeError("PINECONE_API_KEY가 없습니다(.env 로드 확인).")

    pc = Pinecone(api_key=api_key)
    idx = pc.Index(index_name)

    stats = idx.describe_index_stats()
    print("=== describe_index_stats ===")
    print(stats)
    print()

    # 1) ID 일부를 뽑기 (list API 사용)
    ids = []
    try:
        # pinecone sdk의 list는 보통 iterator/페이징 형태
        # 최신 SDK: idx.list(namespace=..., limit=...)
        # page가 list[str]이거나 {'vectors': [...]} 같은 형태일 수 있어 방어
        for page in idx.list(namespace=ns, limit=100):
            if isinstance(page, dict):
                page_ids = page.get("vectors") or page.get("ids") or []
            else:
                page_ids = list(page)  # list[str] 형태 가정
            ids.extend(page_ids)
            if len(ids) >= 50:
                break
    except Exception as e:
        print("⚠️ index.list 실패:", e)
        print("대신 query 기반으로 샘플을 뽑아야 합니다(아래 안내 참고).")
        return

    if not ids:
        print("❗ namespace에서 id를 하나도 못 가져왔습니다. (ns=%r)" % ns)
        return

    ids = ids[:20]
    print(f"=== sample ids (ns={ns!r}) ===")
    for i, vid in enumerate(ids, 1):
        print(f"{i:02d}. {vid}")
    print()

    # 2) fetch로 metadata 확인
    fetched = idx.fetch(ids=ids, namespace=ns)
    vectors = _get(fetched, "vectors", {}) or {}

    # vectors가 dict가 아닐 수도 있어 방어(대개 dict임)
    if not isinstance(vectors, dict):
        try:
            vectors = dict(vectors)
        except Exception:
            vectors = {}

    # 3) source/doc_id/path 추정 키로 문서 분포 요약
    doc_counter = Counter()
    print("=== sample metadata preview ===")
    for vid, v in vectors.items():
        md = _get(v, "metadata", {}) or {}

        # 문서 출처 키 후보들
        source = (
            md.get("source")
            or md.get("path")
            or md.get("file")
            or md.get("doc_id")
            or md.get("document_id")
            or "UNKNOWN_SOURCE"
        )

        doc_counter[source] += 1

        text = md.get("text") or md.get("chunk") or md.get("content") or ""
        text_preview = (text[:180] + "...") if len(text) > 180 else text

        print(f"- id={vid}")
        print(f"  source={source}")
        if text_preview:
            print(f"  text_preview={text_preview}")
        else:
            print("  text_preview=(metadata에 text/chunk/content가 없음)")
        print()

    print("=== source distribution (top) ===")
    for src, cnt in doc_counter.most_common(30):
        print(f"{src}: {cnt}")


if __name__ == "__main__":
    main()
