from __future__ import annotations

from typing import Any, Dict, List
import os
import json
import re

from openai import OpenAI


SYSTEM = """
You are a campaign_text normalizer for cosmetics CRM.

Goal:
- Convert a short natural-language campaign_text into a compact structured object.

Rules:
- Output MUST be a single valid JSON object only (no markdown, no commentary).
- Do NOT invent brand/product names.
- Do NOT claim verified efficacy or medical benefits.
- Focus on intent/occasion/style/format/feel/finish/category.
- If uncertain, keep fields empty and set lower confidence.
- keywords must be 4~12 short Korean keywords (no full sentences).
- negative is only for explicit exclusions in the input (e.g., "무향만", "끈적임 싫어").
"""

USER_TEMPLATE = """
Input campaign_text (Korean):
{campaign_text}

Return JSON schema:
{{
  "normalized_text": "짧은 한 줄 요약(가능하면 명사형, 과장 금지)",
  "keywords": ["키워드1", "키워드2"],
  "category": "예: 아이섀도우/립/쿠션/크림/세럼/선케어 등 (추정 어려우면 빈 문자열)",
  "occasion": "예: 연말/파티/선물/데일리 등 (없으면 빈 문자열)",
  "finish_or_texture": ["예: 펄감/매트/글로시/촉촉/보송 등"],
  "mood_or_style": ["예: 화려한/은은한/고급스러운/산뜻한 등"],
  "negative": ["명시적 제외 조건만"],
  "confidence": 0.0
}}
"""


def _extract_json(text: str) -> Dict[str, Any]:
    m = re.search(r"\{.*\}", text or "", flags=re.DOTALL)
    if not m:
        raise RuntimeError(f"LLM did not return JSON. RAW:\n{text[:1500]}")
    return json.loads(m.group(0))


def _call_openai(prompt: str) -> Dict[str, Any]:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing")

    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    client = OpenAI(api_key=api_key)
    resp = client.responses.create(
        model=model,
        input=prompt,
    )

    text = getattr(resp, "output_text", None)
    if not text:
        try:
            text = json.dumps(resp.model_dump(), ensure_ascii=False)
        except Exception:
            text = str(resp)

    return _extract_json(text)


def normalize_campaign_text(campaign_text: str) -> Dict[str, Any]:
    """
    campaign_text를 LLM으로 정형화하여 반환.
    실패 시에도 상위 로직이 멈추지 않도록 안전한 fallback 구조를 반환.
    """
    campaign_text = (campaign_text or "").strip()

    # 빈 입력이면 그대로 빈 구조 반환
    if not campaign_text:
        return {
            "normalized_text": "",
            "keywords": [],
            "category": "",
            "occasion": "",
            "finish_or_texture": [],
            "mood_or_style": [],
            "negative": [],
            "confidence": 0.0,
        }

    prompt = (SYSTEM.strip() + "\n\n" + USER_TEMPLATE.format(campaign_text=campaign_text)).strip()

    try:
        out = _call_openai(prompt)
    except Exception as e:
        # fallback: 아주 단순 키워드화(공백/특수문자 기반)
        toks = re.sub(r"[^\w가-힣\s]", " ", campaign_text)
        toks = [t for t in toks.split() if t]
        keywords = toks[:8]

        return {
            "normalized_text": campaign_text[:60],
            "keywords": keywords,
            "category": "",
            "occasion": "",
            "finish_or_texture": [],
            "mood_or_style": [],
            "negative": [],
            "confidence": 0.2,
            "llm_error": repr(e),
        }

    # 필드 보정
    if not isinstance(out, dict):
        out = {}

    out.setdefault("normalized_text", "")
    out.setdefault("keywords", [])
    out.setdefault("category", "")
    out.setdefault("occasion", "")
    out.setdefault("finish_or_texture", [])
    out.setdefault("mood_or_style", [])
    out.setdefault("negative", [])
    out.setdefault("confidence", 0.5)

    # 타입 보정
    if not isinstance(out["keywords"], list):
        out["keywords"] = []
    if not isinstance(out["finish_or_texture"], list):
        out["finish_or_texture"] = []
    if not isinstance(out["mood_or_style"], list):
        out["mood_or_style"] = []
    if not isinstance(out["negative"], list):
        out["negative"] = []

    # 키워드 길이 제한/정리
    out["keywords"] = [str(k).strip() for k in out["keywords"] if str(k).strip()]
    out["keywords"] = out["keywords"][:12]

    # confidence 범위 보정
    try:
        c = float(out["confidence"])
        if c < 0:
            c = 0.0
        if c > 1:
            c = 1.0
        out["confidence"] = c
    except Exception:
        out["confidence"] = 0.5

    return out
