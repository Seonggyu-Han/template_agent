from __future__ import annotations

from typing import Dict, Any, List
import os
import json
import re

from crm_agent.services.tone_guide import load_tone_guide
from crm_agent.agents.brief_normalizer import normalize_campaign_text


REQUIRED_SLOTS_BY_CHANNEL = {
    "PUSH": ["customer_name", "product_name", "offer", "cta"],
    "SMS": ["customer_name", "product_name", "offer", "cta", "unsubscribe"],
    "KAKAO": ["customer_name", "product_name", "offer", "cta"],
    "EMAIL": ["customer_name", "product_name", "offer", "cta", "subject"],
}

OPTIONAL_SLOTS = [
    "coupon_code",
    "expiry_date",
    "deep_link",
    "brand_name",
    "support_contact",
]


def _normalize_channel(channel: str) -> str:
    c = (channel or "").strip().upper()
    if c in ("PUSH", "SMS", "KAKAO", "EMAIL"):
        return c
    return "PUSH"


def _slot_placeholders_in_text(text: str) -> set[str]:
    return set(re.findall(r"\{([a-zA-Z0-9_]+)\}", text or ""))


def _validate_candidate_body(body: str, required_slots: List[str]) -> List[str]:
    present = _slot_placeholders_in_text(body)
    return [s for s in required_slots if s not in present]


def _format_normalized_campaign_text(normalized: Dict[str, Any], raw_campaign_text: str) -> str:
    keywords = normalized.get("keywords") or []
    if not isinstance(keywords, list):
        keywords = []

    normalized_text = (normalized.get("normalized_text") or "").strip()
    category = (normalized.get("category") or "").strip()
    occasion = (normalized.get("occasion") or "").strip()

    finish = normalized.get("finish_or_texture") or []
    style = normalized.get("mood_or_style") or []
    negative = normalized.get("negative") or []

    parts = []
    if normalized_text:
        parts.append(f"- 요약: {normalized_text}")
    if keywords:
        parts.append(f"- 키워드: {', '.join([str(k) for k in keywords[:12]])}")
    if category:
        parts.append(f"- 카테고리(추정): {category}")
    if occasion:
        parts.append(f"- 상황/목적(추정): {occasion}")
    if finish:
        parts.append(f"- 제형/피니시: {', '.join([str(x) for x in finish[:8]])}")
    if style:
        parts.append(f"- 무드/스타일: {', '.join([str(x) for x in style[:8]])}")
    if negative:
        parts.append(f"- 제외조건: {', '.join([str(x) for x in negative[:8]])}")

    parts.append(f"- 원문: {raw_campaign_text}")
    return "\n".join(parts).strip()


def _fallback_openers_ctas_by_brand(tone_id: str) -> tuple[list[str], list[str]]:
    tone_id = (tone_id or "").strip().lower()
    if tone_id == "innisfree":
        openers = [
            "고객님, 오늘은 산뜻한 데일리 루틴으로 추천드려요 🍃",
            "가볍게 루틴에 더해보기 좋은 {product_name} 소식이에요 🍃",
        ]
        ctas = ["앱에서 확인하기", "가볍게 보러가기"]
        return openers, ctas

    # default: amoremall
    openers = [
        "고객님, 회원 전용 혜택 안내드려요.",
        "고객님, 지금 앱에서 확인해 보세요.",
    ]
    ctas = ["지금 확인하기", "자세히 보기"]
    return openers, ctas


def _fallback_candidates(
        channel: str,
        tone: str,
        brief: dict,
        rag_context: str,
        k: int,
) -> Dict[str, Any]:
    channel = _normalize_channel(channel)
    required = REQUIRED_SLOTS_BY_CHANNEL[channel]

    raw_campaign_text = (brief or {}).get("campaign_text", "").strip()
    campaign_goal = (brief or {}).get("goal", "").strip()
    evidence_hint = (rag_context or "").strip()[:500]

    tone_id = (tone or "amoremall").strip().lower()
    tone_guide = load_tone_guide(tone_id)

    normalized = normalize_campaign_text(raw_campaign_text)
    normalized_prompt_text = _format_normalized_campaign_text(normalized, raw_campaign_text)

    openers, ctas = _fallback_openers_ctas_by_brand(tone_id)

    footer = "수신거부: {unsubscribe}" if channel == "SMS" else ""
    default_subject = "{campaign_goal} 안내 | {product_name} {offer}"

    notes = {
        "campaign_goal": campaign_goal,
        "campaign_text_hint": normalized_prompt_text[:300],
        "rag_evidence_hint": evidence_hint,
        "brand_tone_id": tone_id,
        "brand_tone_guide_snippet": (tone_guide[:500] if tone_guide else ""),
        "principle": "Template agent must not decide product/offer. Keep as slots.",
        "fallback": True,
        "campaign_text_normalized": normalized,
    }

    candidates: List[Dict[str, Any]] = [
        {
            "template_id": "T001",
            "title": f"{tone_id} | 요약형(FALLBACK)",
            "slot_schema": {"required": required, "optional": OPTIONAL_SLOTS},
            "body_with_slots": (
                f"{openers[0]}\n"
                f"이번 캠페인에 딱 맞는 {{product_name}} 안내드려요.\n"
                f"{{offer}}\n"
                f"👉 {ctas[0]}: {{cta}}\n"
                f"{footer}"
            ).strip(),
            "channel": channel,
            "tone": tone_id,
            "notes": notes,
            "default_slot_values": {
                "cta": "{deep_link}",
                "subject": default_subject if channel == "EMAIL" else "",
            },
        },
        {
            "template_id": "T002",
            "title": f"{tone_id} | 혜택/리마인드(FALLBACK)",
            "slot_schema": {"required": required, "optional": OPTIONAL_SLOTS},
            "body_with_slots": (
                f"{openers[1]}\n"
                f"{{product_name}} 관련 안내예요.\n"
                f"{{offer}}\n"
                f"쿠폰: {{coupon_code}} / 종료일: {{expiry_date}}\n"
                f"✅ {ctas[1]}: {{cta}}\n"
                f"{footer}"
            ).strip(),
            "channel": channel,
            "tone": tone_id,
            "notes": notes,
            "default_slot_values": {
                "coupon_code": "{coupon_code}",
                "expiry_date": "{expiry_date}",
                "cta": "{deep_link}",
                "subject": default_subject if channel == "EMAIL" else "",
            },
        },
        {
            "template_id": "T003",
            "title": f"{tone_id} | 개인화(FALLBACK)",
            "slot_schema": {
                "required": required,
                "optional": OPTIONAL_SLOTS + ["skin_concern_primary", "sensitivity_level", "persona"],
            },
            "body_with_slots": (
                f"{openers[0]}\n"
                f"{{skin_concern_primary}} 고민을 고려해 {{product_name}}을(를) 제안드려요.\n"
                f"{{offer}}\n"
                f"👉 {{cta}}\n"
                f"{footer}"
            ).strip(),
            "channel": channel,
            "tone": tone_id,
            "notes": notes,
            "default_slot_values": {
                "cta": "{deep_link}",
                "subject": default_subject if channel == "EMAIL" else "",
            },
        },
        {
            "template_id": "T004",
            "title": f"{tone_id} | 초간단(FALLBACK)",
            "slot_schema": {"required": required, "optional": OPTIONAL_SLOTS},
            "body_with_slots": (
                f"{{customer_name}}님, {{product_name}}\n"
                f"{{offer}}\n"
                f"👉 {{cta}}\n"
                f"{footer}"
            ).strip(),
            "channel": channel,
            "tone": tone_id,
            "notes": notes,
            "default_slot_values": {
                "cta": "{deep_link}",
                "subject": default_subject if channel == "EMAIL" else "",
            },
        },
    ]

    return {"candidates": candidates[: max(1, min(k, 4))]}


def _build_prompt(
        *,
        channel: str,
        tone_id: str,
        tone_guide_md: str,
        campaign_goal: str,
        campaign_text_normalized: str,
        rag_context: str,
        required_slots: List[str],
        k: int,
) -> str:
    channel_guide = {
        "SMS": "SMS는 짧고 명확하게(가능하면 90자 내외), 수신거부 슬롯({unsubscribe})을 포함.",
        "PUSH": "PUSH는 1~2문장 + CTA 중심으로 간결하게.",
        "KAKAO": "KAKAO는 친근/가독성(줄바꿈) + CTA 명확.",
        "EMAIL": "EMAIL은 body는 짧게, subject는 슬롯/템플릿 형태로 제공 가능.",
    }.get(channel, "")

    tone_guide_block = tone_guide_md.strip() if tone_guide_md else "(없음: 기본 톤 가이드 + RAG 근거를 따르세요.)"

    return f"""
너는 화장품/뷰티 CRM 마케터를 돕는 "Template Agent"다.
중요 원칙:
- 절대 상품/혜택/가격/쿠폰을 확정하지 마라. 모든 변수는 반드시 슬롯(예: {{product_name}}, {{offer}})으로 남겨라.
- 고객에게 사실 단정/의학적 효능 단정/과장 표현 금지. (예: 100% 효과, 완치 등)
- 출력은 반드시 JSON만. 다른 설명/문장은 출력하지 마라.

[입력]
- channel: {channel}
- tone_id(brand): {tone_id}
- campaign_goal: {campaign_goal}
- campaign_text (normalized):
{campaign_text_normalized}

[브랜드 톤 가이드(md)]
{tone_guide_block}

[근거 컨텍스트(RAG 요약)]
{rag_context}

[슬롯 규칙]
- 필수 슬롯(required): {required_slots}
- 옵션 슬롯(optional): {OPTIONAL_SLOTS}
- body_with_slots에는 "필수 슬롯들이 모두 등장"해야 한다.
- 슬롯은 반드시 중괄호 한 쌍으로 표기: {{slot_name}}

[작성 가이드]
- {channel_guide}
- 브랜드 톤 가이드(md)를 최우선으로 지켜라. (금지/이모지 규칙 포함)
- {k}개의 서로 다른 템플릿을 만들어라. (동일 표현 반복 금지)
- CTA는 {{cta}} 슬롯을 사용하되, 라벨은 톤 가이드에 맞게 변주.

[출력 JSON 스키마]
{{
  "candidates": [
    {{
      "title": "설명",
      "body_with_slots": "슬롯 포함 본문",
      "default_slot_values": {{
        "cta": "{{deep_link}}",
        "subject": "{{campaign_goal}} 안내 | {{product_name}} {{offer}}"
      }}
    }}
  ]
}}

반드시 JSON만 출력.
""".strip()


def _call_openai(prompt: str) -> Dict[str, Any]:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing")

    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    from openai import OpenAI
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

    m = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not m:
        raise RuntimeError(f"LLM did not return JSON. RAW:\n{text[:1500]}")
    return json.loads(m.group(0))


def generate_template_candidates(
        *,
        brief: dict,
        channel: str,
        tone: str,
        rag_context: str,
        k: int = 4,
) -> Dict[str, Any]:
    channel = _normalize_channel(channel)
    required = REQUIRED_SLOTS_BY_CHANNEL[channel]

    tone_id = (tone or "amoremall").strip().lower()
    tone_guide_md = load_tone_guide(tone_id)

    raw_campaign_text = (brief or {}).get("campaign_text", "").strip()
    campaign_goal = (brief or {}).get("goal", "").strip() or (brief or {}).get("campaign_goal", "").strip()

    rag_context = (rag_context or "").strip()[:2500]

    normalized = normalize_campaign_text(raw_campaign_text)
    normalized_prompt_text = _format_normalized_campaign_text(normalized, raw_campaign_text)

    default_subject = "{campaign_goal} 안내 | {product_name} {offer}"

    notes = {
        "campaign_goal": campaign_goal,
        "campaign_text_hint": normalized_prompt_text[:300],
        "rag_evidence_hint": rag_context[:500],
        "brand_tone_id": tone_id,
        "brand_tone_guide_snippet": (tone_guide_md[:500] if tone_guide_md else ""),
        "principle": "Template agent must not decide product/offer. Keep as slots.",
        "llm": True,
        "campaign_text_normalized": normalized,
    }

    try:
        prompt = _build_prompt(
            channel=channel,
            tone_id=tone_id,
            tone_guide_md=tone_guide_md,
            campaign_goal=campaign_goal,
            campaign_text_normalized=normalized_prompt_text,
            rag_context=rag_context,
            required_slots=required,
            k=max(1, int(k)),
        )
        out = _call_openai(prompt)
    except Exception as e:
        fb = _fallback_candidates(channel=channel, tone=tone_id, brief=brief, rag_context=rag_context, k=k)
        for c in fb["candidates"]:
            c.setdefault("notes", {})
            c["notes"]["llm_error"] = repr(e)
        return fb

    raw_cands = (out or {}).get("candidates", []) or []
    if not isinstance(raw_cands, list) or not raw_cands:
        fb = _fallback_candidates(channel=channel, tone=tone_id, brief=brief, rag_context=rag_context, k=k)
        for c in fb["candidates"]:
            c.setdefault("notes", {})
            c["notes"]["llm_error"] = "LLM returned empty candidates"
        return fb

    final: List[Dict[str, Any]] = []
    for idx, rc in enumerate(raw_cands[: max(1, int(k))], start=1):
        title = (rc.get("title") or f"{tone_id} | 후보{idx}").strip()
        body = (rc.get("body_with_slots") or "").strip()

        missing = _validate_candidate_body(body, required_slots=required)
        if missing:
            body = (body + "\n" + "\n".join([f"{{{m}}}" for m in missing])).strip()

        dsv = rc.get("default_slot_values") or {}
        if not isinstance(dsv, dict):
            dsv = {}
        dsv.setdefault("cta", "{deep_link}")
        if channel == "EMAIL":
            dsv.setdefault("subject", default_subject)
        else:
            dsv.setdefault("subject", "")

        cand = {
            "template_id": f"T{idx:03d}",
            "title": title,
            "slot_schema": {"required": required, "optional": OPTIONAL_SLOTS},
            "body_with_slots": body,
            "channel": channel,
            "tone": tone_id,
            "notes": {**notes, "missing_slots_fixed": missing},
            "default_slot_values": dsv,
        }
        final.append(cand)

    if not final:
        return _fallback_candidates(channel=channel, tone=tone_id, brief=brief, rag_context=rag_context, k=k)

    return {"candidates": final[: max(1, int(k))]}
