from __future__ import annotations

from typing import Dict, Any
from datetime import datetime, timedelta
import random
import re


SLOT_PATTERN = re.compile(r"\{([a-zA-Z0-9_]+)\}")


def _default_slots(brief: dict) -> Dict[str, str]:
    # 기본 슬롯 값(실서비스에선 user_features 기반으로 더 채움)
    today = datetime.now().date()
    expiry = today + timedelta(days=7)

    return {
        "customer_name": "고객",
        "product_name": brief.get("product_name", "상품"),
        "benefit": brief.get("benefit", "혜택"),
        "coupon_code": random.choice(["AMORE10", "WELCOME5", "CARE15"]),
        "expiry_date": str(expiry),
        "deep_link": "https://example.com/campaign",
        "cta": "https://example.com/campaign",
        "unsubscribe": "080-0000-0000",
        "brand_name": "AMORE",
        "support_contact": "고객센터 080-0000-0000",
        "skin_concern_primary": brief.get("skin_concern_primary", "피부 고민"),
        "sensitivity_level": str(brief.get("sensitivity_level", "any")),
        "persona": "default",
        "subject": f"[{brief.get('goal','캠페인')}] {brief.get('product_name','상품')} {brief.get('benefit','혜택')}",
    }


def _render(text: str, slot_values: Dict[str, str]) -> str:
    def repl(match):
        k = match.group(1)
        return str(slot_values.get(k, f"{{{k}}}"))

    return SLOT_PATTERN.sub(repl, text or "")


def generate_final_message(*, brief: dict, selected_template: dict, rag_context: str = "") -> Dict[str, Any]:
    body = selected_template.get("body_with_slots", "") or ""
    slot_values = _default_slots(brief)

    # template에서 default_slot_values가 있으면 우선 적용
    defaults = selected_template.get("default_slot_values") or {}
    for k, v in defaults.items():
        slot_values[k] = str(v)

    # CTA가 "{deep_link}" 같은 형태라면 실제 deep_link로 치환
    if slot_values.get("cta") == "{deep_link}":
        slot_values["cta"] = slot_values.get("deep_link", slot_values["cta"])

    rendered = _render(body, slot_values)

    return {
        "used_template_id": selected_template.get("template_id"),
        "final_message": rendered,
        "slot_values": slot_values,
        "rag_used_hint": (rag_context or "")[:800],
    }
