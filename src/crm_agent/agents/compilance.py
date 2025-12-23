from __future__ import annotations

import re
from typing import Dict, Any, List


SLOT_PATTERN = re.compile(r"\{([a-zA-Z0-9_]+)\}")


def _extract_slots(text: str) -> List[str]:
    return sorted(set(SLOT_PATTERN.findall(text or "")))


def validate_candidates(candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    결과 형태:
    {
      "results": [
        {"template_id": "...", "status": "PASS|WARN|FAIL", "reasons": [...], "found_slots":[...]}
      ]
    }
    """
    results = []

    for c in candidates:
        tid = c.get("template_id", "")
        body = c.get("body_with_slots", "") or ""
        schema = (c.get("slot_schema") or {})
        required = schema.get("required") or []

        found = _extract_slots(body)
        missing = [s for s in required if s not in found]

        status = "PASS"
        reasons = []

        # ✅ 필수 슬롯 누락이면 FAIL (slot 없는 템플릿 방지)
        if missing:
            status = "FAIL"
            reasons.append(f"필수 슬롯 누락: {missing}")

        # 과장/확정 표현 (샘플)
        if any(x in body for x in ["100% 효과", "완치", "무조건"]):
            status = "FAIL"
            reasons.append("과장/확정 표현 가능성")

        # 너무 길면 WARN (채널별 상세 룰은 추후 강화)
        if len(body) > 220:
            if status != "FAIL":
                status = "WARN"
            reasons.append("문구가 길 수 있음(채널별 길이 가이드 확인 필요)")

        results.append(
            {
                "template_id": tid,
                "status": status,
                "reasons": reasons,
                "found_slots": found,
            }
        )

    return {"results": results}
