from __future__ import annotations

from typing import Dict, Any
from datetime import date
from sqlalchemy import text

GENDER_DB = {"여": "F", "남": "M"}

def _age_group_to_birth_year_range(age_group: str) -> tuple[int | None, int | None]:
    this_year = date.today().year

    if age_group.endswith("대+"):
        base = int(age_group.replace("대+", ""))
        max_birth = this_year - base
        return (None, max_birth)

    base = int(age_group.replace("대", ""))
    min_age, max_age = base, base + 9

    min_birth = this_year - max_age
    max_birth = this_year - min_age
    return (min_birth, max_birth)


def _show_columns(db, table: str) -> set[str]:
    rows = db.execute(text(f"SHOW COLUMNS FROM {table}")).fetchall()
    return {r[0] for r in rows}


def _detect_join_keys(db) -> tuple[str | None, str | None]:
    """
    users ↔ user_features 조인 키 자동 탐지
    """
    try:
        u = _show_columns(db, "users")
        f = _show_columns(db, "user_features")
    except Exception:
        return None, None

    if "user_id" in u and "user_id" in f:
        return "user_id", "user_id"
    if "id" in u and "user_id" in f:
        return "id", "user_id"
    if "id" in u and "id" in f:
        return "id", "id"

    common = list(u.intersection(f))
    for cand in ["user_id", "id", "username"]:
        if cand in common:
            return cand, cand
    return None, None


def build_target(db, *, brief: dict, channel: str, tone: str) -> Dict[str, Any]:
    """
    ✅ 목표
    - Step2에서 고른 target_input(키워드)을 'DB 필터 가능한 형태'로 TARGET에 저장
    - (다음 단계) count/sample은 app에서 preview_target_users로 이미 보여줌
      -> 여기서는 workflow 내부에서도 query/summary를 만들도록 유지
    """
    target_input = (brief or {}).get("target_input", {}) or {}

    gender_ui = target_input.get("gender", []) or []
    age_groups = target_input.get("age_group", []) or []
    skin_type = target_input.get("skin_type", []) or []
    skin_concern = target_input.get("skin_concern", []) or []

    gender_db = [GENDER_DB[g] for g in gender_ui if g in GENDER_DB]
    birth_ranges = [_age_group_to_birth_year_range(a) for a in age_groups]

    # user_features 컬럼 존재하면 컬럼명을 함께 기록(나중에 SQL 생성/추적용)
    ukey, fkey = _detect_join_keys(db)
    feature_cols = {}
    try:
        fcols = _show_columns(db, "user_features")
        for cand in ["skin_type", "skin_type_primary"]:
            if cand in fcols:
                feature_cols["skin_type_col"] = cand
                break
        for cand in ["skin_concern", "skin_concern_primary"]:
            if cand in fcols:
                feature_cols["skin_concern_col"] = cand
                break
    except Exception:
        pass

    target_query = {
        "gender_in": gender_db,                 # ['F','M']
        "birth_year_ranges": birth_ranges,      # [(1996,2005), ...] or [(None, 1965)]
        "skin_type_in": skin_type,              # 키워드
        "skin_concern_in": skin_concern,        # 키워드
        "join_keys": {"users_key": ukey, "features_key": fkey},
        "feature_cols": feature_cols,
    }

    summary_parts = []
    if gender_ui: summary_parts.append(f"성별={','.join(gender_ui)}")
    if age_groups: summary_parts.append(f"나이대={','.join(age_groups)}")
    if skin_type: summary_parts.append(f"피부타입={','.join(skin_type)}")
    if skin_concern: summary_parts.append(f"피부고민={','.join(skin_concern)}")
    summary = " / ".join(summary_parts) if summary_parts else "타겟 조건 없음"

    return {
        "target_query": target_query,
        "summary": summary,
        "channel": channel,
        "tone": tone,
    }
