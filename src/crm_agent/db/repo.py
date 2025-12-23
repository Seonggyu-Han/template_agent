from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any, Optional, Dict, List

from sqlalchemy import text
from sqlalchemy.orm import Session


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _uuid36() -> str:
    return str(uuid.uuid4())


def _upper_channel(channel: str) -> str:
    c = (channel or "").strip().upper()
    if c in ("SMS", "KAKAO", "PUSH", "EMAIL"):
        return c
    m = {"push": "PUSH", "sms": "SMS", "kakao": "KAKAO", "email": "EMAIL"}
    return m.get(c.lower(), "PUSH")


class Repo:
    def __init__(self, db: Session):
        self.db = db
    # repo.py 안 (class Repo 내부에 추가)
    def preview_target_users(self, target_input: Dict[str, Any], sample_size: int = 5) -> Dict[str, Any]:
        """
        target_input 예시:
        {
        "gender": ["F","M"],                 # users.gender
        "age_bands": ["20대","30대"],        # users.birth_year로 계산
        "skin_types": ["dry","oily"]         # user_features.skin_type
        }

        반환:
        {
        "count": int,
        "sample": [{"user_id":..., "gender":..., "birth_year":..., "age":..., "skin_type":...}, ...]
        }
        """
        gender_vals = (target_input or {}).get("gender") or []
        age_bands = (target_input or {}).get("age_bands") or []
        skin_vals = (target_input or {}).get("skin_types") or []

        where_clauses: List[str] = []
        params: Dict[str, Any] = {}

        # 1) gender filter
        if gender_vals:
            where_clauses.append("u.gender IN :genders")
            params["genders"] = tuple(gender_vals)

        # 2) skin_type filter (user_features join 필요)
        #    user_features에 skin_type 컬럼이 있어야 함(없으면 unknown만 나올 수 있음)
        if skin_vals:
            where_clauses.append("uf.skin_type IN :skin_types")
            params["skin_types"] = tuple(skin_vals)

        # 3) age band -> birth_year range (KST 기준 현재연도)
        #    나이 = current_year - birth_year + 1 (한국식) 대신,
        #    여기선 단순히 "연령대"를 birth_year 범위로 근사
        current_year = datetime.now().year

        # 밴드별 birth_year 범위(만 나이 기준 근사)
        # 20대 = 20~29세 => birth_year in [current_year-29, current_year-20]
        # 30대 = 30~39세 => [current_year-39, current_year-30]
        # 10대 = 10~19세 ...
        # 50대+ = 50~120세 => <= current_year-50
        band_ranges: List[Tuple[int, int]] = []
        for b in age_bands:
            b = str(b).strip()
            if b == "10대":
                band_ranges.append((current_year - 19, current_year - 10))
            elif b == "20대":
                band_ranges.append((current_year - 29, current_year - 20))
            elif b == "30대":
                band_ranges.append((current_year - 39, current_year - 30))
            elif b == "40대":
                band_ranges.append((current_year - 49, current_year - 40))
            elif b in ("50대+", "50대"):
                # 50대+는 하한만
                # (상한은 아주 과거 연도까지 허용)
                band_ranges.append((1900, current_year - 50))

        if band_ranges:
            ors = []
            for i, (y_min, y_max) in enumerate(band_ranges):
                # birth_year BETWEEN y_min AND y_max
                ors.append(f"(u.birth_year BETWEEN :by_min_{i} AND :by_max_{i})")
                params[f"by_min_{i}"] = y_min
                params[f"by_max_{i}"] = y_max
            where_clauses.append("(" + " OR ".join(ors) + ")")

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        # COUNT
        q_count = text(f"""
                SELECT COUNT(*) AS cnt
                FROM users u
                LEFT JOIN user_features uf ON uf.user_id = u.user_id
                {where_sql}
            """)
        cnt = self.db.execute(q_count, params).scalar() or 0

        # SAMPLE
        q_sample = text(f"""
                SELECT
                u.user_id,
                u.gender,
                u.birth_year,
                uf.skin_type
                FROM users u
                LEFT JOIN user_features uf ON uf.user_id = u.user_id
                {where_sql}
                ORDER BY u.user_id
                LIMIT :limit_n
            """)
        params2 = dict(params)
        params2["limit_n"] = int(sample_size)

        rows = self.db.execute(q_sample, params2).mappings().all()

        # age 계산(만 나이 근사)
        sample = []
        for r in rows:
            by = r.get("birth_year")
            age = None
            if by:
                age = current_year - int(by)
            sample.append({
                "user_id": r.get("user_id"),
                "gender": r.get("gender"),
                "birth_year": by,
                "age": age,
                "skin_type": r.get("skin_type"),
            })

        return {"count": int(cnt), "sample": sample}
    def get_gender_options_label(self):
        """
        UI용 성별 옵션(label) 반환
        DB 컬럼 값은 users.gender = 'F'/'M'
        """
        return ["여", "남"]

    def get_age_band_options_label(self):
        """
        UI용 나이대(label) 반환
        users.birth_year로 계산할 예정이므로 밴드만 제공
        """
        return ["10대", "20대", "30대", "40대", "50대+"]

    def get_skin_type_options_label(self):
        """
        UI용 피부타입(label) 반환
        (user_features.skin_type가 dry/oily/combination/normal/unknown 라는 전제)
        """
        return ["건성", "지성", "복합성", "중성"]


    # ---------------------------
    # users (FK 대비: 없으면 생성)
    # ---------------------------
    def ensure_user(self, user_id: str) -> None:
        """
        campaign_runs.user_id -> users.user_id FK 때문에
        user가 없으면 최소 row를 만들어준다.
        users 테이블 컬럼이 무엇인지 100% 확정이 안 되어도 동작하도록:
        - 일단 user_id 존재 여부만 체크
        - INSERT는 user_id만 넣는 형태로 시도
        """
        # 존재 여부
        row = self.db.execute(
            text("SELECT user_id FROM users WHERE user_id = :user_id LIMIT 1"),
            {"user_id": user_id},
        ).mappings().first()

        if row:
            return

        # users 테이블이 user_id만으로 INSERT 가능한 스키마라고 가정(대부분 PK만 필수)
        # 만약 NOT NULL 컬럼이 더 있다면 여기서 다시 에러가 날 것이고,
        # 그때 users DESC 결과 보고 정확히 맞춰줄게.
        self.db.execute(
            text("INSERT INTO users (user_id) VALUES (:user_id)"),
            {"user_id": user_id},
        )
        self.db.commit()

    # ---------------------------
    # campaign_runs
    # ---------------------------
    def create_run(self, created_by: str, brief: dict, channel: str = None):
        run_id = str(uuid.uuid4())

        tone = (brief.get("tone_hint") or None)
        if isinstance(tone, str):
            tone = tone.strip().lower()

        self.db.execute(
            text("""
                INSERT INTO campaign_runs
                (run_id, created_by, status, step_id, brief_json, channel, tone)
                VALUES
                (:run_id, :created_by, :status, :step_id, :brief_json, :channel, :tone)
            """),
            {
                "run_id": run_id,
                "created_by": created_by,
                "status": "CREATED",
                "step_id": "S1_BRIEF",
                "brief_json": json.dumps(brief, ensure_ascii=False),
                "channel": channel,
                "tone": tone,
            }
        )
        self.db.commit()
        return run_id
    
    def get_run(self, run_id: str) -> Optional[dict]:
        row = self.db.execute(
            text("SELECT * FROM campaign_runs WHERE run_id = :run_id"),
            {"run_id": run_id},
        ).mappings().first()

        if not row:
            return None

        brief_h = self.get_latest_handoff(run_id, "BRIEF")
        brief_json = brief_h["payload_json"] if brief_h else {"goal": row.get("campaign_goal")}

        return {**dict(row), "brief_json": brief_json}

    def update_run(
            self,
            run_id: str,
            *,
            channel: Optional[str] = None,
            campaign_goal: Optional[str] = None,
            step_id: Optional[str] = None,
            candidate_id: Optional[str] = None,
            status: Optional[str] = None,
            rendered_text: Optional[str] = None,
            error_code: Optional[str] = None,
            error_message: Optional[str] = None,
            sent_at: Optional[str] = None,
    ) -> None:
        sets = []
        params: Dict[str, Any] = {"run_id": run_id}

        if channel is not None:
            sets.append("channel = :channel")
            params["channel"] = _upper_channel(channel)

        if campaign_goal is not None:
            sets.append("campaign_goal = :campaign_goal")
            params["campaign_goal"] = campaign_goal

        if step_id is not None:
            sets.append("step_id = :step_id")
            params["step_id"] = step_id[:16]

        if candidate_id is not None:
            sets.append("candidate_id = :candidate_id")
            params["candidate_id"] = candidate_id[:16] if candidate_id else None

        if status is not None and status in ("CREATED", "SENT", "FAILED", "SKIPPED"):
            sets.append("status = :status")
            params["status"] = status

        if rendered_text is not None:
            sets.append("rendered_text = :rendered_text")
            params["rendered_text"] = rendered_text

        if error_code is not None:
            sets.append("error_code = :error_code")
            params["error_code"] = error_code

        if error_message is not None:
            sets.append("error_message = :error_message")
            params["error_message"] = error_message

        if sent_at is not None:
            sets.append("sent_at = :sent_at")
            params["sent_at"] = sent_at

        if not sets:
            return

        sql = "UPDATE campaign_runs SET " + ", ".join(sets) + " WHERE run_id = :run_id"
        self.db.execute(text(sql), params)
        self.db.commit()

    # ---------------------------
    # handoffs
    # ---------------------------
    def create_handoff(self, run_id: str, stage: str, payload: dict, payload_version: int = 1) -> str:
        handoff_id = _uuid36()
        created_at = _now_str()

        self.db.execute(
            text(
                """
                INSERT INTO handoffs
                (handoff_id, run_id, stage, payload_json, payload_version, created_at)
                VALUES
                (:handoff_id, :run_id, :stage, CAST(:payload_json AS JSON), :payload_version, :created_at)
                """
            ),
            {
                "handoff_id": handoff_id,
                "run_id": run_id,
                "stage": stage,
                "payload_json": json.dumps(payload, ensure_ascii=False),
                "payload_version": payload_version,
                "created_at": created_at,
            },
        )
        self.db.commit()
        return handoff_id

    def get_latest_handoff(self, run_id: str, stage: str) -> Optional[dict]:
        row = self.db.execute(
            text(
                """
                SELECT handoff_id, run_id, stage, payload_json, payload_version, created_at
                FROM handoffs
                WHERE run_id = :run_id AND stage = :stage
                ORDER BY created_at DESC
                LIMIT 1
                """
            ),
            {"run_id": run_id, "stage": stage},
        ).mappings().first()

        if not row:
            return None

        payload = row["payload_json"]
        if isinstance(payload, str):
            payload = json.loads(payload)

        return {**dict(row), "payload_json": payload}

    def list_handoffs(self, run_id: str) -> List[dict]:
        rows = self.db.execute(
            text(
                """
                SELECT handoff_id, run_id, stage, payload_json, payload_version, created_at
                FROM handoffs
                WHERE run_id = :run_id
                ORDER BY created_at ASC
                """
            ),
            {"run_id": run_id},
        ).mappings().all()

        out = []
        for r in rows:
            payload = r["payload_json"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            out.append({**dict(r), "payload_json": payload})
        return out

    # approvals -> handoff로 저장
    def add_approval(self, run_id: str, marketer_id: str, decision: str, comment: str = "") -> str:
        payload = {
            "marketer_id": marketer_id,
            "decision": decision,
            "comment": comment,
            "created_at": _now_str(),
        }
        return self.create_handoff(run_id, "APPROVAL", payload)

    def list_approvals(self, run_id: str) -> List[dict]:
        rows = self.db.execute(
            text(
                """
                SELECT handoff_id, run_id, stage, payload_json, payload_version, created_at
                FROM handoffs
                WHERE run_id = :run_id AND stage = 'APPROVAL'
                ORDER BY created_at ASC
                """
            ),
            {"run_id": run_id},
        ).mappings().all()

        out = []
        for r in rows:
            payload = r["payload_json"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            out.append({**dict(r), "payload_json": payload})
        return out


    # crm_agent/db/repo.py (class Repo 내부에 추가)

from datetime import date
from sqlalchemy import text

def _show_columns(self, table: str) -> list[str]:
    rows = self.db.execute(text(f"SHOW COLUMNS FROM {table}")).fetchall()
    # rows: (Field, Type, Null, Key, Default, Extra)
    return [r[0] for r in rows]

def _detect_user_id_col(self) -> str:
    cols = set(self._show_columns("users"))
    for cand in ["user_id", "id", "username"]:
        if cand in cols:
            return cand
    # 최후 fallback
    return "id" if "id" in cols else "user_id"

def _detect_user_features_join(self) -> tuple[str | None, str | None]:
    """
    users 와 user_features 조인 키 자동 탐지
    - (users_key, user_features_key)
    """
    try:
        u_cols = set(self._show_columns("users"))
        f_cols = set(self._show_columns("user_features"))
    except Exception:
        return None, None

    # 가장 흔한 케이스
    if "user_id" in u_cols and "user_id" in f_cols:
        return "user_id", "user_id"
    if "id" in u_cols and "user_id" in f_cols:
        return "id", "user_id"
    if "id" in u_cols and "id" in f_cols:
        return "id", "id"

    # 교집합 후보
    common = list(u_cols.intersection(f_cols))
    for cand in ["user_id", "id", "username"]:
        if cand in common:
            return cand, cand

    return None, None

def list_user_genders(self) -> list[str]:
    # users.gender: 'F' / 'M'
    rows = self.db.execute(
        text("SELECT DISTINCT gender FROM users WHERE gender IS NOT NULL AND gender <> ''")
    ).fetchall()
    out = []
    for r in rows:
        g = r[0]
        if g in ("F", "M"):
            out.append(g)
    return out

def min_max_birth_year(self) -> tuple[int | None, int | None]:
    row = self.db.execute(
        text("SELECT MIN(birth_year) as min_y, MAX(birth_year) as max_y FROM users WHERE birth_year IS NOT NULL")
    ).fetchone()
    if not row:
        return None, None
    return row[0], row[1]

def get_gender_options_label(self) -> list[str]:
    mapping = {"F": "여", "M": "남"}
    gs = self.list_user_genders()
    if not gs:
        return ["여", "남"]
    return [mapping[g] for g in gs if g in mapping]

def get_age_group_options_label(self) -> list[str]:
    """
    birth_year 범위를 보고 '20대/30대/...' 옵션 생성
    """
    min_birth, max_birth = self.min_max_birth_year()
    if not min_birth or not max_birth:
        return ["20대", "30대", "40대", "50대+"]

    this_year = date.today().year
    min_age = this_year - max_birth
    max_age = this_year - min_birth

    # 10대~60대+
    labels = []
    for d in range((min_age // 10) * 10, (max_age // 10) * 10 + 1, 10):
        if d < 10:
            continue
        if d >= 60:
            if "60대+" not in labels:
                labels.append("60대+")
        else:
            lab = f"{d}대"
            if lab not in labels:
                labels.append(lab)

    # 너무 길면 핵심만
    return labels[:7] if labels else ["20대", "30대", "40대", "50대+"]

def _age_group_to_birth_range(self, age_group: str) -> tuple[int | None, int | None]:
    """
    '20대' -> (min_birth, max_birth)
    '60대+' -> (None, max_birth)
    """
    this_year = date.today().year
    if age_group.endswith("대+"):
        base = int(age_group.replace("대+", ""))
        max_birth = this_year - base
        return None, max_birth

    base = int(age_group.replace("대", ""))
    min_age, max_age = base, base + 9
    min_birth = this_year - max_age
    max_birth = this_year - min_age
    return min_birth, max_birth

def preview_target_users(self, target_input: dict, sample_size: int = 5) -> dict:
    """
    ✅ 다음 단계까지: DB 기반 미리보기 (count + sample)
    - 성별: users.gender (F/M)
    - 나이대: users.birth_year (range)
    - 피부타입/피부고민: user_features에 컬럼 있으면 적용, 없으면 무시(경고는 error에 담지 않음)
    """
    try:
        ucols = set(self._show_columns("users"))
    except Exception as e:
        return {"error": f"users 테이블 컬럼 조회 실패: {repr(e)}"}

    user_id_col = self._detect_user_id_col()

    # ---- UI -> DB 변환 ----
    gender_ui = (target_input or {}).get("gender", []) or []
    gender_db = []
    for g in gender_ui:
        if g == "여":
            gender_db.append("F")
        elif g == "남":
            gender_db.append("M")

    age_groups = (target_input or {}).get("age_group", []) or []
    birth_ranges = [self._age_group_to_birth_range(a) for a in age_groups]

    # feature 필터는 가능한 경우에만 적용
    skin_type = (target_input or {}).get("skin_type", []) or []
    skin_concern = (target_input or {}).get("skin_concern", []) or []

    # user_features 조인 탐지
    ukey, fkey = self._detect_user_features_join()
    can_join = (ukey is not None and fkey is not None)

    fcols = set()
    if can_join:
        try:
            fcols = set(self._show_columns("user_features"))
        except Exception:
            can_join = False

    # 피부타입/피부고민 컬럼명 후보 (프로젝트마다 다를 수 있어 자동 대응)
    skin_type_col = None
    for cand in ["skin_type", "skin_type_primary"]:
        if cand in fcols:
            skin_type_col = cand
            break

    concern_col = None
    for cand in ["skin_concern", "skin_concern_primary"]:
        if cand in fcols:
            concern_col = cand
            break

    where = []
    params = {}

    # gender
    if gender_db and "gender" in ucols:
        where.append("u.gender IN :genders")
        params["genders"] = tuple(gender_db)

    # birth_year ranges (OR)
    if birth_ranges and "birth_year" in ucols:
        ors = []
        for i, (mn, mx) in enumerate(birth_ranges):
            if mn is None and mx is not None:
                ors.append(f"(u.birth_year <= :mx{i})")
                params[f"mx{i}"] = mx
            elif mn is not None and mx is not None:
                ors.append(f"(u.birth_year BETWEEN :mn{i} AND :mx{i})")
                params[f"mn{i}"] = mn
                params[f"mx{i}"] = mx
        if ors:
            where.append("(" + " OR ".join(ors) + ")")

    # join + features
    join_sql = ""
    if can_join and ( (skin_type and skin_type_col) or (skin_concern and concern_col) ):
        join_sql = f" JOIN user_features uf ON uf.{fkey} = u.{ukey} "

        if skin_type and skin_type_col:
            where.append(f"uf.{skin_type_col} IN :skin_type")
            params["skin_type"] = tuple(skin_type)

        if skin_concern and concern_col:
            where.append(f"uf.{concern_col} IN :concern")
            params["concern"] = tuple(skin_concern)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    # count
    count_sql = f"SELECT COUNT(*) FROM users u {join_sql} {where_sql}"
    cnt = self.db.execute(text(count_sql), params).scalar() or 0

    # sample fields
    fields = [f"u.{user_id_col} AS user_key"]
    if "gender" in ucols:
        fields.append("u.gender")
    if "birth_year" in ucols:
        fields.append("u.birth_year")

    if join_sql:
        if skin_type_col:
            fields.append(f"uf.{skin_type_col} AS skin_type")
        if concern_col:
            fields.append(f"uf.{concern_col} AS skin_concern")

    sample_sql = f"SELECT {', '.join(fields)} FROM users u {join_sql} {where_sql} LIMIT {int(sample_size)}"
    rows = self.db.execute(text(sample_sql), params).mappings().all()
    sample = [dict(r) for r in rows]

    return {"count": int(cnt), "sample": sample}

