import sys
from pathlib import Path
import json
import streamlit as st

from datetime import datetime, date
from decimal import Decimal

from sqlalchemy import text, bindparam

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from crm_agent.db.engine import SessionLocal
from crm_agent.db.repo import Repo
from crm_agent.flow.workflow import run_until_candidates  # 후보 생성까지만 사용


def j(obj):
    """
    Streamlit JSON 출력용 helper
    - datetime/date/Decimal 등 json.dumps가 못 직렬화하는 타입을 문자열로 변환
    """
    def _default(o):
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        if isinstance(o, Decimal):
            return float(o)
        return str(o)

    st.code(json.dumps(obj, ensure_ascii=False, indent=2, default=_default), language="json")


st.set_page_config(page_title="AMORE Template Agent (B+LangGraph)", layout="wide")


# -------------------------
# Target helpers (DB preview)
# -------------------------
GENDER_LABEL_TO_DB = {"여": "F", "남": "M"}
SKIN_LABEL_TO_DB = {"건성": "dry", "지성": "oily", "복합성": "combination", "중성": "normal"}


def _has_column(db, table: str, column: str) -> bool:
    row = db.execute(text(f"SHOW COLUMNS FROM {table} LIKE :col"), {"col": column}).fetchone()
    return row is not None


def _age_band_to_birthyear_ranges(age_bands: list[str]) -> list[tuple[int, int]]:
    """
    만 나이 근사:
    - 20대(20~29) => birth_year in [cur-29, cur-20]
    """
    cur = datetime.now().year
    ranges = []
    for b in (age_bands or []):
        b = str(b).strip()
        if b == "10대":
            ranges.append((cur - 19, cur - 10))
        elif b == "20대":
            ranges.append((cur - 29, cur - 20))
        elif b == "30대":
            ranges.append((cur - 39, cur - 30))
        elif b == "40대":
            ranges.append((cur - 49, cur - 40))
        elif b in ("50대+", "50대"):
            ranges.append((1900, cur - 50))
    return ranges


def preview_target_users_local(db, target_input: dict, sample_size: int = 5) -> dict:
    gender_vals = target_input.get("gender") or []
    age_bands = target_input.get("age_bands") or []
    skin_vals = target_input.get("skin_types") or []

    has_skin = _has_column(db, "user_features", "skin_type")

    where_clauses = []
    params = {}

    if gender_vals:
        where_clauses.append("u.gender IN :genders")
        params["genders"] = gender_vals  # tuple 말고 list로

    ranges = _age_band_to_birthyear_ranges(age_bands)
    if ranges:
        ors = []
        for i, (y_min, y_max) in enumerate(ranges):
            ors.append(f"(u.birth_year BETWEEN :by_min_{i} AND :by_max_{i})")
            params[f"by_min_{i}"] = y_min
            params[f"by_max_{i}"] = y_max
        where_clauses.append("(" + " OR ".join(ors) + ")")

    if has_skin and skin_vals:
        where_clauses.append("uf.skin_type IN :skin_types")
        params["skin_types"] = skin_vals  # list로

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    # expanding bindparams 적용
    bp = []
    if "genders" in params:
        bp.append(bindparam("genders", expanding=True))
    if "skin_types" in params:
        bp.append(bindparam("skin_types", expanding=True))

    q_count = text(f"""
        SELECT COUNT(*) AS cnt
        FROM users u
        LEFT JOIN user_features uf ON uf.user_id = u.user_id
        {where_sql}
    """).bindparams(*bp)

    cnt = db.execute(q_count, params).scalar() or 0

    if has_skin:
        q_sample = text(f"""
            SELECT u.user_id, u.gender, u.birth_year, uf.skin_type
            FROM users u
            LEFT JOIN user_features uf ON uf.user_id = u.user_id
            {where_sql}
            ORDER BY u.user_id
            LIMIT :limit_n
        """).bindparams(*bp)
    else:
        q_sample = text(f"""
            SELECT u.user_id, u.gender, u.birth_year
            FROM users u
            LEFT JOIN user_features uf ON uf.user_id = u.user_id
            {where_sql}
            ORDER BY u.user_id
            LIMIT :limit_n
        """).bindparams(*bp)

    params2 = dict(params)
    params2["limit_n"] = int(sample_size)
    rows = db.execute(q_sample, params2).mappings().all()

    cur = datetime.now().year
    sample = []
    for r in rows:
        by = r.get("birth_year")
        age = (cur - int(by)) if by else None
        item = {"user_id": r.get("user_id"), "gender": r.get("gender"), "birth_year": by, "age": age}
        if has_skin:
            item["skin_type"] = r.get("skin_type")
        sample.append(item)

    return {"count": int(cnt), "sample": sample, "has_skin_type": has_skin}


def main():
    st.title("AMORE Template Agent")

    st.caption(
        "✅ 목표: 마케터는 캠페인 자연어 입력 + 톤/타겟 선택만 하고, "
        "Template Agent는 '슬롯 템플릿(뼈대)'만 생성합니다. "
        "상품/혜택/쿠폰/링크 채우기는 뒤 Execution Agent가 담당합니다."
    )

    page = st.sidebar.radio(
        "페이지",
        [
            "Step1 브리프(캠페인 입력)",
            "Step2 후보 생성(톤/타겟)",
            "Step3 후보/선택(템플릿 확정)",
            "Step4 승인(템플릿 승인/반려)",
            "Run 타임라인",
        ],
    )

    run_id = st.sidebar.text_input("run_id", value=st.session_state.get("run_id", ""))
    if run_id:
        st.session_state["run_id"] = run_id

    with SessionLocal() as db:
        repo = Repo(db)

        # -------------------------
        # Step1
        # -------------------------
        if page == "Step1 브리프(캠페인 입력)":
            st.subheader("Step1) 캠페인 브리프 입력 → run 생성")

            created_by = st.text_input("created_by(=user_id)", value="marketer_001")

            # goal(캠페인 목표) = 3개 시나리오 토글(라디오)
            scenario_labels = [
                "1) 화장품 특징 기반 추천 CRM(자연어 입력 필요)",
                "2) 장바구니 미구매 상품 CRM",
                "3) 재구매율 높은 상품 CRM",
            ]
            scenario_codes = {
                scenario_labels[0]: "feature_reco",
                scenario_labels[1]: "cart_abandon",
                scenario_labels[2]: "reorder_top",
            }

            goal_label = st.radio(
                "goal(캠페인 목표)",
                scenario_labels,
                index=0,
                horizontal=True,
            )
            campaign_goal_code = scenario_codes[goal_label]

            col1, col2 = st.columns(2)
            with col1:
                channel_hint = st.selectbox("채널 힌트(선택)", ["PUSH", "SMS", "KAKAO", "EMAIL"], index=0)
            with col2:
                # tone_hint를 브랜드 선택으로
                tone_hint = st.selectbox("브랜드 톤(선택)", ["amoremall", "innisfree"], index=0)

            # 1번 시나리오만 campaign_text 입력 허용
            disable_campaign_text = (goal_label != scenario_labels[0])
            campaign_text_value = "" if disable_campaign_text else "겨울철 보습 루틴을 강조하면서, 기존 구매 고객의 재구매를 유도하는 캠페인. 톤은 친근하게."

            campaign_text = st.text_area(
                "campaign_text(자연어 캠페인 설명)",
                value=campaign_text_value,
                height=120,
                disabled=disable_campaign_text,
                placeholder="(1번 시나리오에서만 입력 가능)",
            )

            brief = {
                "goal": goal_label,                  # UI 라벨
                "campaign_goal": campaign_goal_code, # 내부 코드
                "campaign_text": campaign_text,      # 1번에서만 값 존재
                "channel_hint": channel_hint,
                "tone_hint": tone_hint,              # 브랜드 id
            }

            if st.button("run 생성"):
                rid = repo.create_run(created_by, brief, channel=channel_hint)

                # BRIEF handoff 저장
                repo.create_handoff(rid, "BRIEF", brief)

                repo.update_run(rid, step_id="S1_BRIEF")
                st.session_state["run_id"] = rid
                st.success(f"run_id = {rid}")

            if st.session_state.get("run_id"):
                run = repo.get_run(st.session_state["run_id"])
                if run:
                    st.markdown("### 현재 Run")
                    st.write(f"status: {run.get('status')} / step_id: {run.get('step_id')} / channel: {run.get('channel')}")
                    j(run.get("brief_json", {}))

        # -------------------------
        # Step2
        # -------------------------
        elif page == "Step2 후보 생성(톤/타겟)":
            st.subheader("Step2) 톤/타겟 선택 → LangGraph 실행(템플릿 후보 생성까지)")
            st.caption("✅ Step2에서 channel/tone 선택 UI는 제거했습니다. Step1 힌트를 사용합니다.")

            if not run_id:
                st.warning("좌측 run_id 입력 또는 Step1에서 생성하세요.")
                return

            run = repo.get_run(run_id)
            if not run:
                st.error("run_id가 유효하지 않습니다.")
                return

            brief = run.get("brief_json", {}) or {}
            channel = run.get("channel") or brief.get("channel_hint") or "PUSH"

            # tone은 브랜드 id
            tone = (brief.get("tone_hint") or "amoremall").strip().lower()

            st.markdown("### 현재 채널/톤(=Step1 힌트)")
            st.write({"channel": channel, "tone": tone})

            st.markdown("### 타겟 선택(키워드 멀티선택)")

            # UI 옵션(라벨)
            gender_labels = ["여", "남"]
            age_band_labels = ["10대", "20대", "30대", "40대", "50대+"]
            skin_labels = ["건성", "지성", "복합성", "중성"]

            sel_genders_label = st.multiselect("성별", gender_labels, default=gender_labels)
            sel_age_bands = st.multiselect("나이대", age_band_labels, default=["20대"])
            sel_skin_label = st.multiselect("피부타입", skin_labels, default=[])

            # DB 값으로 변환
            sel_genders_db = [GENDER_LABEL_TO_DB[x] for x in sel_genders_label if x in GENDER_LABEL_TO_DB]
            sel_skin_db = [SKIN_LABEL_TO_DB[x] for x in sel_skin_label if x in SKIN_LABEL_TO_DB]

            target_input = {
                "gender": sel_genders_db,     # F/M
                "age_bands": sel_age_bands,   # 밴드 라벨 유지
                "skin_types": sel_skin_db,    # 컬럼 없으면 preview에서 자동 무시
            }

            preview = preview_target_users_local(repo.db, target_input, sample_size=5)

            st.markdown("### 타겟 미리보기")
            st.write(f"대상 수: **{preview['count']}명**")
            if not preview.get("has_skin_type"):
                st.info("user_features.skin_type 컬럼이 없어 피부타입 필터/표시는 현재 무시됩니다. (추가하면 자동 활성화)")

            j(preview)

            if st.button("LangGraph 실행(후보 생성까지)"):
                repo.create_handoff(run_id, "TARGET_INPUT", target_input)
                repo.update_run(run_id, step_id="S2_READY")

                run_until_candidates(run_id, channel=channel, tone=tone)
                st.success("완료: TARGET/RAG/후보/컴플라이언스 생성됨 (템플릿 후보 단계까지)")

            st.markdown("### TARGET")
            h = repo.get_latest_handoff(run_id, "TARGET")
            if h:
                j(h["payload_json"])
            else:
                st.info("TARGET handoff가 아직 없습니다. (Step2 실행 필요)")

            st.markdown("### RAG")
            h = repo.get_latest_handoff(run_id, "RAG")
            if h:
                j(h["payload_json"])
            else:
                st.info("RAG handoff가 아직 없습니다.")

            st.markdown("### TEMPLATE_CANDIDATES (슬롯 템플릿 후보)")
            h = repo.get_latest_handoff(run_id, "TEMPLATE_CANDIDATES")
            if h:
                j(h["payload_json"])
            else:
                st.info("후보가 아직 없습니다. Step2 실행 필요")

            st.markdown("### COMPLIANCE (후보별 PASS/WARN/FAIL)")
            h = repo.get_latest_handoff(run_id, "COMPLIANCE")
            if h:
                j(h["payload_json"])
            else:
                st.info("컴플라이언스 결과가 아직 없습니다.")

        # -------------------------
        # Step3
        # -------------------------
        elif page == "Step3 후보/선택(템플릿 확정)":
            st.subheader("Step3) 후보 확인 → 템플릿 선택(확정)")
            if not run_id:
                st.warning("좌측 run_id 입력 후 진행하세요.")
                return

            # 후보/컴플라이언스 로드
            h_cands = repo.get_latest_handoff(run_id, "TEMPLATE_CANDIDATES")
            h_comp = repo.get_latest_handoff(run_id, "COMPLIANCE")
            h_sel = repo.get_latest_handoff(run_id, "SELECTED_TEMPLATE")

            if not h_cands:
                st.warning("TEMPLATE_CANDIDATES가 없습니다. Step2에서 'LangGraph 실행(후보 생성까지)'를 먼저 실행하세요.")
                return

            cands_payload = h_cands["payload_json"] or {}
            candidates = cands_payload.get("candidates", []) or []

            if not candidates:
                st.warning("후보 리스트가 비어 있습니다. Step2 실행 로그를 확인하세요.")
                j(cands_payload)
                return

            comp_map = {}
            if h_comp:
                comp_payload = h_comp["payload_json"] or {}
                for r in (comp_payload.get("results") or []):
                    comp_map[r.get("template_id")] = r

            selected_id = None
            if h_sel:
                selected_id = (h_sel["payload_json"] or {}).get("template_id")

            labels = []
            for i, c in enumerate(candidates):
                tid = c.get("template_id")
                status = (comp_map.get(tid) or {}).get("status", "N/A")
                title = c.get("title") or ""
                labels.append(f"[{status}] {tid}  {title}")

            # 기본 선택값(기존 선택이 있으면 그거로)
            default_idx = 0
            if selected_id:
                for i, c in enumerate(candidates):
                    if c.get("template_id") == selected_id:
                        default_idx = i
                        break

            idx = st.radio("후보 선택", list(range(len(labels))), format_func=lambda i: labels[i], index=default_idx)

            picked = candidates[idx]
            tid = picked.get("template_id")
            st.markdown("### 선택 후보 미리보기")
            j({
                "template_id": tid,
                "title": picked.get("title"),
                "body_with_slots": picked.get("body_with_slots"),
                "compliance": comp_map.get(tid, {}),
            })

            colA, colB = st.columns(2)
            with colA:
                if st.button("✅ 이 후보로 확정(SELECTED_TEMPLATE 저장)"):
                    repo.create_handoff(run_id, "SELECTED_TEMPLATE", picked)
                    # campaign_runs에 status/step_id/candidate_id가 있는 경우 업데이트(없으면 무시되도록 repo가 설계되어 있진 않지만, 현재 repo 기준 컬럼 있음)
                    try:
                        repo.update_run(run_id, step_id="S3_SELECTED", candidate_id=(tid or "")[:16], status="SELECTED")
                    except Exception:
                        pass
                    st.success("SELECTED_TEMPLATE 저장 완료. Step4에서 승인 진행하세요.")

            with colB:
                if h_sel:
                    st.info("현재 DB에 저장된 SELECTED_TEMPLATE가 있습니다.")
                    j(h_sel["payload_json"])

        # -------------------------
        # Step4
        # -------------------------
        elif page == "Step4 승인(템플릿 승인/반려)":
            st.subheader("Step4) 마케터 승인/반려")
            if not run_id:
                st.warning("좌측 run_id 입력 후 진행하세요.")
                return

            h_sel = repo.get_latest_handoff(run_id, "SELECTED_TEMPLATE")
            if not h_sel:
                st.warning("SELECTED_TEMPLATE가 없습니다. Step3에서 템플릿을 먼저 확정하세요.")
                return

            st.markdown("### 선택된 템플릿(SELECTED_TEMPLATE)")
            j(h_sel["payload_json"])

            marketer_id = st.text_input("marketer_id", value="marketer_001")
            decision = st.radio("결정", ["APPROVED", "REJECTED"], horizontal=True)
            comment = st.text_area("코멘트(선택)", value="", height=100)

            if st.button("결정 저장(APPROVAL handoff)"):
                repo.add_approval(run_id, marketer_id=marketer_id, decision=decision, comment=comment)
                try:
                    repo.update_run(run_id, step_id="S4_APPROVAL", status=decision)
                except Exception:
                    pass
                st.success("승인/반려 저장 완료")

            st.markdown("### 승인 이력")
            approvals = repo.list_approvals(run_id)
            if approvals:
                for a in approvals:
                    st.write(f"- {a.get('created_at')} | {a['payload_json'].get('marketer_id')} | {a['payload_json'].get('decision')}")
                    if a["payload_json"].get("comment"):
                        st.caption(a["payload_json"]["comment"])
            else:
                st.info("승인 이력이 없습니다.")

        # -------------------------
        # Run Timeline
        # -------------------------
        elif page == "Run 타임라인":
            st.subheader("Run 타임라인(handoffs)")
            if not run_id:
                st.warning("좌측 run_id 입력 후 진행하세요.")
                return

            run = repo.get_run(run_id)
            st.markdown("### campaign_runs")
            j(run or {})

            st.markdown("### handoffs")
            rows = repo.list_handoffs(run_id)
            if not rows:
                st.info("handoff가 없습니다.")
                return

            # 가볍게 stage/time만 표로
            import pandas as pd
            df = pd.DataFrame([{
                "created_at": r.get("created_at"),
                "stage": r.get("stage"),
                "payload_version": r.get("payload_version"),
                "handoff_id": r.get("handoff_id"),
            } for r in rows])
            st.dataframe(df, use_container_width=True)

            st.markdown("### 상세 payload(최근 1개)")
            j(rows[-1].get("payload_json"))

if __name__ == "__main__":
    main()
