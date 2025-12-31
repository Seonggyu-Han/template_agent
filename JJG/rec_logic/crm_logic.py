import json
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer

# 1. DB 접속 정보
db_host = "127.0.0.1"
db_port = "3307"
db_user = "root"
db_pass = "goodboyseongmin12!"
db_name = "crm"

# 테이블 정보
user_table = "users"
feature_table = "user_features"
product_table = "products"
map_table = "product_concern_map"
ocr_table = "product_ocr_text"

# DB 연결 엔진 생성
db_url = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
engine = create_engine(db_url)

# 모델 로드 (최초 1회 실행 - 전역 변수로 유지)
print("⏳ AI 모델 로딩 중...")
embedding_model = SentenceTransformer('jhgan/ko-sroberta-multitask')
print("✅ 모델 로딩 완료!")

def process_ai_recommendation(run_id=None):
    print(f"📡 [Case 1] AI 유사도 기반 추천 로직 실행 (Run ID: {run_id})")

    if not run_id:
        print("❌ Run ID가 없습니다.")
        return None

    # --- [Step 1] 타겟 & 템플릿 데이터 조회 ---
    query_target = f"SELECT payload_json FROM handoffs WHERE stage = 'TARGET_AUDIENCE' AND run_id = '{run_id}' LIMIT 1"
    query_template = f"SELECT payload_json FROM handoffs WHERE stage = 'SELECTED_TEMPLATE' AND run_id = '{run_id}' LIMIT 1"

    try:
        df_target = pd.read_sql(query_target, engine)
        df_template = pd.read_sql(query_template, engine)
    except Exception as e:
        print(f"❌ DB 접속 실패: {e}")
        return None

    if df_target.empty or df_template.empty:
        print(f"⚠️ 데이터 없음 (Target or Template missing for run_id: {run_id})")
        return None

    target_data = json.loads(df_target.iloc[0]['payload_json'])
    template_data = json.loads(df_template.iloc[0]['payload_json'])
    
    user_ids = target_data.get('user_ids', [])
    template_body = template_data.get('body_with_slots', "")
    
    if not user_ids: 
        print("⚠️ 타겟 유저 ID가 없습니다.")
        return None

    # --- [Step 2] 캠페인 키워드 추출 ---
    try:
        campaign_keywords_list = template_data['notes']['campaign_text_normalized']['keywords']
        campaign_text = " ".join(campaign_keywords_list)
        print(f"\n🎯 [캠페인 키워드]: {campaign_text}")
    except KeyError:
        campaign_text = "추천 상품"
        print("⚠️ 캠페인 키워드 추출 실패, 기본값 사용")

    # --- [Step 3] 1차 필터링 ---
    ids_tuple = tuple(user_ids)
    in_clause = f"('{user_ids[0]}')" if len(user_ids) == 1 else str(ids_tuple)

    user_query = f"""
        SELECT f.keyword FROM {user_table} u
        LEFT JOIN {feature_table} f ON u.user_id = f.user_id
        WHERE u.user_id IN {in_clause}
    """
    user_df = pd.read_sql(user_query, engine)
    valid_keywords = user_df['keyword'].dropna()
    
    if valid_keywords.empty:
        print("⛔ 유저 키워드 데이터 없음.")
        return None
    
    winning_category = valid_keywords.value_counts().idxmax().split(',')[0].strip()
    print(f"🏆 [1차 필터] 카테고리: '{winning_category}'")

    # --- [Step 4] 상품 및 상세 정보 조회 ---
    product_query = f"""
        SELECT 
            p.prod_sn,
            p.product_name,
            p.detail_url,
            o.keyword as db_product_keywords,
            o.detail_slot
        FROM {product_table} p
        JOIN {map_table} m ON p.prod_sn = m.prod_sn
        LEFT JOIN {ocr_table} o ON p.prod_sn = o.prod_sn
        WHERE m.product_concern = '{winning_category}'
    """
    try:
        candidate_df = pd.read_sql(product_query, engine)
    except Exception as e:
        print(f"❌ 상품 조회 실패: {e}")
        return None

    if candidate_df.empty:
        print(f"⛔ 해당 카테고리({winning_category})의 후보 상품 없음.")
        return None

    candidate_df['db_product_keywords'] = candidate_df['db_product_keywords'].fillna("").astype(str)
    candidate_df['detail_url'] = candidate_df['detail_url'].fillna("")
    candidate_df['detail_slot'] = candidate_df['detail_slot'].fillna("")
    candidate_df['offer'] = "" 

    print(f"✅ 후보 상품 수: {len(candidate_df)}개")

    # --- [Step 5] 임베딩 유사도 분석 ---
    campaign_embedding = embedding_model.encode([campaign_text])
    product_keywords_list = candidate_df['db_product_keywords'].tolist()
    product_embeddings = embedding_model.encode(product_keywords_list)
    similarity_scores = cosine_similarity(campaign_embedding, product_embeddings).flatten()

    best_match_idx = similarity_scores.argmax()
    final_product = candidate_df.iloc[best_match_idx]

    print(f"👉 [최종 선정 상품]: {final_product['product_name']} (유사도: {similarity_scores[best_match_idx]:.4f})")

    # --- [Step 6] 메시지 생성 및 결과 반환 ---
    final_results = []
    
    user_name_df = pd.read_sql(f"SELECT user_id, customer_name FROM {user_table} WHERE user_id IN {in_clause}", engine)
    name_map = user_name_df.set_index('user_id')['customer_name'].to_dict()

    # 💡 [추가됨] 미리보기 타이틀 출력
    print("\n[메시지 발송 미리보기]")

    for uid in user_ids:
        real_name = name_map.get(uid, "고객")
        
        slot_values = {
            "customer_name": real_name,
            "product_name": final_product['product_name'],
            "offer": final_product['offer'],
            "cta": final_product['detail_url'],
            "product_detail": final_product['detail_slot']
        }

        try:
            completed_message = template_body.format(**slot_values)
            
            # 💡 [추가됨] 여기서 메시지 내용을 print로 찍어줍니다!
            print(f"[{uid}/{real_name}] {completed_message}")
            
            final_results.append({
                "run_id": run_id,
                "user_id": uid,
                "customer_name": real_name,
                "phone_number": "010-0000-0000",
                "message": completed_message,
                "product_id": final_product['prod_sn'],
                "status": "READY"
            })
        except KeyError as e:
            print(f"❌ 메시지 생성 중 슬롯 에러: {e}")

    print(f"✅ 총 {len(final_results)}건의 메시지 생성 완료")
    return final_results