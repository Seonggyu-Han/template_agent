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

# 테이블 정보 (모든 로직에서 쓰는 테이블 통합 정의)
user_table = "users"
feature_table = "user_features"
product_table = "products"
map_table = "product_concern_map"
ocr_table = "product_ocr_text"
cart_table = "carts"
cart_item_table = "cart_items"
order_table = "orders"
order_item_table = "order_items"

# DB 연결
db_url = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
engine = create_engine(db_url)

# 모델 로드 (최초 1회 실행)
print("⏳ AI 모델 로딩 중...")
embedding_model = SentenceTransformer('jhgan/ko-sroberta-multitask')
print("✅ 모델 로딩 완료!")

# =========================================================
# [Case 1] counseling: AI 유사도 기반 추천
# =========================================================
def process_ai_recommendation(run_id=None):
    print(f"📡 [Case 1] AI 유사도 기반 추천 로직 실행 (Run ID: {run_id})")
    
    if not run_id: return None

    # 1. 데이터 조회
    query_target = f"SELECT payload_json FROM handoffs WHERE stage = 'TARGET_AUDIENCE' AND run_id = '{run_id}' LIMIT 1"
    query_template = f"SELECT payload_json FROM handoffs WHERE stage = 'SELECTED_TEMPLATE' AND run_id = '{run_id}' LIMIT 1"

    try:
        df_target = pd.read_sql(query_target, engine)
        df_template = pd.read_sql(query_template, engine)
    except Exception as e:
        print(f"❌ DB 접속 실패: {e}")
        return None

    if df_target.empty or df_template.empty: return None

    target_data = json.loads(df_target.iloc[0]['payload_json'])
    template_data = json.loads(df_template.iloc[0]['payload_json'])
    user_ids = target_data.get('user_ids', [])
    template_body = template_data.get('body_with_slots', "")

    if not user_ids: return None

    # 2. 키워드 추출
    try:
        campaign_keywords_list = template_data['notes']['campaign_text_normalized']['keywords']
        campaign_text = " ".join(campaign_keywords_list)
        print(f"🎯 [캠페인 키워드]: {campaign_text}")
    except KeyError:
        campaign_text = "추천 상품"

    # 3. 카테고리 선정
    ids_tuple = tuple(user_ids)
    in_clause = f"('{user_ids[0]}')" if len(user_ids) == 1 else str(ids_tuple)

    user_query = f"SELECT f.keyword FROM {user_table} u LEFT JOIN {feature_table} f ON u.user_id = f.user_id WHERE u.user_id IN {in_clause}"
    user_df = pd.read_sql(user_query, engine)
    valid_keywords = user_df['keyword'].dropna()
    
    if valid_keywords.empty: return None
    winning_category = valid_keywords.value_counts().idxmax().split(',')[0].strip()
    print(f"🏆 [1차 필터] 카테고리: '{winning_category}'")

    # 4. 상품 조회
    product_query = f"""
        SELECT p.prod_sn, p.product_name, p.detail_url, o.keyword as db_product_keywords, o.detail_slot
        FROM {product_table} p
        JOIN {map_table} m ON p.prod_sn = m.prod_sn
        LEFT JOIN {ocr_table} o ON p.prod_sn = o.prod_sn
        WHERE m.product_concern = '{winning_category}'
    """
    candidate_df = pd.read_sql(product_query, engine)
    if candidate_df.empty: return None

    candidate_df['db_product_keywords'] = candidate_df['db_product_keywords'].fillna("").astype(str)
    candidate_df['detail_url'] = candidate_df['detail_url'].fillna("")
    candidate_df['detail_slot'] = candidate_df['detail_slot'].fillna("")
    candidate_df['offer'] = ""

    # 5. AI 매칭
    campaign_embedding = embedding_model.encode([campaign_text])
    product_embeddings = embedding_model.encode(candidate_df['db_product_keywords'].tolist())
    similarity_scores = cosine_similarity(campaign_embedding, product_embeddings).flatten()
    
    best_match_idx = similarity_scores.argmax()
    final_product = candidate_df.iloc[best_match_idx]
    print(f"👉 [선정]: {final_product['product_name']} (유사도: {similarity_scores[best_match_idx]:.4f})")

    # 6. 메시지 생성
    final_results = []
    user_name_df = pd.read_sql(f"SELECT user_id, customer_name FROM {user_table} WHERE user_id IN {in_clause}", engine)
    name_map = user_name_df.set_index('user_id')['customer_name'].to_dict()

    print("\n[AI 메시지 미리보기]")
    for uid in user_ids:
        real_name = name_map.get(uid, "고객")
        slot_values = {
            "customer_name": real_name, "product_name": final_product['product_name'],
            "offer": final_product['offer'], "cta": final_product['detail_url'], "product_detail": final_product['detail_slot']
        }
        try:
            completed_message = template_body.format(**slot_values)
            print(f"[{uid}] {completed_message}")
            final_results.append({
                "run_id": run_id, "user_id": uid, "customer_name": real_name, "phone_number": "010-0000-0000",
                "message": completed_message, "product_id": final_product['prod_sn'], "status": "READY"
            })
        except KeyError: pass

    return final_results


# =========================================================
# [Case 2] cart: 장바구니 이탈 (오래된 순)
# =========================================================
def process_abandoned_cart(run_id=None):
    print(f"📡 [Case 2] 장바구니 이탈 로직 실행 (Run ID: {run_id})")

    if not run_id: return None

    # 1. 데이터 조회
    query_target = f"SELECT payload_json FROM handoffs WHERE stage = 'TARGET_AUDIENCE' AND run_id = '{run_id}' LIMIT 1"
    query_template = f"SELECT payload_json FROM handoffs WHERE stage = 'SELECTED_TEMPLATE' AND run_id = '{run_id}' LIMIT 1"

    try:
        df_target = pd.read_sql(query_target, engine)
        df_template = pd.read_sql(query_template, engine)
    except Exception as e:
        print(f"❌ DB 접속 실패: {e}")
        return None

    if df_target.empty or df_template.empty: return None

    target_data = json.loads(df_target.iloc[0]['payload_json'])
    template_data = json.loads(df_template.iloc[0]['payload_json'])
    user_ids = target_data.get('user_ids', [])
    template_body = template_data.get('body_with_slots', "")
    
    if not user_ids: return None

    # 2. 유저별 장바구니 조회 (가장 오래된 것)
    ids_tuple = tuple(user_ids)
    in_clause = f"('{user_ids[0]}')" if len(user_ids) == 1 else str(ids_tuple)

    personal_query = f"""
        SELECT u.user_id, u.customer_name, p.prod_sn, p.product_name, p.detail_url, o.detail_slot, c.created_at
        FROM {cart_table} c
        JOIN {user_table} u ON c.user_id = u.user_id
        JOIN {cart_item_table} ci ON c.cart_id = ci.cart_id
        JOIN {product_table} p ON ci.prod_sn = p.prod_sn
        LEFT JOIN {ocr_table} o ON p.prod_sn = o.prod_sn
        WHERE c.status = 'ABANDONED' AND c.user_id IN {in_clause}
    """
    
    df = pd.read_sql(personal_query, engine)
    if df.empty:
        print("⛔ 장바구니 이탈 내역 없음")
        return None

    # 3. 정렬 및 중복 제거
    df['created_at'] = pd.to_datetime(df['created_at'])
    df_sorted = df.sort_values(by=['user_id', 'created_at'], ascending=[True, True])
    target_df = df_sorted.drop_duplicates(subset=['user_id'], keep='first').copy()
    target_df.fillna("", inplace=True)
    target_df['offer'] = ""

    print(f"✅ 대상 유저: {len(target_df)}명")

    # 4. 메시지 생성
    final_results = []
    print("\n[장바구니 메시지 미리보기]")
    for _, row in target_df.iterrows():
        uid = row['user_id']
        name = row['customer_name']
        slot_values = {
            "customer_name": name, "product_name": row['product_name'], "offer": row['offer'],
            "cta": row['detail_url'], "product_detail": row['detail_slot']
        }
        try:
            completed_message = template_body.format(**slot_values)
            print(f"[{uid}] {completed_message}")
            final_results.append({
                "run_id": run_id, "user_id": uid, "customer_name": name, "phone_number": "010-0000-0000",
                "message": completed_message, "product_id": row['prod_sn'], "status": "READY"
            })
        except KeyError: pass

    return final_results


# =========================================================
# [Case 3] repurchase: 재구매 유도 (최다 구매 상품) - [NEW]
# =========================================================
def process_repurchase_recommendation(run_id=None):
    print(f"📡 [Case 3] 유저별 최다 구매(재구매) 상품 분석 시작 (Run ID: {run_id})")

    if not run_id: return None

    # 1. 데이터 조회
    query_target = f"SELECT payload_json FROM handoffs WHERE stage = 'TARGET_AUDIENCE' AND run_id = '{run_id}' LIMIT 1"
    query_template = f"SELECT payload_json FROM handoffs WHERE stage = 'SELECTED_TEMPLATE' AND run_id = '{run_id}' LIMIT 1"

    try:
        df_target = pd.read_sql(query_target, engine)
        df_template = pd.read_sql(query_template, engine)
    except Exception as e:
        print(f"❌ DB 접속 실패: {e}")
        return None

    if df_target.empty or df_template.empty: return None

    target_data = json.loads(df_target.iloc[0]['payload_json'])
    template_data = json.loads(df_template.iloc[0]['payload_json'])
    user_ids = target_data.get('user_ids', [])
    template_body = template_data.get('body_with_slots', "")
    
    if not user_ids: return None

    # 2. 유저별 구매 이력 조회 (DELIVERED 상태)
    ids_tuple = tuple(user_ids)
    in_clause = f"('{user_ids[0]}')" if len(user_ids) == 1 else str(ids_tuple)

    history_query = f"""
        SELECT o.user_id, u.customer_name, oi.prod_sn, p.product_name, p.detail_url as cta, ocr.detail_slot as product_detail
        FROM {order_table} o
        JOIN {user_table} u ON o.user_id = u.user_id
        JOIN {order_item_table} oi ON o.order_id = oi.order_id
        JOIN {product_table} p ON oi.prod_sn = p.prod_sn
        LEFT JOIN {ocr_table} ocr ON p.prod_sn = ocr.prod_sn
        WHERE o.order_status = 'DELIVERED' AND o.user_id IN {in_clause}
    """
    
    df_history = pd.read_sql(history_query, engine)
    if df_history.empty:
        print("⛔ 구매 이력 없음")
        return None

    df_history['cta'] = df_history['cta'].fillna("")
    df_history['product_detail'] = df_history['product_detail'].fillna("")
    df_history['offer'] = ""

    # 3. 최다 구매 상품 선정 (Frequency 계산)
    df_count = df_history.groupby(['user_id', 'prod_sn']).size().reset_index(name='purchase_count')
    df_product_info = df_history[['prod_sn', 'product_name', 'cta', 'product_detail', 'offer']].drop_duplicates()
    df_merged = pd.merge(df_count, df_product_info, on='prod_sn', how='left')
    df_user_info = df_history[['user_id', 'customer_name']].drop_duplicates()
    df_merged = pd.merge(df_merged, df_user_info, on='user_id', how='left')

    # 정렬: [유저ID] 오름차순, [구매횟수] 내림차순 -> 유저별 1위 상품 선정
    df_sorted = df_merged.sort_values(by=['user_id', 'purchase_count'], ascending=[True, False])
    final_df = df_sorted.drop_duplicates(subset=['user_id'], keep='first')

    print(f"✅ 대상 유저: {len(final_df)}명 (재구매 추천)")

    # 4. 메시지 생성
    final_results = []
    print("\n[재구매 메시지 미리보기]")
    for _, row in final_df.iterrows():
        uid = row['user_id']
        name = row['customer_name']
        cnt = row['purchase_count']
        
        slot_values = {
            "customer_name": name, "product_name": row['product_name'], "offer": row['offer'],
            "cta": row['cta'], "product_detail": row['product_detail']
        }
        try:
            completed_message = template_body.format(**slot_values)
            print(f"[{uid}] {completed_message}")
            print(f"   👉 (과거 {cnt}회 구매)")
            
            final_results.append({
                "run_id": run_id, "user_id": uid, "customer_name": name, "phone_number": "010-0000-0000",
                "message": completed_message, "product_id": row['prod_sn'], "status": "READY"
            })
        except KeyError: pass

    return final_results