import json
import pandas as pd
from sqlalchemy import create_engine, text

# 1. DB 접속 정보
db_host = "127.0.0.1"
db_port = "3307"
db_user = "root"
db_pass = "goodboyseongmin12!"
db_name = "crm"

# 테이블 정보
user_table = "users"
order_table = "orders"
order_item_table = "order_items"
product_table = "products"
ocr_table = "product_ocr_text"

db_url = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
engine = create_engine(db_url)

def process_personal_repurchase_message():
    print(f"📡 [Case 3] 유저별 최다 구매(재구매) 상품 분석 시작...")

    # --- [Step 1] 최신 타겟 & 템플릿 데이터 조회 ---
    query_target = "SELECT payload_json FROM handoffs WHERE stage = 'TARGET_AUDIENCE' ORDER BY created_at DESC LIMIT 1"
    query_template = "SELECT payload_json FROM handoffs WHERE stage = 'SELECTED_TEMPLATE' ORDER BY created_at DESC LIMIT 1"

    try:
        df_target = pd.read_sql(query_target, engine)
        df_template = pd.read_sql(query_template, engine)
    except Exception as e:
        print(f"❌ DB 접속 실패: {e}")
        return

    if df_target.empty or df_template.empty:
        print("⚠️ 처리할 데이터가 없습니다.")
        return

    target_data = json.loads(df_target.iloc[0]['payload_json'])
    template_data = json.loads(df_template.iloc[0]['payload_json'])
    
    user_ids = target_data.get('user_ids', [])
    template_body = template_data.get('body_with_slots', "")
    
    if not user_ids:
        print("⚠️ 타겟 유저 ID가 없습니다.")
        return

    # --- [Step 2] 유저별 구매 이력 전체 조회 ---
    ids_tuple = tuple(user_ids)
    in_clause = f"('{user_ids[0]}')" if len(user_ids) == 1 else str(ids_tuple)

    # 1. 타겟 유저들의 '배송완료(DELIVERED)'된 모든 주문 상품을 가져옵니다.
    #    여기서는 아직 카운트를 세지 않고 Raw Data를 가져옵니다.
    history_query = f"""
        SELECT 
            o.user_id,
            u.customer_name,
            oi.prod_sn,
            p.product_name,
            p.detail_url as cta,
            ocr.detail_slot as product_detail
        FROM {order_table} o
        JOIN {user_table} u ON o.user_id = u.user_id
        JOIN {order_item_table} oi ON o.order_id = oi.order_id
        JOIN {product_table} p ON oi.prod_sn = p.prod_sn
        LEFT JOIN {ocr_table} ocr ON p.prod_sn = ocr.prod_sn
        WHERE o.order_status = 'DELIVERED'
          AND o.user_id IN {in_clause}
    """
    
    try:
        df_history = pd.read_sql(history_query, engine)
    except Exception as e:
        print(f"❌ 구매 이력 조회 실패: {e}")
        return

    if df_history.empty:
        print("⛔ 타겟 유저들의 구매 이력이 없습니다.")
        return

    # 전처리 (Null 방지)
    df_history['cta'] = df_history['cta'].fillna("")
    df_history['product_detail'] = df_history['product_detail'].fillna("")
    df_history['offer'] = ""  # 요청하신 대로 빈 값

    print(f"✅ 총 {len(df_history)}건의 구매 이력 확보")

    # =================================================================
    # 💡 [Step 3] 유저별 최다 구매 상품 선정 (핵심 로직)
    # =================================================================
    # 1. 유저별 + 상품별로 몇 번 샀는지 카운트 (Frequency 계산)
    #    groupby size()를 하면 (user_id, prod_sn) 별 구매 횟수가 나옵니다.
    df_count = df_history.groupby(['user_id', 'prod_sn']).size().reset_index(name='purchase_count')
    
    # 2. 원본 정보와 합칩니다 (상품명, URL 등 정보를 다시 붙임)
    #    중복을 피하기 위해 원본에서 상품 정보만 따로 떼어내서 병합합니다.
    df_product_info = df_history[['prod_sn', 'product_name', 'cta', 'product_detail', 'offer']].drop_duplicates()
    df_merged = pd.merge(df_count, df_product_info, on='prod_sn', how='left')
    
    # 3. 유저 이름 정보도 다시 붙입니다.
    df_user_info = df_history[['user_id', 'customer_name']].drop_duplicates()
    df_merged = pd.merge(df_merged, df_user_info, on='user_id', how='left')

    # 4. 💡 정렬: [유저ID] 기준 오름차순, [구매횟수] 기준 내림차순
    #    이렇게 하면 유저별로 가장 많이 산 상품이 맨 위로 올라옵니다.
    df_sorted = df_merged.sort_values(by=['user_id', 'purchase_count'], ascending=[True, False])

    # 5. 💡 유저별로 1개만 남기기 (가장 많이 산 1위 상품만 남음)
    final_df = df_sorted.drop_duplicates(subset=['user_id'], keep='first')

    print(f"✅ 유저별 맞춤 상품 선정 완료: 총 {len(final_df)}명 대상")
    
    # --- [Step 4] 메시지 생성 ---
    final_results = []
    print("\n[메시지 발송 미리보기]")

    for _, row in final_df.iterrows():
        uid = row['user_id']
        name = row['customer_name']
        p_name = row['product_name']
        cnt = row['purchase_count']
        
        slot_values = {
            "customer_name": name,
            "product_name": p_name,
            "offer": row['offer'],
            "cta": row['cta'],
            "product_detail": row['product_detail']
        }

        try:
            completed_message = template_body.format(**slot_values)
            final_results.append({"user_id": uid, "message": completed_message})
            print(f"[{uid}/{name}] {completed_message}")
            print(f"   👉 (과거 {cnt}회 구매한 최애템: {p_name})")
        except KeyError as e:
            print(f"❌ 슬롯 에러 ({uid}): {e}")
