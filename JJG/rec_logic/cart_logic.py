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
cart_table = "carts"
cart_item_table = "cart_items"
product_table = "products"
ocr_table = "product_ocr_text"

db_url = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
engine = create_engine(db_url)

def process_abandoned_cart_longest_duration():
    print(f"📡 [Case 2] 개인화 메시지 (가장 오래된 장바구니 기준) 생성 시작...")

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

    # --- [Step 2] 유저별 ABANDONED 장바구니 및 시간 정보 조회 ---
    ids_tuple = tuple(user_ids)
    in_clause = f"('{user_ids[0]}')" if len(user_ids) == 1 else str(ids_tuple)

    # carts 테이블의 created_at, updated_at을 조회합니다.
    personal_query = f"""
        SELECT 
            u.user_id,
            u.customer_name,
            p.product_name,
            p.detail_url,
            o.detail_slot,
            c.created_at,   -- 장바구니 생성일 (오래된 기준)
            c.updated_at    -- 장바구니 수정일 (참고용)
        FROM {cart_table} c
        JOIN {user_table} u ON c.user_id = u.user_id
        JOIN {cart_item_table} ci ON c.cart_id = ci.cart_id
        JOIN {product_table} p ON ci.prod_sn = p.prod_sn
        LEFT JOIN {ocr_table} o ON p.prod_sn = o.prod_sn
        WHERE c.status = 'ABANDONED'
          AND c.user_id IN {in_clause}
    """
    
    try:
        df = pd.read_sql(personal_query, engine)
    except Exception as e:
        print(f"❌ 데이터 조회 실패: {e}")
        return

    if df.empty:
        print("⛔ 대상 유저 중 장바구니 이탈 내역이 없습니다.")
        return

    # --- [Step 3] 가장 오래된 장바구니의 상품 1개 선정 ---
    
    # 1. 날짜 형식 변환
    if 'created_at' in df.columns:
        df['created_at'] = pd.to_datetime(df['created_at'])
    if 'updated_at' in df.columns:
        df['updated_at'] = pd.to_datetime(df['updated_at'])

    # 2. 정렬 로직:
    #    '가장 오랫동안 담겨 있는' = '생성일(created_at)이 가장 과거인 것'
    #    Ascending=True로 설정하여 예전 날짜가 위로 오게 합니다.
    df_sorted = df.sort_values(by=['user_id', 'created_at'], ascending=[True, True])

    # 3. 중복 제거: 유저별로 가장 위에 있는(가장 오래된) 행만 남김
    target_df = df_sorted.drop_duplicates(subset=['user_id'], keep='first').copy()

    # NULL 값 처리
    target_df.fillna("", inplace=True)

    print(f"✅ 메시지 발송 대상: {len(target_df)}명 (오래된 장바구니 우선 선정)")
    print("-" * 50)

    # --- [Step 4] 메시지 생성 ---
    final_results = []
    
    print("\n[개인화 메시지 미리보기]")
    for _, row in target_df.iterrows():
        uid = row['user_id']
        name = row['customer_name']
        p_name = row['product_name']
        c_time = row['created_at']
        
        slot_values = {
            "customer_name": name,
            "product_name": p_name,
            "offer": "",
            "cta": row['detail_url'],
            "product_detail": row['detail_slot']
        }

        try:
            completed_message = template_body.format(**slot_values)
            final_results.append({"user_id": uid, "message": completed_message})
            print(f"[{uid}/{name}] (상품:{p_name} / 담은날짜:{c_time})\n └-> {completed_message}")
        except KeyError as e:
            print(f"❌ 슬롯 매칭 에러 ({uid}): {e}")
