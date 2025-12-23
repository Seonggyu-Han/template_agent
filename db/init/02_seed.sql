USE crm;

-- ============================================================
-- 02_seed.sql
-- - Creates 50 demo users (u_001 ~ u_050)
-- - Creates 50 demo user_features rows (skin_type included)
-- - Safe to re-run (UPSERT)
-- ============================================================

-- 0) Build 1..50 sequence in a TEMPORARY table
DROP TEMPORARY TABLE IF EXISTS tmp_seq;
CREATE TEMPORARY TABLE tmp_seq (
  n INT NOT NULL PRIMARY KEY
);

INSERT INTO tmp_seq (n)
SELECT (a.d + b.d * 10 + 1) AS n
FROM
  (SELECT 0 AS d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
   UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a
CROSS JOIN
  (SELECT 0 AS d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) b
WHERE (a.d + b.d * 10 + 1) <= 50;

-- 1) users: u_001 ~ u_050
INSERT INTO users
  (user_id, customer_name, gender, birth_year, region,
   preferred_channel, sms_opt_in, kakao_opt_in, push_opt_in, email_opt_in,
   phone_e164, kakao_user_key, push_token, email)
SELECT
  CONCAT('u_', LPAD(n, 3, '0')) AS user_id,
  CONCAT('User', LPAD(n, 3, '0')) AS customer_name,

  -- gender: F/M alternating
  CASE WHEN MOD(n, 2) = 0 THEN 'M' ELSE 'F' END AS gender,

  -- birth_year: 1980 ~ 2004
  (1980 + MOD(n, 25)) AS birth_year,

  -- region: rotate 5 cities
  CASE MOD(n, 5)
    WHEN 0 THEN 'Seoul'
    WHEN 1 THEN 'Busan'
    WHEN 2 THEN 'Incheon'
    WHEN 3 THEN 'Daegu'
    ELSE 'Gwangju'
  END AS region,

  'SMS' AS preferred_channel,

  -- opt-ins: most are 1, 일부는 0
  CASE WHEN MOD(n, 7) = 0 THEN 0 ELSE 1 END AS sms_opt_in,
  0 AS kakao_opt_in,
  0 AS push_opt_in,
  0 AS email_opt_in,

  -- phone: +8210 + 8 digits
  CONCAT('+8210', LPAD(n, 8, '0')) AS phone_e164,

  NULL AS kakao_user_key,
  NULL AS push_token,
  NULL AS email
FROM tmp_seq
ON DUPLICATE KEY UPDATE
  customer_name = VALUES(customer_name),
  gender        = VALUES(gender),
  birth_year    = VALUES(birth_year),
  region        = VALUES(region),
  preferred_channel = VALUES(preferred_channel),
  sms_opt_in    = VALUES(sms_opt_in),
  kakao_opt_in  = VALUES(kakao_opt_in),
  push_opt_in   = VALUES(push_opt_in),
  email_opt_in  = VALUES(email_opt_in),
  phone_e164    = VALUES(phone_e164),
  updated_at    = CURRENT_TIMESTAMP;

-- 2) user_features: 50 rows (skin_type matches Streamlit mapping)
-- Streamlit mapping:
-- 건성=dry, 지성=oily, 복합성=combination, 중성=normal
INSERT INTO user_features
  (user_id, lifecycle_stage, last_browse_at, last_cart_at, last_purchase_at,
   cart_items_count, persona_id, skin_type,
   skin_concern_primary, sensitivity_level, top_category_30d)
SELECT
  CONCAT('u_', LPAD(n, 3, '0')) AS user_id,

  CASE
    WHEN MOD(n, 10) = 0 THEN 'dormant'
    WHEN MOD(n,  3) = 0 THEN 'new'
    ELSE 'active'
  END AS lifecycle_stage,

  (NOW() - INTERVAL MOD(n, 14) DAY) AS last_browse_at,
  CASE WHEN MOD(n, 4) = 0 THEN NULL ELSE (NOW() - INTERVAL MOD(n, 48) HOUR) END AS last_cart_at,
  CASE WHEN MOD(n, 5) = 0 THEN NULL ELSE (NOW() - INTERVAL MOD(n, 60) DAY) END AS last_purchase_at,

  MOD(n, 4) AS cart_items_count,

  CASE WHEN MOD(n, 2) = 0 THEN 'ingredient_care' ELSE 'hydration' END AS persona_id,

  CASE MOD(n, 4)
    WHEN 0 THEN 'dry'
    WHEN 1 THEN 'oily'
    WHEN 2 THEN 'combination'
    ELSE 'normal'
  END AS skin_type,

  CASE MOD(n, 6)
    WHEN 0 THEN 'hydration'
    WHEN 1 THEN 'sensitivity'
    WHEN 2 THEN 'pores'
    WHEN 3 THEN 'redness'
    WHEN 4 THEN 'acne'
    ELSE 'unknown'
  END AS skin_concern_primary,

  CASE MOD(n, 3)
    WHEN 0 THEN 'low'
    WHEN 1 THEN 'mid'
    ELSE 'high'
  END AS sensitivity_level,

  CASE MOD(n, 4)
    WHEN 0 THEN 'skincare'
    WHEN 1 THEN 'makeup'
    WHEN 2 THEN 'hair'
    ELSE 'unknown'
  END AS top_category_30d
FROM tmp_seq
ON DUPLICATE KEY UPDATE
  lifecycle_stage      = VALUES(lifecycle_stage),
  last_browse_at       = VALUES(last_browse_at),
  last_cart_at         = VALUES(last_cart_at),
  last_purchase_at     = VALUES(last_purchase_at),
  cart_items_count     = VALUES(cart_items_count),
  persona_id           = VALUES(persona_id),
  skin_type            = VALUES(skin_type),
  skin_concern_primary = VALUES(skin_concern_primary),
  sensitivity_level    = VALUES(sensitivity_level),
  top_category_30d     = VALUES(top_category_30d),
  updated_at           = CURRENT_TIMESTAMP;

-- 3) Cleanup
DROP TEMPORARY TABLE IF EXISTS tmp_seq;

-- 4) Debug counts (safe)
SELECT COUNT(*) AS users_cnt FROM users;
SELECT COUNT(*) AS features_cnt FROM user_features;
