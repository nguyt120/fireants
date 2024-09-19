WITH
comsumer_card_transaction AS (
  -- ATGT tables
  SELECT *
  FROM {{ source("T1_VPS_AUS_CFDL_VW","VPS_AU_ATGT_S1_CFDL_VW") }}
  UNION ALL
  SELECT *
  FROM {{ source("T1_VPS_AUS_CFDL_VW","VPS_AU_ATGT_S2_CFDL_VW") }}
  UNION ALL
  SELECT *
  FROM {{ source("T1_VPS_AUS_CFDL_VW","VPS_AU_ATGT_S3_CFDL_VW") }}
  UNION ALL
  SELECT *
  FROM {{ source("T1_VPS_AUS_CFDL_VW","VPS_AU_ATGT_S4_CFDL_VW") }}
  UNION ALL
  SELECT *
  FROM {{ source("T1_VPS_AUS_CFDL_VW","VPS_AU_ATGT_S5_CFDL_VW") }}
  UNION ALL
  SELECT *
  FROM {{ source("T1_VPS_AUS_CFDL_VW","VPS_AU_ATGT_S6_CFDL_VW") }}
  UNION ALL
  -- ATPT tables
  SELECT *
  FROM {{ source("T1_VPS_AUS_CFDL_VW","VPS_AU_ATPT_S1_CFDL_VW") }}
  UNION ALL
  SELECT *
  FROM {{ source("T1_VPS_AUS_CFDL_VW","VPS_AU_ATPT_S2_CFDL_VW") }}
  UNION ALL
  SELECT *
  FROM {{ source("T1_VPS_AUS_CFDL_VW","VPS_AU_ATPT_S3_CFDL_VW") }}
  UNION ALL
  SELECT *
  FROM {{ source("T1_VPS_AUS_CFDL_VW","VPS_AU_ATPT_S4_CFDL_VW") }}
  UNION ALL
  SELECT *
  FROM {{ source("T1_VPS_AUS_CFDL_VW","VPS_AU_ATPT_S5_CFDL_VW") }}
  UNION ALL
  SELECT *
  FROM {{ source("T1_VPS_AUS_CFDL_VW","VPS_AU_ATPT_S6_CFDL_VW") }}
),
cc_tranx_mapping AS (
  SELECT
    src.dl__rec_eff_from_ts       AS transaction_datetime,
    CAST(NULL AS STRING)          AS transaction_id,
    CAST(NULL AS STRING)          AS transaction_puid,
    'Consumer_Cards'              AS transaction_src,
    src.atgt_mt_type              AS transaction_type,
    CAST(NULL AS STRING)          AS transaction_status,
    src.atgt_acct                 AS src_account_number,
    CAST(NULL AS STRING)          AS src_bsb_number,
    rm.portfolio                  AS src_portfolio,
    rm.product_group              AS src_product_group,
    CAST(src.atgt_org AS STRING)  AS src_product_code,
    CAST(src.atgt_logo AS STRING) AS src_sub_product_code,
    CAST(NULL AS STRING)          AS src_marketing_code,
    CAST(NULL AS STRING)          AS src_term,
    CAST(NULL AS STRING)          AS dst_account_number,
    CAST(NULL AS STRING)          AS dst_bsb_number,
    CAST(NULL AS STRING)          AS dst_portfolio,
    CAST(NULL AS STRING)          AS dst_product_group,
    CAST(NULL AS STRING)          AS dst_product_code,
    CAST(NULL AS STRING)          AS dst_sub_product_code,
    CAST(NULL AS STRING)          AS dst_marketing_code,
    CAST(NULL AS STRING)          AS dst_term,
    CASE src.atgt_mt_type
      WHEN 'D' THEN atgt_mt_amount / 100
      WHEN 'C' THEN -1 * atgt_mt_amount / 100
    END                           AS transaction_amount, --from cents unit
  FROM comsumer_card_transaction AS src
  LEFT JOIN {{ ref("reg_data_mapping") }} AS rm
    ON rm.product_group = "Consumer_Cards"
    AND src.atgt_org = CAST(rm.product_code AS NUMERIC)
    AND CAST(src.atgt_logo AS STRING) = rm.sub_product_code
  WHERE atgt_curr_input_source <> '9'
    AND atgt_mt_posting_flag = 0
    AND atgt_mt_type IN ('D', 'C')
)
SELECT * FROM cc_tranx_mapping
