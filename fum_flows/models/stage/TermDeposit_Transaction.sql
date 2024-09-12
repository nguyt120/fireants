WITH raw AS (
    SELECT
        PARSE_DATETIME('%Y%m%d', CAST(TXN_POST_DATE AS STRING)) AS transaction_datetime,
        CASE TRAN_TYPE WHEN 'C' THEN 'TD_CREDIT' WHEN 'D' THEN 'TD_DEBIT' ELSE NULL END AS transaction_type,
        FORMAT('%023d', CAST(HFR_ACCT_NUMBER AS INT64)) AS account_number, --Identifier used by CAP that the account is domiciled in Australia
        REPLACE(ACCT_BSB, '-', '') AS bsb_number , --Bank and Branch Identifier
        HOGAN_PROD AS product_code, --Identifies the account AS a Demand Deposit Account
        HOGAN_SUB_PROD AS sub_product_code,
        IF(TXN_AMOUNT_SIGN='+',TXN_AMOUNT,-1*TXN_AMOUNT) AS transaction_amount
    FROM {{ source("T1_CAP_AUS_CFDL_VW", "CAPIDSAU_BTRCDA_01_CFDL_VW") }}
    WHERE dl__rec_eff_to_ts = TIMESTAMP('9999-12-31 00:00:00 UTC')
)
SELECT
    transaction_datetime
    ,CAST(NULL AS STRING)	transaction_id
    ,CAST(NULL AS STRING)	transaction_puid
    ,'Term Deposit' transaction_src
    ,transaction_type
    ,CAST(NULL AS STRING)	transaction_status
    ,account_number src_account_number
    ,bsb_number src_bsb_number
    ,product_code src_product_code
    ,sub_product_code src_sub_product_code
    ,CAST(NULL AS STRING)	src_marketing_code
    ,CAST(NULL AS STRING)	src_term
    ,CAST(NULL AS STRING)	dst_account_number
    ,CAST(NULL AS STRING)	dst_bsb_number
    ,CAST(NULL AS STRING)	dst_product_code
    ,CAST(NULL AS STRING)	dst_sub_product_code
    ,CAST(NULL AS STRING)	dst_marketing_code
    ,CAST(NULL AS STRING)	dst_term
    ,transaction_amount
FROM raw
