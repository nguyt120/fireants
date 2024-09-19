<<<<<<< HEAD
WITH TD_Tranx AS (
=======
WITH 
td_tranx AS (
>>>>>>> main
    SELECT
        PARSE_TIMESTAMP('%Y%m%d', CAST(TXN_POST_DATE AS STRING))    AS transaction_datetime,
        CASE TRAN_TYPE  WHEN 'C' THEN 'TD_CREDIT' 
                        WHEN 'D' THEN 'TD_DEBIT' ELSE NULL END      AS transaction_type,
        FORMAT('%023d', CAST(HFR_ACCT_NUMBER AS INT64))             AS account_number,  --Identifier used by CAP that the account is domiciled in Australia
        REPLACE(ACCT_BSB, '-', '')                                  AS bsb_number,      --Bank and Branch Identifier
        HOGAN_PROD                                                  AS product_code,    --Identifies the account AS a Demand Deposit Account
        HOGAN_SUB_PROD                                              AS sub_product_code,
        IF(TXN_AMOUNT_SIGN='+',TXN_AMOUNT,-1*TXN_AMOUNT)            AS transaction_amount
    FROM {{ source("T1_CAP_AUS_CFDL_VW", "CAPIDSAU_BTRCDA_01_CFDL_VW") }}
    WHERE dl__rec_eff_to_ts = TIMESTAMP('9999-12-31 00:00:00 UTC')
),
<<<<<<< HEAD
TD_Tranx_Mapping AS (
=======
td_tranx_mapping AS (
>>>>>>> main
    SELECT
        src.transaction_datetime
        , CAST(NULL AS STRING)   AS	transaction_id
        , CAST(NULL AS STRING)   AS transaction_puid
        , 'Term Deposit'         AS transaction_src
        , src.transaction_type
        , CAST(NULL AS STRING)	 AS transaction_status
        , src.account_number     AS src_account_number
        , src.bsb_number         AS src_bsb_number
        , rm_td.portfolio        AS src_portfolio
        , rm_td.product_group    AS src_product_group
        , src.product_code       AS src_product_code
        , src.sub_product_code   AS src_sub_product_code
        , CAST(NULL AS STRING)	 AS src_marketing_code
        , CAST(NULL AS STRING)	 AS src_term
        , CAST(NULL AS STRING)	 AS dst_account_number
        , CAST(NULL AS STRING)	 AS dst_bsb_number
        , CAST(NULL AS STRING)	 AS dst_portfolio
        , CAST(NULL AS STRING)	 AS dst_product_group
        , CAST(NULL AS STRING)	 AS dst_product_code
        , CAST(NULL AS STRING)	 AS dst_sub_product_code
        , CAST(NULL AS STRING)	 AS dst_marketing_code
        , CAST(NULL AS STRING)	 AS dst_term
        , src.transaction_amount
<<<<<<< HEAD
    FROM TD_Tranx src
    LEFT JOIN {{ ref("bsb_cc_mapping") }} ccm_td ON acc_type = 'CDA'
                                            AND SUBSTR(src.account_number, -9) = ccm_td.account_number 
                                            AND cast(src.bsb_number AS NUMERIC) = ccm_td.bsb
                                            AND src.product_code = ccm_td.sub_product_code
                                            AND CAST(src.transaction_datetime AS DATETIME) >= ccm_td.dl__src_eff_from_dttm
                                            AND CAST(src.transaction_datetime AS DATETIME) <= ccm_td.dl__src_eff_to_dttm 
    LEFT JOIN {{ ref("reg_data_mapping") }} rm_td ON  src.product_code = rm_td.product_code
                                            AND  src.sub_product_code = rm_td.sub_product_code
                                            AND cast(ccm_td.cost_centre as string) = rm_td.cost_centre
)
SELECT * FROM TD_Tranx_Mapping
=======
    FROM td_tranx AS src
    -- Get cost_centre
    LEFT JOIN {{ ref("bsb_cc_mapping") }} AS ccm_td 
        ON acc_type = 'CDA'
        AND SUBSTR(src.account_number, -9) = ccm_td.account_number 
        AND CAST(src.bsb_number AS NUMERIC) = ccm_td.bsb
        AND src.sub_product_code = ccm_td.sub_product_code
        AND CAST(src.transaction_datetime AS DATETIME) >= ccm_td.effective_from_datetime
        AND CAST(src.transaction_datetime AS DATETIME) <= ccm_td.effective_to_datetime
    -- Get portfolio and product_group
    LEFT JOIN {{ ref("reg_data_mapping") }} AS rm_td 
        ON  src.product_code = rm_td.product_code
        AND src.sub_product_code = rm_td.sub_product_code
        AND CAST(ccm_td.cost_centre as string) = rm_td.cost_centre
)

SELECT * FROM td_tranx_mapping
>>>>>>> main
