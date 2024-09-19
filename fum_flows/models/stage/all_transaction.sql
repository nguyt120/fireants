{{ config(materialized="view") }}

WITH
-- Prepare transaction data
raw_df_transaction AS (
    SELECT * 
    FROM {{ ref("df_transaction") }}
    WHERE transaction_datetime < DATE_TRUNC(CURRENT_TIMESTAMP(), DAY)
),

valid_two_legs_puids AS (
    SELECT DISTINCT transaction_puid, COUNT(transaction_puid) leg_count
    FROM raw_df_transaction
    WHERE dst_account_number IS NOT NULL
    GROUP BY transaction_puid
    HAVING leg_count = 2
),

mapping_transaction AS (
    SELECT
        org.transaction_datetime
        ,org.transaction_id
        ,org.transaction_puid
        ,org.transaction_src
        ,org.transaction_type
        ,org.transaction_status
        ,org.src_account_number
        ,org.src_bsb_number
        ,org.src_portfolio
        ,org.src_product_group
        ,org.src_product_code
        ,org.src_sub_product_code
        ,org.src_marketing_code
        ,org.src_term
        ,map.src_account_number     AS dst_account_number
        ,map.src_bsb_number         AS dst_bsb_number
        ,map.src_portfolio          AS dst_portfolio
        ,map.src_product_group      AS dst_product_group
        ,map.src_product_code	    AS dst_product_code
        ,map.src_sub_product_code   AS dst_sub_product_code
        ,map.src_marketing_code     AS dst_marketing_code
        ,map.src_term               AS dst_term
        ,org.transaction_amount
    FROM raw_df_transaction org
    INNER JOIN raw_df_transaction map USING (transaction_puid)
    WHERE org.transaction_puid IN (SELECT transaction_puid FROM valid_two_legs_puids)
    AND org.src_account_number != map.src_account_number
),

no_mapping_transaction AS (
    SELECT
        transaction_datetime
        ,transaction_id
        ,transaction_puid
        ,transaction_src
        ,transaction_type
        ,transaction_status
        ,src_account_number
        ,src_bsb_number
        ,src_portfolio
        ,src_product_group
        ,src_product_code
        ,src_sub_product_code
        ,src_marketing_code
        ,src_term
        ,dst_account_number
        ,dst_bsb_number
        ,CAST(NULL AS STRING)	    AS dst_portfolio
        ,CAST(NULL AS STRING)	    AS dst_product_group
        ,CAST(NULL AS STRING)	    AS dst_product_code
        ,CAST(NULL AS STRING)	    AS dst_sub_product_code
        ,CAST(NULL AS STRING)	    AS dst_marketing_code
        ,CAST(NULL AS STRING)	    AS dst_term
        ,transaction_amount
    FROM raw_df_transaction
    -- Since transaction_id can be duplicated between classic and plus
    WHERE CONCAT(transaction_id, transaction_src) NOT IN (SELECT CONCAT(transaction_id, transaction_src) FROM mapping_transaction)
)

SELECT * FROM mapping_transaction
UNION ALL
SELECT * FROM no_mapping_transaction
-- add term deposit transactions
UNION ALL
SELECT * FROM {{ ref("td_transaction") }}
