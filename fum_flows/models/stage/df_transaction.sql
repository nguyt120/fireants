WITH
df_plus AS (
    SELECT
        transaction_datetime,
        transaction_id,
        COALESCE(transfer_puid, payment_puid, bpay_puid)                                    AS transaction_puid,
        "plus"                                                                              AS transaction_src,
        transaction_type,
        transaction_status,
        src.account_number                                                                  AS src_account_number,
        account_bsb                                                                         AS src_bsb_number,
        rm_pl.portfolio                                                                     AS src_portfolio,
        rm_pl.product_group                                                                 AS src_product_group,
        src.product_code                                                                    AS src_product_code,
        src.sub_product_code                                                                AS src_sub_product_code,
        src.marketing_code                                                                  AS src_marketing_code,
        CAST(NULL AS STRING)                                                                AS src_term, -- No TD data yet
        COALESCE(transfer_destination_account_number, payment_other_entity_account_number)  AS dst_account_number,
        COALESCE(transfer_destination_account_bsb, payment_other_entity_account_bsb)        AS dst_bsb_number,
        transaction_amount
    FROM {{ source("dragonfish_transaction_v1", "transaction_anz_plus") }} AS src
    -- Get missing marketing code
    LEFT JOIN {{ ref("sirius_marketing_code_mapping") }} AS smc 
        ON smc.account_number = src.account_number
    -- Get portfolio and product_group
    LEFT JOIN {{ ref("reg_data_mapping") }} AS rm_pl 
        ON CONCAT(src.sub_product_code, CONCAT('|',smc.marketing_code)) = rm_pl.sub_product_code
        AND CAST(RIGHT(src.account_bsb, 4) AS STRING) = rm_pl.cost_centre
    WHERE transaction_datetime < DATE_TRUNC(CURRENT_TIMESTAMP(), DAY)
    -- AND transaction_datetime >= DATE_SUB(DATE_TRUNC(CURRENT_TIMESTAMP(), DAY), INTERVAL 365 DAY)
),

df_classic AS (
    SELECT
        transaction_datetime,
        transaction_id,
        COALESCE(transfer_puid, payment_puid, bpay_puid)                                    AS transaction_puid,
        "classic"                                                                           AS transaction_src,
        transaction_type,
        transaction_status,
        src.account_number                                                                  AS src_account_number,
        account_bsb                                                                         AS src_bsb_number,
        rm_cl.portfolio                                                                     AS src_portfolio,
        rm_cl.product_group                                                                 AS src_product_group,
        src.product_code                                                                    AS src_product_code,
        src.sub_product_code                                                                AS src_sub_product_code,
        src.marketing_code                                                                  AS src_marketing_code,
        CAST(NULL AS STRING)                                                                AS src_term, -- No TD data yet
        COALESCE(transfer_destination_account_number, payment_other_entity_account_number)  AS dst_account_number,
        COALESCE(transfer_destination_account_bsb, payment_other_entity_account_bsb)        AS dst_bsb_number,
        transaction_amount
    FROM {{ source("dragonfish_transaction_v1", "transaction_anz_classic") }} AS src
    -- Get cost_centre 
    LEFT JOIN {{ ref("bsb_cc_mapping") }} AS ccm_cl 
        ON acc_type = 'DDA'
        AND RIGHT(src.account_number,9) = ccm_cl.account_number 
        AND CAST(RIGHT(src.account_bsb,4) AS NUMERIC) = ccm_cl.bsb 
        AND src.sub_product_code = ccm_cl.sub_product_code
        AND CAST(src.transaction_datetime AS DATETIME) >= ccm_cl.effective_from_datetime
        AND CAST(src.transaction_datetime AS DATETIME) <= ccm_cl.effective_to_datetime
    -- Get portfolio and product_group
    LEFT JOIN {{ ref("reg_data_mapping") }} AS rm_cl 
        ON src.product_code = rm_cl.product_code
        AND src.sub_product_code = rm_cl.sub_product_code
        AND CAST(ccm_cl.cost_centre AS STRING) = rm_cl.cost_centre 
    WHERE transaction_datetime < DATE_TRUNC(CURRENT_TIMESTAMP(), DAY) 
    -- AND transaction_datetime >= DATE_SUB(DATE_TRUNC(CURRENT_TIMESTAMP(), DAY), INTERVAL 365 DAY)
),

-- df_plus AS (
--     SELECT
--         transaction_datetime,
--         transaction_id,
--         COALESCE(transfer_puid, payment_puid, bpay_puid)                                    AS transaction_puid,
--         "plus"                                                                              AS transaction_src,
--         transaction_type,
--         transaction_status,
--         account_number                                                                      AS src_account_number,
--         account_bsb                                                                         AS src_bsb_number,
--         product_code                                                                        AS src_product_code,
--         sub_product_code                                                                    AS src_sub_product_code,
--         marketing_code                                                                      AS src_marketing_code,
--         CAST(NULL AS STRING)                                                                AS src_term, -- No TD data yet
--         COALESCE(transfer_destination_account_number, payment_other_entity_account_number)  AS dst_account_number,
--         COALESCE(transfer_destination_account_bsb, payment_other_entity_account_bsb)        AS dst_bsb_number,
--         transaction_amount
--     FROM {{ source("dragonfish_transaction_v1", "transaction_anz_plus") }}
--     -- WHERE transaction_datetime >= DATE_SUB(DATE_TRUNC(CURRENT_TIMESTAMP(), DAY), INTERVAL 365 DAY)
--     AND transaction_datetime < DATE_TRUNC(CURRENT_TIMESTAMP(), DAY)
-- ),



-- df_classic AS (
--     SELECT
--         transaction_datetime,
--         transaction_id,
--         COALESCE(transfer_puid, payment_puid, bpay_puid)                                    AS transaction_puid,
--         "classic"                                                                           AS transaction_src,
--         transaction_type,
--         transaction_status,
--         account_number                                                                      AS src_account_number,
--         account_bsb                                                                         AS src_bsb_number,
--         product_code                                                                        AS src_product_code,
--         sub_product_code                                                                    AS src_sub_product_code,
--         marketing_code                                                                      AS src_marketing_code,
--         CAST(NULL AS STRING)                                                                AS src_term, -- No TD data yet
--         COALESCE(transfer_destination_account_number, payment_other_entity_account_number)  AS dst_account_number,
--         COALESCE(transfer_destination_account_bsb, payment_other_entity_account_bsb)        AS dst_bsb_number,
--         transaction_amount
--     FROM {{ source("dragonfish_transaction_v1", "transaction_anz_classic") }}
--     -- WHERE transaction_datetime >= DATE_SUB(DATE_TRUNC(CURRENT_TIMESTAMP(), DAY), INTERVAL 365 DAY)
--     AND transaction_datetime < DATE_TRUNC(CURRENT_TIMESTAMP(), DAY)
-- ),

-- df_classic_mapping AS (
--     SELECT   
--         src.transaction_datetime,
--         src.transaction_id,
--         src.transaction_puid,
--         src.transaction_src,
--         src.transaction_type,
--         src.transaction_status,
--         src.src_account_number,
--         src.src_bsb_number,
--         rm_cl.portfolio         AS src_portfolio,
--         rm_cl.product_group     AS src_product_group,
--         src.src_product_code,
--         src.src_sub_product_code,
--         src.src_marketing_code,
--         src.src_term, 
--         src.dst_account_number,
--         src.dst_bsb_number,
--         src.transaction_amount
--     FROM df_classic AS src
--     -- Get cost_centre 
--     LEFT JOIN {{ ref("bsb_cc_mapping") }} AS ccm_cl 
--         ON acc_type = 'DDA'
--         AND RIGHT(src.src_account_number,9) = ccm_cl.account_number 
--         AND CAST(RIGHT(src.src_bsb_number,4) AS NUMERIC) = ccm_cl.bsb 
--         AND src.src_sub_product_code = ccm_cl.sub_product_code
--         AND CAST(src.transaction_datetime AS DATETIME) >= ccm_cl.effective_from_datetime
--         AND CAST(src.transaction_datetime AS DATETIME) <= ccm_cl.effective_to_datetime
--     -- Get portfolio and product_group
--     LEFT JOIN {{ ref("reg_data_mapping") }} AS rm_cl 
--         ON src.src_product_code = rm_cl.product_code
--         AND src.src_sub_product_code = rm_cl.sub_product_code
--         AND CAST(ccm_cl.cost_centre AS STRING) = rm_cl.cost_centre 
-- ),

-- df_plus_mapping AS (
--     SELECT
--         src.transaction_datetime,
--         src.transaction_id,
--         src.transaction_puid,
--         src.transaction_src,
--         src.transaction_type,
--         src.transaction_status,
--         src.src_account_number,
--         src.src_bsb_number,
--         rm_pl.portfolio         AS src_portfolio,
--         rm_pl.product_group     AS src_product_group,
--         src.src_product_code,
--         src.src_sub_product_code,
--         src.src_marketing_code,
--         src.src_term, 
--         src.dst_account_number,
--         src.dst_bsb_number,
--         src.transaction_amount
--     FROM df_plus AS src
--     -- Get missing marketing code
--     LEFT JOIN {{ ref("sirius_marketing_code_mapping") }} AS smc 
--         ON smc.account_number = src.src_account_number
--     -- Get portfolio and product_group
--     LEFT JOIN {{ ref("reg_data_mapping") }} AS rm_pl 
--         ON CONCAT(src.src_sub_product_code, CONCAT('|',smc.marketing_code)) = rm_pl.sub_product_code
--         AND CAST(RIGHT(src.src_bsb_number,4) AS STRING) = rm_pl.cost_centre
-- ),

-- Process 2-legs transactions to get the other leg's information
df_all AS (
    SELECT * FROM df_classic
    UNION ALL
    SELECT * FROM df_plus
),

valid_two_legs_puids AS (
    SELECT DISTINCT transaction_puid, COUNT(transaction_puid) leg_count
    FROM df_all
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
    FROM df_all org
    INNER JOIN df_all map USING (transaction_puid)
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
    FROM df_all
    -- Since transaction_id can be duplicated between classic and plus
    WHERE CONCAT(transaction_id, transaction_src) NOT IN (SELECT CONCAT(transaction_id, transaction_src) FROM mapping_transaction)
)

SELECT * FROM mapping_transaction
UNION ALL
SELECT * FROM no_mapping_transaction