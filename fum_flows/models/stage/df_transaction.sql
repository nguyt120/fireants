WITH
<<<<<<< HEAD
DF_Plus AS (
=======
df_plus AS (
>>>>>>> main
    SELECT
        transaction_datetime,
        transaction_id,
        COALESCE(transfer_puid, payment_puid, bpay_puid)                                    AS transaction_puid,
        "plus"                                                                              AS transaction_src,
        transaction_type,
        transaction_status,
        account_number                                                                      AS src_account_number,
        account_bsb                                                                         AS src_bsb_number,
        product_code                                                                        AS src_product_code,
        sub_product_code                                                                    AS src_sub_product_code,
        marketing_code                                                                      AS src_marketing_code,
        CAST(NULL AS STRING)                                                                AS src_term, -- No TD data yet
        COALESCE(transfer_destination_account_number, payment_other_entity_account_number)  AS dst_account_number,
        COALESCE(transfer_destination_account_bsb, payment_other_entity_account_bsb)        AS dst_bsb_number,
        transaction_amount
    FROM {{ source("dragonfish_transaction_v1", "transaction_anz_plus") }}
<<<<<<< HEAD
),

DF_Classic AS (
=======
    WHERE transaction_datetime > DATE_SUB(DATE_TRUNC(CURRENT_TIMESTAMP(), DAY), INTERVAL 180 DAY)
),

df_classic AS (
>>>>>>> main
    SELECT
        transaction_datetime,
        transaction_id,
        COALESCE(transfer_puid, payment_puid, bpay_puid)                                    AS transaction_puid,
        "classic"                                                                           AS transaction_src,
        transaction_type,
        transaction_status,
        account_number                                                                      AS src_account_number,
        account_bsb                                                                         AS src_bsb_number,
        product_code                                                                        AS src_product_code,
        sub_product_code                                                                    AS src_sub_product_code,
        marketing_code                                                                      AS src_marketing_code,
        CAST(NULL AS STRING)                                                                AS src_term, -- No TD data yet
        COALESCE(transfer_destination_account_number, payment_other_entity_account_number)  AS dst_account_number,
        COALESCE(transfer_destination_account_bsb, payment_other_entity_account_bsb)        AS dst_bsb_number,
        transaction_amount
    FROM {{ source("dragonfish_transaction_v1", "transaction_anz_classic") }}
<<<<<<< HEAD
),

DF_Classic_Mapping AS (
=======
    WHERE transaction_datetime > DATE_SUB(DATE_TRUNC(CURRENT_TIMESTAMP(), DAY), INTERVAL 180 DAY)
),

df_classic_mapping AS (
>>>>>>> main
    SELECT   
        src.transaction_datetime,
        src.transaction_id,
        src.transaction_puid,
        src.transaction_src,
        src.transaction_type,
        src.transaction_status,
        src.src_account_number,
        src.src_bsb_number,
        rm_cl.portfolio         AS src_portfolio,
        rm_cl.product_group     AS src_product_group,
        src.src_product_code,
        src.src_sub_product_code,
        src.src_marketing_code,
        src.src_term, 
        src.dst_account_number,
        src.dst_bsb_number,
        src.transaction_amount
<<<<<<< HEAD
    FROM DF_Classic src
    -- Get portfolio+product_group from Reg mapping by cost_centre
    LEFT JOIN {{ ref("bsb_cc_mapping") }} ccm_cl ON acc_type = 'DDA'
                                AND RIGHT(src.src_account_number,9) = ccm_cl.account_number 
                                AND cast(RIGHT(src.src_bsb_number,4) AS NUMERIC) = ccm_cl.bsb 
                                AND src.src_sub_product_code = ccm_cl.sub_product_code
                                AND CAST(src.transaction_datetime AS DATETIME) >= ccm_cl.dl__src_eff_from_dttm 
                                AND CAST(src.transaction_datetime AS DATETIME) <= ccm_cl.dl__src_eff_to_dttm 

    LEFT JOIN {{ ref("reg_data_mapping") }} rm_cl ON src.src_product_code = rm_cl.product_code
                                AND  src.src_sub_product_code = rm_cl.sub_product_code
                                AND cast(ccm_cl.cost_centre AS STRING) = rm_cl.cost_centre 

),
DF_Plus_Mapping AS (
=======
    FROM df_classic AS src
    -- Get cost_centre 
    LEFT JOIN {{ ref("bsb_cc_mapping") }} AS ccm_cl 
        ON acc_type = 'DDA'
        AND RIGHT(src.src_account_number,9) = ccm_cl.account_number 
        AND CAST(RIGHT(src.src_bsb_number,4) AS NUMERIC) = ccm_cl.bsb 
        AND src.src_sub_product_code = ccm_cl.sub_product_code
        AND CAST(src.transaction_datetime AS DATETIME) >= ccm_cl.effective_from_datetime
        AND CAST(src.transaction_datetime AS DATETIME) <= ccm_cl.effective_to_datetime
    -- Get portfolio and product_group
    LEFT JOIN {{ ref("reg_data_mapping") }} AS rm_cl 
        ON src.src_product_code = rm_cl.product_code
        AND src.src_sub_product_code = rm_cl.sub_product_code
        AND CAST(ccm_cl.cost_centre AS STRING) = rm_cl.cost_centre 
),

df_plus_mapping AS (
>>>>>>> main
    SELECT
        src.transaction_datetime,
        src.transaction_id,
        src.transaction_puid,
        src.transaction_src,
        src.transaction_type,
        src.transaction_status,
        src.src_account_number,
        src.src_bsb_number,
        rm_pl.portfolio         AS src_portfolio,
        rm_pl.product_group     AS src_product_group,
        src.src_product_code,
        src.src_sub_product_code,
        src.src_marketing_code,
        src.src_term, 
        src.dst_account_number,
        src.dst_bsb_number,
        src.transaction_amount
<<<<<<< HEAD
    FROM DF_Plus src
 -- Get portfolio+product_group from Reg mapping by  mkt_code
    LEFT JOIN {{ ref("sirius_marketing_code_mapping") }} smc ON smc.account_number = src.src_account_number

    LEFT JOIN {{ ref("reg_data_mapping") }} rm_pl ON concat(src.src_sub_product_code, concat('|',smc.marketing_code)) = rm_pl.sub_product_code
                                AND cast(RIGHT(src.src_bsb_number,4) AS STRING) = rm_pl.cost_centre
)

SELECT * FROM DF_Classic_Mapping
UNION ALL
SELECT * FROM DF_Plus_Mapping
=======
    FROM df_plus AS src
    -- Get missing marketing code
    LEFT JOIN {{ ref("sirius_marketing_code_mapping") }} AS smc 
        ON smc.account_number = src.src_account_number
    -- Get portfolio and product_group
    LEFT JOIN {{ ref("reg_data_mapping") }} AS rm_pl 
        ON CONCAT(src.src_sub_product_code, CONCAT('|',smc.marketing_code)) = rm_pl.sub_product_code
        AND CAST(RIGHT(src.src_bsb_number,4) AS STRING) = rm_pl.cost_centre
)

SELECT * FROM df_classic_mapping
UNION ALL
SELECT * FROM df_plus_mapping
>>>>>>> main
