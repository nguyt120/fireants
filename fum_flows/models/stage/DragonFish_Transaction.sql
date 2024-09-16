WITH
DF_Classic AS (
    SELECT
        transaction_datetime,
        transaction_id,
        COALESCE(transfer_puid, payment_puid, bpay_puid) transaction_puid,
        "plus" transaction_src,
        transaction_type,
        transaction_status,
        account_number src_account_number,
        account_bsb src_bsb_number,
        product_code src_product_code,
        sub_product_code src_sub_product_code,
        marketing_code src_marketing_code,
        CAST(NULL AS STRING) src_term, -- No TD data yet
        COALESCE(transfer_destination_account_number, payment_other_entity_account_number) dst_account_number,
        COALESCE(transfer_destination_account_bsb, payment_other_entity_account_bsb) dst_bsb_number,
        transaction_amount
    FROM {{ source("dragonfish_transaction_v1", "transaction_anz_plus") }}
),

DF_Plus AS (
    SELECT
        transaction_datetime,
        transaction_id,
        COALESCE(transfer_puid, payment_puid, bpay_puid) transaction_puid,
        "classic" transaction_src,
        transaction_type,
        transaction_status,
        account_number src_account_number,
        account_bsb src_bsb_number,
        product_code src_product_code,
        sub_product_code src_sub_product_code,
        marketing_code src_marketing_code,
        CAST(NULL AS STRING) src_term, -- No TD data yet
        COALESCE(transfer_destination_account_number, payment_other_entity_account_number) dst_account_number,
        COALESCE(transfer_destination_account_bsb, payment_other_entity_account_bsb) dst_bsb_number,
        transaction_amount
    FROM {{ source("dragonfish_transaction_v1", "transaction_anz_classic") }}
),

DF_Classic_Mapping AS (
    SELECT   
        src.transaction_datetime,
        src.transaction_id,
        src.transaction_puid,
        src.transaction_src,
        src.transaction_type,
        src.transaction_status,
        src.src_account_number,
        src.src_bsb_number,
        src.src_product_code,
        src.src_sub_product_code,
        src.src_marketing_code,
        src.src_term, 
        src.dst_account_number,
        src.dst_bsb_number,
        rm_cl.portfolio,
        rm_cl.product_group,
        src.transaction_amount
    FROM DF_Classic src
    -- Get portfolio+product_group from Reg mapping by cost_centre
    LEFT JOIN {{ ref("bsb_cc_mapping") }} ccm_cl ON acc_type = 'DDA'
                                AND RIGHT(src.src_account_number,9) = ccm_cl.account_number 
                                AND cast(RIGHT(src.src_bsb_number,4) AS NUMERIC) = ccm_cl.bsb 
                                AND src.src_sub_product_code = ccm_cl.sub_product_code
                                AND CAST(src.transaction_datetime AS DATETIME) >= ccm_cl.dl__src_eff_from_dttm 
                                AND CAST(src.transaction_datetime AS DATETIME) <= ccm_cl.dl__src_eff_to_dttm 

    LEFT JOIN {{ ref("Reg_Data_Mapping") }} rm_cl ON src.src_product_code = rm_cl.product_code
                                AND  src.src_sub_product_code = rm_cl.sub_product_code
                                AND cast(ccm_cl.cost_centre AS STRING) = rm_cl.cost_centre 

),
DF_Plus_Mapping AS (
    SELECT
        src.transaction_datetime,
        src.transaction_id,
        src.transaction_puid,
        src.transaction_src,
        src.transaction_type,
        src.transaction_status,
        src.src_account_number,
        src.src_bsb_number,
        src.src_product_code,
        src.src_sub_product_code,
        src.src_marketing_code,
        src.src_term, 
        src.dst_account_number,
        src.dst_bsb_number,
        rm_pl.portfolio,
        rm_pl.product_group,
        src.transaction_amount
    FROM DF_Plus src
 -- Get portfolio+product_group from Reg mapping by  mkt_code
    LEFT JOIN {{ ref("Sr_Marketing_Code_Mapping") }} smc ON smc.account_number = src.src_account_number

    LEFT JOIN {{ ref("Reg_Data_Mapping") }} rm_pl ON concat(src.src_sub_product_code, concat('|',smc.marketing_code)) = rm_pl.sub_product_code
                                AND cast(RIGHT(src.src_bsb_number,4) AS STRING) = rm_pl.cost_centre
)

SELECT * FROM DF_Classic_Mapping
UNION ALL
SELECT * FROM DF_Plus_Mapping
