{{ config(materialized="view") }}

WITH
-- Prepare transaction data
raw_transaction AS (
    SELECT * FROM {{ ref("DragonFish_Transaction") }}
    WHERE transaction_datetime < DATE_TRUNC(CURRENT_TIMESTAMP(), DAY)
),

valid_two_legs_puids AS (
    SELECT DISTINCT transaction_puid, COUNT(transaction_puid) leg_count
    FROM raw_transaction
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
        ,org.src_product_code
        ,org.src_sub_product_code
        ,org.src_marketing_code
        ,org.src_term
        ,map.src_account_number dst_account_number
        ,map.src_bsb_number	dst_bsb_number
        ,map.src_product_code	dst_product_code
        ,map.src_sub_product_code	dst_sub_product_code
        ,map.src_marketing_code dst_marketing_code
        ,map.src_term dst_term
        ,org.transaction_amount
    FROM raw_transaction org
    INNER JOIN raw_transaction map USING (transaction_puid)
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
        ,src_product_code
        ,src_sub_product_code
        ,src_marketing_code
        ,src_term
        ,dst_account_number
        ,dst_bsb_number
        ,CAST(NULL AS STRING)	dst_product_code
        ,CAST(NULL AS STRING)	dst_sub_product_code
        ,CAST(NULL AS STRING)	dst_marketing_code
        ,CAST(NULL AS STRING)	dst_term
        ,transaction_amount
    FROM raw_transaction
    -- Since transaction_id can be duplicated between classic and plus
    WHERE CONCAT(transaction_id, transaction_src) NOT IN (SELECT CONCAT(transaction_id, transaction_src) FROM mapping_transaction)
),

stg_transaction_whole_bank as (
    SELECT * FROM mapping_transaction
    UNION ALL
    SELECT * FROM no_mapping_transaction
    -- add term deposit transactions
    UNION ALL
    SELECT * FROM {{ ref("TermDeposit_Transaction") }}
),
-- Categorize trnx asset by product_group 
stg_transaction_whole_bank_mapping as (
    SELECT   
        src.*,
        case transaction_src 
            when 'classic' then  rm_cl.Retail_Mapping 
            when 'plus'    then  rm_pl.Retail_Mapping end as portfolio,
        case transaction_src 
            when 'classic' then  rm_cl.product_group 
            when 'plus'    then  rm_pl.product_group end as product_group
    FROM stg_transaction_whole_bank src
    -- Get portfolio+product_group from Reg mapping by cost_centre
        left JOIN {{ ref("bsb_cc_mapping_dda") }} ccm_cl on transaction_src = 'classic' 
                                    and RIGHT(src.src_account_number,9) = ccm_cl.account_number 
                                    and cast(RIGHT(src.src_bsb_number,4) as numeric) = ccm_cl.bsb 
                                    and src.src_sub_product_code = ccm_cl.sub_product_code
                                    and CAST(src.transaction_datetime AS DATETIME) >= ccm_cl.dl__src_eff_from_dttm 
                                    and CAST(src.transaction_datetime AS DATETIME) <= ccm_cl.dl__src_eff_to_dttm 

        left JOIN {{ ref("Reg_Data_Mapping") }} rm_cl on  transaction_src = 'classic' 
                                    and src.src_product_code = rm_cl.product_code
                                    and  src.src_sub_product_code = rm_cl.sub_product_code
                                    and cast(ccm_cl.cost_centre as string) = rm_cl.cost_centre 

    -- Get portfolio+product_group from Reg mapping by  mkt_code
        left JOIN {{ ref("sr_marketing_code_mapping") }} smc on transaction_src = 'plus' 
                                    and smc.account_number = src.src_account_number

        left JOIN {{ ref("Reg_Data_Mapping") }} rm_pl on transaction_src = 'plus' 
                                    and concat(src.src_sub_product_code, concat('|',smc.marketing_code)) = rm_pl.sub_product_code
                                    and cast(RIGHT(src.src_bsb_number,4) as string) = rm_pl.cost_centre
    WHERE transaction_src IN ('classic', 'plus')
-- TD - Term Deposit  CDA  
    UNION ALL
    SELECT src_td.*,
            rm_td.Retail_Mapping,
            rm_td.product_group,
    FROM stg_transaction_whole_bank src_td
    left JOIN {{ ref("bsb_cc_mapping_cda") }} ccm_td on CAST(SUBSTR(src_td.src_account_number, -9) AS NUMERIC) = ccm_td.account_number 
                                        and cast(src_td.src_bsb_number AS NUMERIC) = ccm_td.bsb
                                        and src_td.src_sub_product_code = ccm_td.sub_product_code
                                        and CAST(src_td.transaction_datetime AS DATETIME) >= ccm_td.dl__src_eff_from_dttm
                                        and CAST(src_td.transaction_datetime AS DATETIME) <= ccm_td.dl__src_eff_to_dttm 
    left JOIN {{ ref("Reg_Data_Mapping") }} rm_td on  src_td.src_product_code = rm_td.product_code
                                    and  src_td.src_sub_product_code = rm_td.sub_product_code
                                    and cast(ccm_td.cost_centre as string) = rm_td.cost_centre
    WHERE transaction_src = 'Term Deposit'   
)

SELECT * FROM stg_transaction_whole_bank_mapping