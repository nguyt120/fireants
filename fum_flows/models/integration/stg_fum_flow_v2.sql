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

stg_transaction_whole_bank AS (
    SELECT * FROM mapping_transaction
    UNION ALL
    SELECT * FROM no_mapping_transaction
    -- add term deposit transactions
    UNION ALL
    SELECT * FROM {{ ref("TermDeposit_Transaction") }}
),

-- Prepare customer & account data
raw_deposit_account_customer AS (
    SELECT
        dac.account_number,
        dac.product_code,
        dac.sub_product_code,
        marketing_code,
        SUM(all_retail_mfi_flag) total_retail_mfi_flag
    FROM {{ source("sirius_account_v1", "deposit_account_current") }} dac
    LEFT JOIN {{ source("sirius_customer_v1", "customer_to_account_current") }} ctac
        ON dac.account_number = ctac.account_number
        AND dac.product_code = ctac.product_code
        AND dac.sub_product_code = ctac.sub_product_code
    LEFT JOIN {{ source("sirius_reporting", "CUSTOMER_MFI_MONTHLY_SUMMARY") }} cmms
        ON ctac.ocv_id = cmms.ocv_id
        -- All transactions on month M reference M-1's MFI flag
        AND DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 day) = cmms.summary_date
    WHERE ctac.legal_owner_indicator = 'Y'
    GROUP BY 1, 2, 3, 4
),

stg_deposit_account_customer AS (
    SELECT
        account_number,
        product_code,
        sub_product_code,
        marketing_code,
        CASE WHEN total_retail_mfi_flag > 0 THEN "Y"
        ELSE "N" END all_retail_mfi_flag
    FROM raw_deposit_account_customer
),

-- Prepare OFI data
stg_bsb_fi_interest_rate AS (
    SELECT DISTINCT
        bsb_number,
        CASE WHEN bsb_number = "014111" THEN "Australia and New Zealand Banking Group Limited (ANZ Plus)" ELSE fi_name END fi_name,
        -- Currently we don't have a source for saving rates after July 10th, 2024. We'll update this once we got the new source.
        CAST(NULL AS STRING) interest_rate
    FROM {{ source("referencedata_anzx_rdm_entities_v1", "bsb_prdm_view") }}
),

-- Joining together for aggregation, apply some clean-up pre aggregation
fum_flow_whole_bank_not_aggregated AS (
    SELECT
        EXTRACT(DATE FROM transaction_datetime) transaction_date,
        transaction_type,
        CASE WHEN transaction_amount >= 0 THEN "IN" ELSE "OUT" END flow_direction,
        NULLIF(src_ofi.fi_name, "") src_fi,
        NULLIF(src_product_code, "") src_product_code,
        NULLIF(src_sub_product_code, "") src_sub_product_code,
        NULLIF(src_marketing_code, "") src_marketing_code,
        NULLIF(src_ofi.interest_rate, "") src_interest_rate,
        NULLIF(src_term, "") src_term,
        NULLIF(dst_ofi.fi_name, "") dst_fi,
        NULLIF(dst_product_code, "") dst_product_code,
        NULLIF(dst_sub_product_code, "") dst_sub_product_code,
        NULLIF(dst_marketing_code, "") dst_marketing_code,
        NULLIF(dst_ofi.interest_rate, "") dst_interest_rate,
        NULLIF(dst_term, "") dst_term,
        transaction_amount,
        all_retail_mfi_flag
    FROM stg_transaction_whole_bank twb
    LEFT JOIN stg_deposit_account_customer dac ON twb.src_account_number = dac.account_number
     AND twb.src_product_code = dac.product_code AND twb.src_sub_product_code = dac.sub_product_code
    LEFT JOIN stg_bsb_fi_interest_rate src_ofi ON twb.src_bsb_number = src_ofi.bsb_number
    LEFT JOIN stg_bsb_fi_interest_rate dst_ofi ON twb.dst_bsb_number = dst_ofi.bsb_number
),

-- Aggregate
fum_flow_whole_bank_aggregated AS (
    SELECT
        transaction_date,
        flow_direction,
        src_fi,
        src_product_code,
        src_sub_product_code,
        src_marketing_code,
        src_interest_rate,
        src_term,
        dst_fi,
        dst_product_code,
        dst_sub_product_code,
        dst_marketing_code,
        dst_interest_rate,
        dst_term,
        all_retail_mfi_flag,
        transaction_type,
        SUM(transaction_amount) transaction_amount,
        COUNT(1) transaction_count,
    FROM fum_flow_whole_bank_not_aggregated
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
)

SELECT
    SHA256(
    CAST(transaction_date AS STRING)
    || "|" || flow_direction
    || "|" || UPPER(TRIM(IFNULL(src_fi, "")))
    || "|" || TRIM(IFNULL(src_product_code, ""))
    || "|" || TRIM(IFNULL(src_sub_product_code, ""))
    || "|" || TRIM(IFNULL(src_marketing_code, ""))
    || "|" || TRIM(IFNULL(src_interest_rate, ""))
    || "|" || TRIM(IFNULL(src_term, ""))
    || "|" || UPPER(TRIM(IFNULL(dst_fi, "")))
    || "|" || TRIM(IFNULL(dst_product_code, ""))
    || "|" || TRIM(IFNULL(dst_sub_product_code, ""))
    || "|" || TRIM(IFNULL(dst_marketing_code, ""))
    || "|" || TRIM(IFNULL(dst_interest_rate, ""))
    || "|" || TRIM(IFNULL(dst_term, ""))
    || "|" || TRIM(IFNULL(all_retail_mfi_flag, ""))
    || "|" || TRIM(IFNULL(transaction_type, ""))
    ) row_key,
    SHA256(
    CAST(transaction_amount AS STRING)
    || "|" || CAST(transaction_count AS STRING)
    ) hash_diff,
    transaction_date,
    flow_direction,
    src_fi,
    src_product_code,
    src_sub_product_code,
    src_marketing_code,
    src_interest_rate,
    src_term,
    dst_fi,
    dst_product_code,
    dst_sub_product_code,
    dst_marketing_code,
    dst_interest_rate,
    dst_term,
    transaction_type,
    all_retail_mfi_flag,
    transaction_amount,
    transaction_count,
FROM fum_flow_whole_bank_aggregated
