{{ config(materialized="view") }}
WITH
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
        EXTRACT(DATE FROM transaction_datetime)                     AS transaction_date,
        transaction_type,
        CASE WHEN transaction_amount >= 0 THEN "IN" ELSE "OUT" END  AS flow_direction,
        NULLIF(src_ofi.fi_name, "")                                 AS src_fi,
        NULLIF(src_portfolio, "")                                   AS src_portfolio,
        NULLIF(src_product_group, "")                               AS src_product_group,
        NULLIF(src_product_code, "")                                AS src_product_code,
        NULLIF(src_sub_product_code, "")                            AS src_sub_product_code,
        NULLIF(src_marketing_code, "")                              AS src_marketing_code,
        NULLIF(src_ofi.interest_rate, "")                           AS src_interest_rate,
        NULLIF(src_term, "")                                        AS src_term,
        NULLIF(dst_ofi.fi_name, "")                                 AS dst_fi,
        NULLIF(dst_portfolio, "")                                   AS dst_portfolio,
        NULLIF(dst_product_group, "")                               AS dst_product_group,
        NULLIF(dst_product_code, "")                                AS dst_product_code,
        NULLIF(dst_sub_product_code, "")                            AS dst_sub_product_code,
        NULLIF(dst_marketing_code, "")                              AS dst_marketing_code,
        NULLIF(dst_ofi.interest_rate, "")                           AS dst_interest_rate,
        NULLIF(dst_term, "")                                        AS dst_term,
        transaction_amount,
        all_retail_mfi_flag
    FROM {{ ref("all_transaction") }} twb
    LEFT JOIN stg_deposit_account_customer dac ON twb.src_account_number = dac.account_number
        AND twb.src_product_code = dac.product_code 
        AND twb.src_sub_product_code = dac.sub_product_code
    LEFT JOIN stg_bsb_fi_interest_rate src_ofi ON twb.src_bsb_number = src_ofi.bsb_number
    LEFT JOIN stg_bsb_fi_interest_rate dst_ofi ON twb.dst_bsb_number = dst_ofi.bsb_number
),

-- Aggregate
fum_flow_whole_bank_aggregated AS (
    SELECT
        transaction_date,
        flow_direction,
        src_fi,
        src_portfolio,
        src_product_group,
        src_product_code,
        src_sub_product_code,
        src_marketing_code,
        src_interest_rate,
        src_term,
        dst_fi,
        dst_portfolio,
        dst_product_group,
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
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20
)

SELECT
    SHA256(
    CAST(transaction_date AS STRING)
    || "|" || flow_direction
    || "|" || UPPER(TRIM(IFNULL(src_fi, "")))
    || "|" || UPPER(TRIM(IFNULL(src_portfolio, "")))
    || "|" || UPPER(TRIM(IFNULL(src_product_group, "")))
    || "|" || TRIM(IFNULL(src_product_code, ""))
    || "|" || TRIM(IFNULL(src_sub_product_code, ""))
    || "|" || TRIM(IFNULL(src_marketing_code, ""))
    || "|" || TRIM(IFNULL(src_interest_rate, ""))
    || "|" || TRIM(IFNULL(src_term, ""))
    || "|" || UPPER(TRIM(IFNULL(dst_fi, "")))
    || "|" || UPPER(TRIM(IFNULL(dst_portfolio, "")))
    || "|" || UPPER(TRIM(IFNULL(dst_product_group, "")))
    || "|" || TRIM(IFNULL(dst_product_code, ""))
    || "|" || TRIM(IFNULL(dst_sub_product_code, ""))
    || "|" || TRIM(IFNULL(dst_marketing_code, ""))
    || "|" || TRIM(IFNULL(dst_interest_rate, ""))
    || "|" || TRIM(IFNULL(dst_term, ""))
    || "|" || TRIM(IFNULL(all_retail_mfi_flag, ""))
    || "|" || TRIM(IFNULL(transaction_type, ""))
    ) AS row_key,
    SHA256(
    CAST(transaction_amount AS STRING)
    || "|" || CAST(transaction_count AS STRING)
    ) AS hash_diff,
    transaction_date,
    flow_direction,
    src_fi,
    src_portfolio,
    src_product_group,
    src_product_code,
    src_sub_product_code,
    src_marketing_code,
    src_interest_rate,
    src_term,
    dst_fi,
    dst_portfolio,
    dst_product_group,
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
