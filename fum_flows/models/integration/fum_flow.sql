{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["row_key", "hash_diff"],
        partition_by={
            "field": "transaction_date",
            "data_type": "date",
            "granularity": "day",
        }
    )
}}

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
        NULLIF(src_product_code, "")                                AS src_product_code,
        NULLIF(src_sub_product_code, "")                            AS src_sub_product_code,
        NULLIF(src_marketing_code, "")                              AS src_marketing_code,
        NULLIF(src_ofi.interest_rate, "")                           AS src_interest_rate,
        NULLIF(src_term, "")                                        AS src_term,
        NULLIF(dst_ofi.fi_name, "")                                 AS dst_fi,
        NULLIF(dst_product_code, "")                                AS dst_product_code,
        NULLIF(dst_sub_product_code, "")                            AS dst_sub_product_code,
        NULLIF(dst_marketing_code, "")                              AS dst_marketing_code,
        NULLIF(dst_ofi.interest_rate, "")                           AS dst_interest_rate,
        NULLIF(dst_term, "")                                        AS dst_term,
        transaction_amount,
        all_retail_mfi_flag
    FROM {{ ref("all_transaction") }} twb
    JOIN stg_deposit_account_customer dac ON twb.src_account_number = dac.account_number
        AND twb.src_product_code = dac.product_code 
        AND twb.src_sub_product_code = dac.sub_product_code
    JOIN stg_bsb_fi_interest_rate src_ofi ON twb.src_bsb_number = src_ofi.bsb_number
    JOIN stg_bsb_fi_interest_rate dst_ofi ON twb.dst_bsb_number = dst_ofi.bsb_number
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
        SUM(transaction_amount)                                                 AS total_amount,
        SUM(IF(all_retail_mfi_flag = "Y", 1, 0))                                AS mfi_transaction_count,
        SUM(IF(all_retail_mfi_flag = "Y", transaction_amount, 0))               AS mfi_transaction_amount,
        SUM(IF(transaction_type = "DIRECT_DEBIT", 1, 0))                        AS direct_debit_transaction_count,
        SUM(IF(transaction_type = "DIRECT_DEBIT", transaction_amount, 0))       AS direct_debit_transaction_amount,
        SUM(IF(transaction_type = "BPAY", 1, 0))                                AS bpay_transaction_count,
        SUM(IF(transaction_type = "BPAY", transaction_amount, 0))               AS bpay_transaction_amount,
        SUM(IF(transaction_type = "SALARY", 1, 0))                              AS salary_transaction_count,
        SUM(IF(transaction_type = "SALARY", transaction_amount, 0))             AS salary_transaction_amount,
        SUM(IF(transaction_type = "TRANSFER", 1, 0))                            AS transfer_transaction_count,
        SUM(IF(transaction_type = "TRANSFER", transaction_amount, 0))           AS transfer_transaction_amount,
        SUM(IF(transaction_type = "PAYMENT", 1, 0))                             AS payment_transaction_count,
        SUM(IF(transaction_type = "PAYMENT", transaction_amount, 0))            AS payment_transaction_amount,
        SUM(IF(transaction_type = "BSB_ACC_NUM", 1, 0))                         AS bsb_acc_num_transaction_count,
        SUM(IF(transaction_type = "BSB_ACC_NUM", transaction_amount, 0))        AS bsb_acc_num_transaction_amount,
        SUM(IF(transaction_type = "CARD", 1, 0))                                AS card_transaction_count,
        SUM(IF(transaction_type = "CARD", transaction_amount, 0))               AS card_transaction_amount,
        SUM(IF(transaction_type = "FEE", 1, 0))                                 AS fee_transaction_count,
        SUM(IF(transaction_type = "FEE", transaction_amount, 0))                AS fee_transaction_amount,
        SUM(IF(transaction_type = "PAYID", 1, 0))                               AS payid_transaction_count,
        SUM(IF(transaction_type = "PAYID", transaction_amount, 0))              AS payid_transaction_amount,
        SUM(IF(transaction_type = "INTEREST", 1, 0))                            AS interest_transaction_count,
        SUM(IF(transaction_type = "INTEREST", transaction_amount, 0))           AS interest_transaction_amount,
        SUM(IF(transaction_type = "PAYTO", 1, 0))                               AS payto_transaction_count,
        SUM(IF(transaction_type = "PAYTO", transaction_amount, 0))              AS payto_transaction_amount,
        SUM(IF(transaction_type = "DEPOSIT_WITHDRAWAL", 1, 0))                  AS deposit_withdrawal_transaction_count,
        SUM(IF(transaction_type = "DEPOSIT_WITHDRAWAL", transaction_amount, 0)) AS deposit_withdrawal_transaction_amount,
        SUM(IF(transaction_type IS NULL, 1, 0))                                 AS unidentified_transaction_count,
        SUM(IF(transaction_type IS NULL, transaction_amount, 0))                AS unidentified_transaction_amount,
    FROM fum_flow_whole_bank_not_aggregated
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
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
    )                   AS row_key,
    SHA256(
    CAST(total_amount AS STRING)
    || "|" || CAST(mfi_transaction_count AS STRING)
    || "|" || CAST(mfi_transaction_amount AS STRING)
    || "|" || CAST(direct_debit_transaction_count AS STRING)
    || "|" || CAST(direct_debit_transaction_amount AS STRING)
    || "|" || CAST(bpay_transaction_count AS STRING)
    || "|" || CAST(bpay_transaction_amount AS STRING)
    || "|" || CAST(salary_transaction_count AS STRING)
    || "|" || CAST(salary_transaction_amount AS STRING)
    || "|" || CAST(transfer_transaction_count AS STRING)
    || "|" || CAST(transfer_transaction_amount AS STRING)
    || "|" || CAST(payment_transaction_count AS STRING)
    || "|" || CAST(payment_transaction_amount AS STRING)
    || "|" || CAST(bsb_acc_num_transaction_count AS STRING)
    || "|" || CAST(bsb_acc_num_transaction_amount AS STRING)
    || "|" || CAST(card_transaction_count AS STRING)
    || "|" || CAST(card_transaction_amount AS STRING)
    || "|" || CAST(fee_transaction_count AS STRING)
    || "|" || CAST(fee_transaction_amount AS STRING)
    || "|" || CAST(payid_transaction_count AS STRING)
    || "|" || CAST(payid_transaction_amount AS STRING)
    || "|" || CAST(interest_transaction_count AS STRING)
    || "|" || CAST(interest_transaction_count AS STRING)
    || "|" || CAST(payto_transaction_count AS STRING)
    || "|" || CAST(payto_transaction_amount AS STRING)
    || "|" || CAST(deposit_withdrawal_transaction_count AS STRING)
    || "|" || CAST(deposit_withdrawal_transaction_amount AS STRING)
    || "|" || CAST(unidentified_transaction_count AS STRING)
    || "|" || CAST(unidentified_transaction_amount AS STRING)
    )                   AS hash_diff,
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
    total_amount,
    mfi_transaction_count,
    mfi_transaction_amount,
    direct_debit_transaction_count,
    direct_debit_transaction_amount,
    bpay_transaction_count,
    bpay_transaction_amount,
    salary_transaction_count,
    salary_transaction_amount,
    transfer_transaction_count,
    transfer_transaction_amount,
    payment_transaction_count,
    payment_transaction_amount,
    bsb_acc_num_transaction_count,
    bsb_acc_num_transaction_amount,
    card_transaction_count,
    card_transaction_amount,
    fee_transaction_count,
    fee_transaction_amount,
    payid_transaction_count,
    payid_transaction_amount,
    interest_transaction_count,
    interest_transaction_amount,
    payto_transaction_count,
    payto_transaction_amount,
    deposit_withdrawal_transaction_count,
    deposit_withdrawal_transaction_amount,
    unidentified_transaction_count,
    unidentified_transaction_amount,
    CURRENT_TIMESTAMP() AS _insert_time,
    CURRENT_TIMESTAMP() AS _update_time,
FROM fum_flow_whole_bank_aggregated
