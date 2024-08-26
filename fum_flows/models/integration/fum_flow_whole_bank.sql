{{
    config(
        materialized="table",
        partition_by={
            "field": "transaction_date",
            "data_type": "date",
            "granularity": "day",
        }
    )
}}

WITH
-- Prepare transaction data
raw_transaction AS (
    SELECT
        EXTRACT(DATE FROM transaction_datetime) transaction_date,
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
        transaction_amount,
        EXTRACT(DATE FROM last_update_time) last_update_date
    FROM {{ source("dragonfish_transaction_v1", "transaction_anz_plus") }}

    UNION ALL

    SELECT
        EXTRACT(DATE FROM transaction_datetime) transaction_date,
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
        transaction_amount,
        EXTRACT(DATE FROM last_update_time) last_update_date
    FROM {{ source("dragonfish_transaction_v1", "transaction_anz_classic") }}
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
        org.transaction_date
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
        ,org.last_update_date
    FROM raw_transaction org
    INNER JOIN raw_transaction map USING (transaction_puid)
    WHERE org.transaction_puid IN (SELECT transaction_puid FROM valid_two_legs_puids)
    AND org.src_account_number != map.src_account_number
),

no_mapping_transaction AS (
    SELECT
        transaction_date
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
        ,last_update_date
    FROM raw_transaction
    -- Since transaction_id can be duplicated between classic and plus
    WHERE CONCAT(transaction_id, transaction_src) NOT IN (SELECT CONCAT(transaction_id, transaction_src) FROM mapping_transaction)
),

stg_transaction_whole_bank AS (
    SELECT * FROM mapping_transaction
    UNION ALL
    SELECT * FROM no_mapping_transaction
),

-- Prepare customer & account data
raw_deposit_account_customer AS (
    SELECT
        CURRENT_DATE() snapshot_date,
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
    GROUP BY 1, 2, 3, 4, 5
),

stg_deposit_account_customer AS (
    SELECT
        snapshot_date,
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

-- Joining together for aggregation
fum_flow_whole_bank_not_aggregated AS (
    SELECT
        transaction_date,
        transaction_type,
        CASE WHEN transaction_amount >= 0 THEN "IN" ELSE "OUT" END flow_direction,
        src_ofi.fi_name src_fi,
        src_product_code,
        src_sub_product_code,
        src_marketing_code,
        src_ofi.interest_rate src_interest_rate,
        src_term,
        dst_ofi.fi_name dst_fi,
        dst_product_code,
        dst_sub_product_code,
        dst_marketing_code,
        dst_ofi.interest_rate dst_interest_rate,
        dst_term,
        transaction_amount,
        all_retail_mfi_flag
    FROM stg_transaction_whole_bank twb
    JOIN stg_deposit_account_customer dac ON twb.src_account_number = dac.account_number
     AND twb.src_product_code = dac.product_code AND twb.src_sub_product_code = dac.sub_product_code
    JOIN stg_bsb_fi_interest_rate src_ofi ON twb.src_bsb_number = src_ofi.bsb_number
    JOIN stg_bsb_fi_interest_rate dst_ofi ON twb.dst_bsb_number = dst_ofi.bsb_number
    -- WHERE last_update_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) -- Get transactions updated in D-1
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
        SUM(transaction_amount) total_amount,
        SUM(IF(all_retail_mfi_flag = "Y", 1, 0)) mfi_transaction_count,
        SUM(IF(all_retail_mfi_flag = "Y", transaction_amount, 0)) mfi_transaction_amount,
        SUM(IF(transaction_type = "DIRECT_DEBIT", 1, 0)) direct_debit_transaction_count,
        SUM(IF(transaction_type = "DIRECT_DEBIT", transaction_amount, 0)) direct_debit_transaction_amount,
        SUM(IF(transaction_type = "BPAY", 1, 0)) bpay_transaction_count,
        SUM(IF(transaction_type = "BPAY", transaction_amount, 0)) bpay_transaction_amount,
        SUM(IF(transaction_type = "SALARY", 1, 0)) salary_transaction_count,
        SUM(IF(transaction_type = "SALARY", transaction_amount, 0)) salary_transaction_amount,
        SUM(IF(transaction_type = "TRANSFER", 1, 0)) transfer_transaction_count,
        SUM(IF(transaction_type = "TRANSFER", transaction_amount, 0)) transfer_transaction_amount,
        SUM(IF(transaction_type = "PAYMENT", 1, 0)) payment_transaction_count,
        SUM(IF(transaction_type = "PAYMENT", transaction_amount, 0)) payment_transaction_amount,
        SUM(IF(transaction_type = "BSB_ACC_NUM", 1, 0)) bsb_acc_num_transaction_count,
        SUM(IF(transaction_type = "BSB_ACC_NUM", transaction_amount, 0)) bsb_acc_num_transaction_amount,
        SUM(IF(transaction_type = "CARD", 1, 0)) card_transaction_count,
        SUM(IF(transaction_type = "CARD", transaction_amount, 0)) card_transaction_amount,
        SUM(IF(transaction_type = "FEE", 1, 0)) fee_transaction_count,
        SUM(IF(transaction_type = "FEE", transaction_amount, 0)) fee_transaction_amount,
        SUM(IF(transaction_type = "PAYID", 1, 0)) payid_transaction_count,
        SUM(IF(transaction_type = "PAYID", transaction_amount, 0)) payid_transaction_amount,
        SUM(IF(transaction_type = "INTEREST", 1, 0)) interest_transaction_count,
        SUM(IF(transaction_type = "INTEREST", transaction_amount, 0)) interest_transaction_amount,
        SUM(IF(transaction_type = "PAYTO", 1, 0)) payto_transaction_count,
        SUM(IF(transaction_type = "PAYTO", transaction_amount, 0)) payto_transaction_amount,
        SUM(IF(transaction_type = "DEPOSIT_WITHDRAWAL", 1, 0)) deposit_withdrawal_transaction_count,
        SUM(IF(transaction_type = "DEPOSIT_WITHDRAWAL", transaction_amount, 0)) deposit_withdrawal_transaction_amount,
        SUM(IF(transaction_type IS NULL, 1, 0)) unidentified_transaction_count,
        SUM(IF(transaction_type IS NULL, transaction_amount, 0)) unidentified_transaction_amount,
    FROM fum_flow_whole_bank_not_aggregated
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
)

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
    unidentified_transaction_amount
FROM fum_flow_whole_bank_aggregated
