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
    transaction_amount
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
    transaction_amount
FROM {{ source("dragonfish_transaction_v1", "transaction_anz_classic") }}