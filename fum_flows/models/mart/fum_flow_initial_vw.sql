SELECT
    transaction_date
    ,flow_direction
    ,IFNULL(src_fi, "N/A") src_fi
    ,IFNULL(src_product_code, "N/A") src_product_code
    ,IFNULL(src_sub_product_code, "N/A") src_sub_product_code
    ,IFNULL(src_marketing_code, "N/A") src_marketing_code
    ,IFNULL(src_interest_rate, "N/A") src_interest_rate
    ,IFNULL(src_term, "N/A") src_term
    ,IFNULL(dst_fi, "N/A") dst_fi
    ,IFNULL(dst_product_code, "N/A") dst_product_code
    ,IFNULL(dst_sub_product_code, "N/A") dst_sub_product_code
    ,IFNULL(dst_marketing_code, "N/A") dst_marketing_code
    ,IFNULL(dst_interest_rate, "N/A") dst_interest_rate
    ,IFNULL(dst_term, "N/A") dst_term
    ,all_retail_mfi_flag
    ,transaction_type
    ,transaction_amount
    ,transaction_count
    ,IF(all_retail_mfi_flag = "Y", transaction_count, 0) mfi_transaction_count
    ,IF(all_retail_mfi_flag = "Y", transaction_amount, 0) mfi_transaction_amount
    ,IF(transaction_type = "DIRECT_DEBIT", transaction_count, 0) direct_debit_transaction_count
    ,IF(transaction_type = "DIRECT_DEBIT", transaction_amount, 0) direct_debit_transaction_amount
    ,IF(transaction_type = "BPAY", transaction_count, 0) bpay_transaction_count
    ,IF(transaction_type = "BPAY", transaction_amount, 0) bpay_transaction_amount
    ,IF(transaction_type = "SALARY", transaction_count, 0) salary_transaction_count
    ,IF(transaction_type = "SALARY", transaction_amount, 0) salary_transaction_amount
    ,IF(transaction_type = "TRANSFER", transaction_count, 0) transfer_transaction_count
    ,IF(transaction_type = "TRANSFER", transaction_amount, 0) transfer_transaction_amount
    ,IF(transaction_type = "PAYMENT", transaction_count, 0) payment_transaction_count
    ,IF(transaction_type = "PAYMENT", transaction_amount, 0) payment_transaction_amount
    ,IF(transaction_type = "BSB_ACC_NUM", transaction_count, 0) bsb_acc_num_transaction_count
    ,IF(transaction_type = "BSB_ACC_NUM", transaction_amount, 0) bsb_acc_num_transaction_amount
    ,IF(transaction_type = "CARD", transaction_count, 0) card_transaction_count
    ,IF(transaction_type = "CARD", transaction_amount, 0) card_transaction_amount
    ,IF(transaction_type = "FEE", transaction_count, 0) fee_transaction_count
    ,IF(transaction_type = "FEE", transaction_amount, 0) fee_transaction_amount
    ,IF(transaction_type = "PAYID", transaction_count, 0) payid_transaction_count
    ,IF(transaction_type = "PAYID", transaction_amount, 0) payid_transaction_amount
    ,IF(transaction_type = "INTEREST", transaction_count, 0) interest_transaction_count
    ,IF(transaction_type = "INTEREST", transaction_amount, 0) interest_transaction_amount
    ,IF(transaction_type = "PAYTO", transaction_count, 0) payto_transaction_count
    ,IF(transaction_type = "PAYTO", transaction_amount, 0) payto_transaction_amount
    ,IF(transaction_type = "DEPOSIT_WITHDRAWAL", transaction_count, 0) deposit_withdrawal_transaction_count
    ,IF(transaction_type = "DEPOSIT_WITHDRAWAL", transaction_amount, 0) deposit_withdrawal_transaction_amount
    ,IF(transaction_type IS NULL, transaction_count, 0) unidentified_transaction_count
    ,IF(transaction_type IS NULL, transaction_amount, 0) unidentified_transaction_amount
FROM {{ ref("fum_flow") }}
qualify
    row_number() over (partition by row_key order by _insert_time ASC) = 1