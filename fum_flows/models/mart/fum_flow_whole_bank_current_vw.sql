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
    ,total_amount
    ,mfi_transaction_count
    ,mfi_transaction_amount
    ,direct_debit_transaction_count
    ,direct_debit_transaction_amount
    ,bpay_transaction_count
    ,bpay_transaction_amount
    ,salary_transaction_count
    ,salary_transaction_amount
    ,transfer_transaction_count
    ,transfer_transaction_amount
    ,payment_transaction_count
    ,payment_transaction_amount
    ,bsb_acc_num_transaction_count
    ,bsb_acc_num_transaction_amount
    ,card_transaction_count
    ,card_transaction_amount
    ,fee_transaction_count
    ,fee_transaction_amount
    ,payid_transaction_count
    ,payid_transaction_amount
    ,interest_transaction_count
    ,interest_transaction_amount
    ,payto_transaction_count
    ,payto_transaction_amount
    ,deposit_withdrawal_transaction_count
    ,deposit_withdrawal_transaction_amount
    ,unidentified_transaction_count
    ,unidentified_transaction_amount
FROM {{ ref("fum_flow_whole_bank") }}