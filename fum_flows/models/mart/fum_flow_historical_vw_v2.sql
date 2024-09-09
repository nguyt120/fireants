SELECT
    row_key _id,
    _insert_time _eff_from,
    TIMESTAMP_SUB(LEAD(_insert_time) over (partition by row_key order by _insert_time), INTERVAL 1 MICROSECOND) _eff_to,
    _deleted_flg,
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
    ,IFNULL(transaction_type, "N/A") transaction_type
    ,IFNULL(all_retail_mfi_flag, "N/A") all_retail_mfi_flag
    ,transaction_amount
    ,transaction_count
FROM {{ ref("fum_flow_v2") }}