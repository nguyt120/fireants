WITH 
    last_deleted_time AS (
        SELECT row_key, MAX(_insert_time) _last_deleted_time
        FROM {{ ref("fum_flow_v2") }}
        WHERE _deleted_flg = 1
        GROUP BY row_key
)

SELECT
    transaction_date
    ,flow_direction
    ,IFNULL(src_fi, "N/A") src_fi
    ,IFNULL(src_portfolio, "N/A") src_portfolio
    ,IFNULL(src_product_group, "N/A") src_product_group
    ,IFNULL(src_product_code, "N/A") src_product_code
    ,IFNULL(src_sub_product_code, "N/A") src_sub_product_code
    ,IFNULL(src_marketing_code, "N/A") src_marketing_code
    ,IFNULL(src_interest_rate, "N/A") src_interest_rate
    ,IFNULL(src_term, "N/A") src_term
    ,IFNULL(dst_fi, "N/A") dst_fi
    ,IFNULL(dst_portfolio, "N/A") dst_portfolio
    ,IFNULL(dst_product_group, "N/A") dst_product_group
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
LEFT JOIN last_deleted_time USING (row_key)
qualify
    row_number() over (partition by row_key order by _insert_time ASC) = 1
    AND _insert_time > COALESCE(_last_deleted_time, TIMESTAMP("1900-01-01"))
    AND _deleted_flg = 0