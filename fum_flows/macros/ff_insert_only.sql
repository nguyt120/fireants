{% macro ff_insert_only(temp_relation) %}

    WITH
        current_target AS (
            SELECT *
            FROM {{ this }}
            qualify
                ROW_NUMBER() over (partition by row_key order by _insert_time DESC) = 1
                AND _deleted_flg = 0
        ),
        source AS (
            SELECT *
            FROM {{ temp_relation }}
        ),
        tracking_tb AS (
            SELECT
                COALESCE(TGT_TB.row_key, SRC_TB.row_key) AS row_key,
                COALESCE(TGT_TB.hash_diff, SRC_TB.hash_diff) AS hash_diff,
                CASE 
                    WHEN SRC_TB.row_key is null THEN "D"
                    WHEN TGT_TB.row_key is null THEN "I"
                END AS status_flg
            FROM current_target AS TGT_TB
            FULL JOIN source AS SRC_TB USING (row_key, hash_diff)
            WHERE TGT_TB.row_key IS NULL OR SRC_TB.row_key IS NULL
        ),
        insert_deleted_items AS (
            SELECT * except(_deleted_flg) FROM current_target
            WHERE CONCAT(row_key, hash_diff) IN (SELECT CONCAT(row_key, hash_diff) FROM tracking_tb WHERE status_flg = "D")
        ),
        insert_new_items AS (
            SELECT * FROM source
            WHERE CONCAT(row_key, hash_diff) IN (SELECT CONCAT(row_key, hash_diff) FROM tracking_tb WHERE status_flg = "I")
        ),
        insert_items AS (
            SELECT *, 1 AS _deleted_flg from insert_deleted_items
            UNION ALL
            SELECT *, 0 AS _deleted_flg from insert_new_items
        )

        SELECT * FROM insert_items

{% endmacro %}