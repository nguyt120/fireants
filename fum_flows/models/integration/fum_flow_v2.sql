{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        partition_by={
            "field": "transaction_date",
            "data_type": "date",
            "granularity": "day",
        }
    )
}}

{% set snapshot_source = ref("stg_fum_flow_v2") %}

WITH

{% if not is_incremental() %}

    insert_records AS (
        SELECT *,
            0 AS _deleted_flg,
        FROM {{ snapshot_source }}
    )

{% else %}

    current_target AS (
        SELECT *
        FROM {{ this }}
        qualify
            ROW_NUMBER() over (partition by row_key order by _insert_time DESC) = 1
            AND _deleted_flg = 0
    ),
    source AS ( SELECT * FROM {{ snapshot_source }} ),
    {#/* getting current records those are not exist in the source, so it is deleted */#}
    insert_deleted_records AS (
        SELECT * EXCEPT(_deleted_flg, _insert_time) FROM current_target
        WHERE NOT EXISTS
            (
                SELECT 1 FROM source
                WHERE source.transaction_date = current_target.transaction_date
                AND source.row_key = current_target.row_key
            )
    ),
    {#/* getting records from source those are not exist in the current, so it is new */#}
    insert_new_records AS (
        SELECT * FROM source
        WHERE NOT EXISTS
            (
                SELECT 1 FROM current_target
                WHERE source.transaction_date = current_target.transaction_date
                AND source.row_key = current_target.row_key
            )
    ),
    {#/* getting records from source those are exist in the current, but it is updated */#}
    insert_updated_records AS (
        SELECT * FROM source
        WHERE EXISTS
            (
                SELECT 1 FROM current_target
                WHERE source.transaction_date = current_target.transaction_date
                AND source.row_key = current_target.row_key
                AND source.hash_diff <> current_target.hash_diff
            )
    ),

    insert_records AS (
        SELECT *, 1 AS _deleted_flg from insert_deleted_records
        UNION ALL
        SELECT *, 0 AS _deleted_flg from insert_new_records
        UNION ALL
        SELECT *, 0 AS _deleted_flg from insert_updated_records
    )

{% endif %}

SELECT *,
    CURRENT_TIMESTAMP() AS _insert_time
FROM insert_records
