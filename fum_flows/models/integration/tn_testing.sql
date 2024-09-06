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

{% if not is_incremental() %}

SELECT *,
    0 AS _deleted_flg,
    CURRENT_TIMESTAMP() _insert_time
FROM {{ ref('tn_testing_stg') }}

{% else %}

SELECT *,
    CURRENT_TIMESTAMP() _insert_time
FROM {{ ff_insert_only(ref('tn_testing_stg')) }}

{% endif %}