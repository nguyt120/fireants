{{ config(materialized='view') }}

SELECT * FROM {{ ref("tn_fum_flow_whole_bank") }}