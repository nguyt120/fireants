{{ config(materialized='view') }}

SELECT * FROM {{ ref("fum_flow_whole_bank") }}