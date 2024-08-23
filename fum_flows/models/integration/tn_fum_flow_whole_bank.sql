{{ config(materialized='table', alias=var('branch'), tags=["daily"]) }}

{% set data_source = ref("tn_stg_fum_flow_whole_bank") %}

{{ insert_overwrite_fum_flows(data_source) }}