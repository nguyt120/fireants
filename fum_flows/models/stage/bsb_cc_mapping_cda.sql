/*
bsb_cc_mapping_cda
*/
SELECT
    X783652_PRDCT_CODE      as product_code,
    X783652_SUB_PRDCT_CODE  as sub_product_code,
    X783652_BRANCH_NBR      as bsb, 
    X783652_COST_CENTER     as cost_centre,
    x783652_KEY             as account_number,
    dl__src_eff_from_dttm,
    dl__src_eff_to_dttm
FROM    {{ source("T1_CAP_AUS_CFDL_VW", "CAP_AU_HFRTDA_CFDL_VW") }}
WHERE   X783652_PRDCT_CODE = 'CDA' 
    AND X783652_ACT_STATUS in ('01','03','99')