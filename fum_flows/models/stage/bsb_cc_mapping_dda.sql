/*
bsb_cc_mapping_dda
*/
SELECT
    X783679_PRDCT_CODE      as product_code,
    X783679_SUB_PRDCT_CODE  as sub_product_code,
    X783679_BSB             as bsb, 
    X783679_COST_CENTER     as cost_centre,
    x783679_KEY             as account_number,
    dl__src_eff_from_dttm,
    dl__src_eff_to_dttm
FROM  {{ source("T1_CAP_AUS_CFDL_VW", "HFRTDDAD_CFDL_VW") }}
WHERE 
    (
        (X783679_PRDCT_CODE = 'DDA' and X783679_ACT_STATUS in ('03','99'))
    OR
        (X783679_PRDCT_CODE = 'ILS' and X783679_ACT_STATUS is null)
    )