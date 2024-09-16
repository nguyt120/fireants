WITH
dda_acc AS (
  -- only active dda account
    SELECT
        X783679_PRDCT_CODE          as product_code,
        X783679_SUB_PRDCT_CODE      as sub_product_code,
        X783679_BSB                 as bsb,
        X783679_COST_CENTER         as cost_centre,
        CAST(x783679_KEY AS STRING) as account_number,
        dl__src_eff_from_dttm,
        dl__src_eff_to_dttm,
        'DDA'                       as acc_type
    FROM {{ source("T1_CAP_AUS_CFDL_VW", "HFRTDDAD_CFDL_VW") }}
    WHERE 1=1
        AND (X783679_PRDCT_CODE = 'DDA' AND X783679_ACT_STATUS IN ('03','99'))
),

cda_acc AS (
  -- only active term deposit account
    SELECT
        X783652_PRDCT_CODE          as product_code,
        X783652_SUB_PRDCT_CODE      as sub_product_code,
        X783652_BRANCH_NBR          as bsb,
        X783652_COST_CENTER         as cost_centre,
        CAST(x783652_KEY AS STRING) as account_number,
        dl__src_eff_from_dttm,
        dl__src_eff_to_dttm,
        'CDA'                       as acc_type
    FROM   {{ source("T1_CAP_AUS_CFDL_VW", "CAP_AU_HFRTDA_CFDL_VW") }}
    WHERE 1=1
        AND (X783652_PRDCT_CODE = 'CDA' AND X783652_ACT_STATUS in ('01','03','99'))
),

hl_acc AS (
  -- only active homeloan account
    SELECT
        X783679_PRDCT_CODE          as product_code,
        X783679_SUB_PRDCT_CODE      as sub_product_code,
        X783679_BSB                 as bsb,
        X783679_COST_CENTER         as cost_centre,
        CAST(x783679_KEY AS STRING) as account_number,
        dl__src_eff_from_dttm,
        dl__src_eff_to_dttm,
        'HL'                        as acc_type
    FROM {{ source("T1_CAP_AUS_CFDL_VW", "HFRTDDAD_CFDL_VW") }}
    WHERE 1=1
        AND (X783679_PRDCT_CODE = 'ILS' AND X783679_ACT_STATUS IS NULL)
)

    SELECT * FROM dda_acc
    UNION ALL 
    SELECT * FROM cda_acc
    UNION ALL 
    SELECT * FROM hl_acc