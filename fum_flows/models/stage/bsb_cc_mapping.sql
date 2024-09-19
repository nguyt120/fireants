WITH
dda_acc AS (
  -- only active dda account
  SELECT
    x783679_prdct_code          AS product_code,
    x783679_sub_prdct_code      AS sub_product_code,
    x783679_bsb                 AS bsb,
    x783679_cost_center         AS cost_centre,
    CAST(x783679_key AS STRING) AS account_number,
    dl__src_eff_from_dttm,
    dl__src_eff_to_dttm,
    'DDA'                       AS acc_type
  FROM {{ source("T1_CAP_AUS_CFDL_VW", "HFRTDDAD_CFDL_VW") }}
  WHERE x783679_prdct_code = 'DDA'
  -- AND x783679_act_status IN ('03', '99')
    -- AND __instance_id >= CURRENT_DATE() - 90
),

cda_acc AS (
  -- only active term deposit account
  SELECT
    x783652_prdct_code          AS product_code,
    x783652_sub_prdct_code      AS sub_product_code,
    x783652_branch_nbr          AS bsb,
    x783652_cost_center         AS cost_centre,
    CAST(x783652_key AS STRING) AS account_number,
    dl__src_eff_from_dttm,
    dl__src_eff_to_dttm,
    'CDA'                       AS acc_type
  FROM {{ source("T1_CAP_AUS_CFDL_VW", "CAP_AU_HFRTDA_CFDL_VW") }}
  WHERE x783652_prdct_code = 'CDA'
    -- AND x783652_act_status IN ('01', '03', '99')
    -- AND __instance_id >= CURRENT_DATE() - 90
),

hl_acc AS (
  -- only active homeloan account
  SELECT
    x711667_product_code                          AS product_code,
    x711667_loan_type                             AS sub_product_code,
    x711667_servicing_branch                      AS bsb,
    x711667_cost_center                           AS cost_centre,
    RIGHT(CAST(x711667_loan_number AS STRING), 9) AS account_number,
    dl__src_eff_from_dttm,
    dl__src_eff_to_dttm,
    'HL'                                          AS acc_type
  FROM {{ source("T1_CAP_AUS_CFDL_VW", "HFRLOANA_CFDL_VW") }}
  WHERE x711667_product_code = 'ILS'
  -- AND x711667_loan_status IS NULL
    -- AND __instance_id >= CURRENT_DATE() - 90
),

all_acc AS (
  SELECT * FROM dda_acc
  UNION ALL
  SELECT * FROM cda_acc
  UNION ALL
  SELECT * FROM hl_acc
),

final AS (
  SELECT 
    account_number, 
    bsb,
    acc_type,
    sub_product_code, 
    cost_centre, 
    MIN(dl__src_eff_from_dttm)  AS effective_from_datetime, 
    MAX(dl__src_eff_to_dttm)    AS effective_to_datetime
  FROM all_acc
  GROUP BY 1, 2, 3, 4, 5
)

SELECT * FROM final


