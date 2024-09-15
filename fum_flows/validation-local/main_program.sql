DECLARE validation_date_from, validation_date_to STRING ;
SET validation_date_from = '2024-08-22';
SET validation_date_to = '2024-09-10';

WITH 
bsb_cc_mapping AS (
  SELECT 
    product_code, 
    sub_product_code , 
    bsb,
    cost_centre,
    account_number, 
    dl__src_eff_from_dttm,
    dl__src_eff_to_dttm,
    acc_type
  FROM `anz-x-cosmos-prod-expt-3e0729.pd_cosmos_fireant_reporting.nguyek42_bsb_cc_mapping`
  WHERE 1=1
  AND DATE(validation_date_to) >= dl__src_eff_from_dttm 
  AND DATE(validation_date_from) <= dl__src_eff_to_dttm
)

SELECT * FROM 