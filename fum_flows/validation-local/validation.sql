select
    v.transaction_date as transaction_date,
    v.portfolio,
    v.product_group as product_group,
    v.daily_movement as amount
  FROM `anz-x-cosmos-prod-expt-3e0729.pd_cosmos_fireant_reporting.psgl_validation_main` v
  WHERE 1=1
AND v.transaction_date >= DATE(validation_date_from)
AND v.transaction_date <= DATE(validation_date_to)