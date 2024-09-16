CREATE OR REPLACE TABLE `anz-x-cosmos-prod-expt-3e0729.pd_cosmos_fireant_reporting.nguyek42_reg_mapping_union`
AS 
WITH 
reg_mapping AS (
  -- 8190 
  SELECT 
    FUM AS fum,
    BU AS bu,
    DIVISION_NAME AS division_name,
    Retail_Mapping AS retail_mapping,
    PSGL_PRODUCT_CODE AS psgl_code,
    LEFT(CAST(PROFIT_CENTRE AS string),4) AS cost_centre,
    CASE
      WHEN DEAL_SUBTYPE LIKE '%V2%' THEN 'DDA'
      ELSE DEAL_TYPE
    END AS product_code,
    CASE
      WHEN DEAL_SUBTYPE LIKE '%V2%' THEN 'V2'
      ELSE DEAL_SUBTYPE
    END AS sub_product_code
FROM `anz-x-cosmos-prod-expt-3e0729.pd_cosmos_fireant_reporting.reg_mapping_v2`
),

reg_mapping_tdp as (
  -- filter for only term deposit, in '3023','3798
  -- bu: bank prod
  -- count: 6, distinct 6
  SELECT 
    rm.fum,
    rm.bu,
    rm.division_name,
    rm.retail_mapping,
    rm.psgl_code,
    rm.cost_centre,
    rm.product_code,
    rm.sub_product_code,
    'Third Party Deposits' as product_group
  FROM reg_mapping AS rm
  WHERE 1=1
  AND rm.bu = 'Banking Prod'
  AND rm.fum = 'Deposits and other borrowings'
  AND rm.cost_centre in ('3023','3798')
  AND rm.division_name = 'Australia Retail Division'
  AND rm.retail_mapping in ('TD Portfolio')
),

reg_mapping_retail_dep as (
  -- filter the other not '3023','3798'
  -- bu: bank prod
  -- count: 545 distinct 542
  SELECT 
    rm.fum,
    rm.bu,
    rm.division_name,
    rm.retail_mapping,
    rm.psgl_code,
    rm.cost_centre,
    rm.product_code,
    rm.sub_product_code,
    CASE
      WHEN psgl_code = 'TD0001' and sub_product_code in ('AA', 'AB', 'AC', 'AD', 'AE', 'AG', 'AO', 'AR', 'AU') 
      THEN 'Retail'

      WHEN psgl_code = 'TD0003' and sub_product_code in ('TA', 'TB', 'TC', 'TD', 'TE', 'TF', 'TG', 'TH', 'TI', 'TJ', 'TK', 'TL')  
      THEN 'Advance Notice TD - Retail'

      WHEN psgl_code = 'CMA002' and sub_product_code in ('EB') 
      THEN 'CMA'

      WHEN psgl_code = 'CMA002' and sub_product_code in ('ET') 
      THEN 'ANZ Share Investing'

      WHEN psgl_code = 'DDASAZ' and sub_product_code in ('SA') 
      THEN 'Progress Saver'

      WHEN psgl_code = 'ONLSAV' and sub_product_code in ('ED') 
      THEN 'Online Saver'

      WHEN psgl_code = 'ONLSAV' and sub_product_code in ('XD|SAVING01','XD|SAVING02') 
      THEN 'ANZ Plus Save'

      WHEN psgl_code = 'SMSFCH' and sub_product_code in ('HS') 
      THEN 'SMSF Cash Hub Account'

      WHEN psgl_code = 'V2PLUS' and sub_product_code in ('V2') 
      THEN 'V2 Plus'

      WHEN psgl_code = 'ACC002' and sub_product_code in ('CF') 
      THEN 'Pensioner Advantage'

      WHEN psgl_code = 'ACC003' and sub_product_code in ('CX','PT') 
      THEN 'Access'

      WHEN psgl_code = 'ACC003' and sub_product_code in ('XD|TRANSACT01') 
      THEN 'ANZ Plus Transact'

      WHEN psgl_code = 'CABI03' and sub_product_code in ('S1') 
      THEN 'CABI'

      WHEN psgl_code = 'DDAS2Z' and sub_product_code in ('S2') 
      THEN 'CABI'

      WHEN psgl_code = 'CANBI2' and sub_product_code in ('CB') 
      THEN 'CABI'

      WHEN psgl_code = 'HPP001' and sub_product_code in ('BD','BE','BG') 
      THEN 'HPP'

      WHEN psgl_code = 'RSVBAZ' and sub_product_code in ('BA','BB') 
      THEN 'HPP'

      ELSE 'Unknown'
    END AS product_group
  FROM reg_mapping rm
  WHERE 1=1 
  AND rm.cost_centre not in ('3023','3798')
  AND rm.bu = 'Banking Prod'
  AND rm.fum = 'Deposits and other borrowings'
  AND rm.division_name = 'Australia Retail Division'
  AND rm.retail_mapping in ('TD Portfolio','Savings Portfolio','Transaction Portfolio')
),

reg_mapping_hl_offset as (
  -- filter for only homeloan
  -- bu: home loans | fum: deposit
  -- product group: Home loan offset
  -- count: 110; distinct: 109
  SELECT 
    rm.fum,
    rm.bu,
    rm.division_name,
    rm.retail_mapping,
    rm.psgl_code,
    rm.cost_centre,
    rm.product_code,
    rm.sub_product_code,
    'Home Loans Offsets' as product_group
  FROM reg_mapping AS rm
  WHERE 1=1
  AND rm.cost_centre not in ('3023','3798')
  AND rm.bu = 'HOME LOANS'
  AND rm.fum = 'Deposits and other borrowings'
  AND rm.division_name = 'Australia Retail Division'
),

-- reg_mapping_rt_lending_hl as (
reg_mapping_hl_nla as(
  -- filter for only homeloan | fum: net loans and advance
  -- product group: Variable, Unproductive, Fixed, Equity
  -- count 771 distinct 771
  SELECT
    rm.fum,
    rm.bu,
    rm.division_name,
    rm.retail_mapping,
    rm.psgl_code,
    rm.cost_centre,
    rm.product_code,
    rm.sub_product_code,
    rm.retail_mapping as product_group -- Variable, Unproductive, Fixed, Equity
  FROM reg_mapping rm
  WHERE 1=1
  AND rm.bu = 'HOME LOANS'
  AND rm.fum = 'Net loans and advances'
  AND rm.division_name = 'Australia Retail Division'
),

reg_mapping_rt_lending_CC as(
  -- count 44  distinct 24
  SELECT
    rm.fum,
    rm.bu,
    rm.division_name,
    rm.retail_mapping,
    rm.psgl_code,
    rm.cost_centre,
    rm.product_code,
    rm.sub_product_code,
    rm.bu  as product_group -- Consumer Cards
  FROM reg_mapping rm
  WHERE 1=1
  AND BU = 'Consumer Cards'
  AND FUM = 'Net loans and advances'
  AND division_name = 'Australia Retail Division'
),
 
/* adding retail lending -  Personal Lending */
reg_mapping_rt_lending_PL as(
  -- count 55 distinct 53
  SELECT 
    rm.fum,
    rm.bu,
    rm.division_name,
    rm.retail_mapping,
    rm.psgl_code,
    rm.cost_centre,
    rm.product_code,
    rm.sub_product_code,
    rm.bu  as product_group -- Personal Lending
  FROM reg_mapping rm
  WHERE 1=1
  AND BU = 'Personal Lending'
  AND FUM = 'Net loans and advances'
  AND division_name = 'Australia Retail Division'
),

reg_mapping_union_bronze AS(
  SELECT * FROM reg_mapping_tdp
  UNION ALL 
  SELECT * FROM reg_mapping_retail_dep
  UNION ALL
  SELECT * FROM reg_mapping_hl_offset
  UNION ALL 
  SELECT * FROM reg_mapping_hl_nla
  UNION ALL
  SELECT * FROM reg_mapping_rt_lending_CC
  UNION ALL 
  SELECT  * FROM reg_mapping_rt_lending_PL
),

reg_mapping_union_silver AS (
  SELECT 
    rmu.fum,
    rmu.bu,
    rmu.division_name,
    -- rmu.retail_mapping,
    rmu.psgl_code,
    rmu.cost_centre,
    rmu.product_code,
    rmu.sub_product_code,
    rmu.product_group,
    CASE
      WHEN bu IN ('Consumer Cards', 'Personal Lending') THEN 'Cards and Payments'
      WHEN product_group = 'Home Loans Offsets' THEN 'Home Loans Offsets'
      WHEN bu IN ('HOME LOANS') THEN 'Home Loans'
      ELSE Retail_Mapping
    END AS portfolio
  FROM reg_mapping_union_bronze rmu
),

reg_mapping_union_gold AS (
  SELECT * EXCEPT(psgl_code) FROM reg_mapping_union_silver
),

reg_mapping_union AS (
  SELECT DISTINCT * FROM reg_mapping_union_gold
)

SELECT * FROM reg_mapping_union