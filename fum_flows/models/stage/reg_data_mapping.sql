/*
  1. Retail Deposit - rd
    1.1 Third Party Deposit (TD-TPD)
    1.2 Term Deposit Retail + Advance notice TD
    1.3 Home Loan Offset
  2. Retail lending - rl
    2.1 Home Loans
    2.2 Consumer Cards
    2.3 Personal Lending
  3. Reg mapping union
*/

WITH
reg_mapping AS (
  SELECT
    fum,
    bu,
    division_name,
    retail_mapping,
    psgl_product_code                      AS psgl_code,
    LEFT(CAST(profit_centre AS STRING), 4) AS cost_centre,
    CASE
      WHEN deal_subtype LIKE '%V2%' THEN 'DDA'
      ELSE deal_type
    END                                    AS product_code,
    CASE
      WHEN deal_subtype LIKE '%V2%' THEN 'V2'
      ELSE deal_subtype
    END                                    AS sub_product_code
  FROM {{ ref("seed_reg_data_mapping") }}
),

/*1. Retail Deposit - rd */
--1.1 Third Party Deposit (TD-TPD)
rm_rd_tpd AS (
  SELECT
    rm.fum,
    rm.bu,
    rm.division_name,
    rm.retail_mapping,
    rm.psgl_code,
    rm.cost_centre,
    rm.product_code,
    rm.sub_product_code,
    'Third Party Deposits' AS product_group
  FROM reg_mapping AS rm
  WHERE 1 = 1
    AND rm.bu = 'Banking Prod'
    AND rm.fum = 'Deposits and other borrowings'
    AND rm.cost_centre IN ('3023', '3798')
    AND rm.division_name = 'Australia Retail Division'
    AND rm.retail_mapping IN ('TD Portfolio')
),
--1.2 Term Deposit Retail + Advance notice TD
rm_rd_td_retail AS (
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
      WHEN psgl_code = 'TD0001' AND sub_product_code IN ('AA', 'AB', 'AC', 'AD', 'AE', 'AG', 'AO', 'AR', 'AU')
        THEN 'Retail'

      WHEN psgl_code = 'TD0003' AND sub_product_code IN ('TA', 'TB', 'TC', 'TD', 'TE', 'TF', 'TG', 'TH', 'TI', 'TJ', 'TK', 'TL')
        THEN 'Advance Notice TD - Retail'

      WHEN psgl_code = 'CMA002' AND sub_product_code IN ('EB')
        THEN 'CMA'

      WHEN psgl_code = 'CMA002' AND sub_product_code IN ('ET')
        THEN 'ANZ Share Investing'

      WHEN psgl_code = 'DDASAZ' AND sub_product_code IN ('SA')
        THEN 'Progress Saver'

      WHEN psgl_code = 'ONLSAV' AND sub_product_code IN ('ED')
        THEN 'Online Saver'

      WHEN psgl_code = 'ONLSAV' AND sub_product_code IN ('XD|SAVING01', 'XD|SAVING02')
        THEN 'ANZ Plus Save'

      WHEN psgl_code = 'SMSFCH' AND sub_product_code IN ('HS')
        THEN 'SMSF Cash Hub Account'

      WHEN psgl_code = 'V2PLUS' AND sub_product_code IN ('V2')
        THEN 'V2 Plus'

      WHEN psgl_code = 'ACC002' AND sub_product_code IN ('CF')
        THEN 'Pensioner Advantage'

      WHEN psgl_code = 'ACC003' AND sub_product_code IN ('CX', 'PT')
        THEN 'Access'

      WHEN psgl_code = 'ACC003' AND sub_product_code IN ('XD|TRANSACT01')
        THEN 'ANZ Plus Transact'

      WHEN psgl_code = 'CABI03' AND sub_product_code IN ('S1')
        THEN 'CABI'

      WHEN psgl_code = 'DDAS2Z' AND sub_product_code IN ('S2')
        THEN 'CABI'

      WHEN psgl_code = 'CANBI2' AND sub_product_code IN ('CB')
        THEN 'CABI'

      WHEN psgl_code = 'HPP001' AND sub_product_code IN ('BD', 'BE', 'BG')
        THEN 'HPP'

      WHEN psgl_code = 'RSVBAZ' AND sub_product_code IN ('BA', 'BB')
        THEN 'HPP'

      ELSE 'Unknown'
    END AS product_group
  FROM reg_mapping AS rm
  WHERE 1 = 1
    AND rm.cost_centre NOT IN ('3023', '3798')
    AND rm.bu = 'Banking Prod'
    AND rm.fum = 'Deposits and other borrowings'
    AND rm.division_name = 'Australia Retail Division'
    AND rm.retail_mapping IN ('TD Portfolio', 'Savings Portfolio', 'Transaction Portfolio')
),
--1.3 Home Loan Offset
rm_rd_homeloans_offset AS (
  SELECT
    rm.fum,
    rm.bu,
    rm.division_name,
    rm.retail_mapping,
    rm.psgl_code,
    rm.cost_centre,
    rm.product_code,
    rm.sub_product_code,
    'Home Loans Offsets' AS product_group
  FROM reg_mapping AS rm
  WHERE 1 = 1
    AND rm.cost_centre NOT IN ('3023', '3798')
    AND rm.bu = 'HOME LOANS'
    AND rm.fum = 'Deposits and other borrowings'
    AND rm.division_name = 'Australia Retail Division'
),

/*2. Retail lending - rl */
--2.1 Home Loans
rm_rl_homeloans AS (
  SELECT
    rm.fum,
    rm.bu,
    rm.division_name,
    rm.retail_mapping,
    rm.psgl_code,
    rm.cost_centre,
    rm.product_code,
    rm.sub_product_code,
    rm.retail_mapping AS product_group -- Variable, Unproductive, Fixed, Equity
  FROM reg_mapping AS rm
  WHERE 1 = 1
    AND rm.bu = 'HOME LOANS'
    AND rm.fum = 'Net loans and advances'
    AND rm.division_name = 'Australia Retail Division'
),
--2.2 Consumer Cards
rm_rl_consumer_cards AS (

  SELECT
    rm.fum,
    rm.bu,
    rm.division_name,
    rm.retail_mapping,
    rm.psgl_code,
    rm.cost_centre,
    rm.product_code,
    rm.sub_product_code,
    rm.bu AS product_group -- Consumer Cards
  FROM reg_mapping AS rm
  WHERE 1 = 1
    AND bu = 'Consumer Cards'
    AND fum = 'Net loans and advances'
    AND division_name = 'Australia Retail Division'
),
--2.3 Personal Lending
rm_rl_personal_lending AS (
  SELECT
    rm.fum,
    rm.bu,
    rm.division_name,
    rm.retail_mapping,
    rm.psgl_code,
    rm.cost_centre,
    rm.product_code,
    rm.sub_product_code,
    rm.bu AS product_group -- Personal Lending
  FROM reg_mapping AS rm
  WHERE 1 = 1
    AND bu = 'Personal Lending'
    AND fum = 'Net loans and advances'
    AND division_name = 'Australia Retail Division'
),

/*3. Reg mapping union */
reg_mapping_union_bronze AS (
  SELECT * FROM rm_rd_tpd
  UNION ALL
  SELECT * FROM rm_rd_td_retail
  UNION ALL
  SELECT * FROM rm_rd_homeloans_offset
  UNION ALL
  SELECT * FROM rm_rl_homeloans
  UNION ALL
  SELECT * FROM rm_rl_consumer_cards
  UNION ALL
  SELECT * FROM rm_rl_personal_lending
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
      ELSE retail_mapping
    END AS portfolio
  FROM reg_mapping_union_bronze AS rmu
),

reg_mapping_union_gold AS (
  SELECT * EXCEPT (psgl_code) FROM reg_mapping_union_silver
),

reg_mapping_union AS (
  SELECT DISTINCT * FROM reg_mapping_union_gold
)

SELECT * FROM reg_mapping_union
