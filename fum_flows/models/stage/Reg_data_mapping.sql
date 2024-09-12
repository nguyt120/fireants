WITH
reg_mapping as (
  select 
    FUM as fum,
    BU as bu,
    DIVISION_NAME as division_name,
    Retail_Mapping,
    PSGL_PRODUCT_CODE as psgl_code,
    LEFT(cast(PROFIT_CENTRE as string),4) as cost_centre,
    CASE
      WHEN DEAL_SUBTYPE LIKE '%V2%' THEN 'DDA'
      ELSE DEAL_TYPE
    END AS product_code,
    CASE
      WHEN DEAL_SUBTYPE LIKE '%V2%' THEN 'V2'
      ELSE DEAL_SUBTYPE
    END AS sub_product_code
  from ref{{"reg_data_mapping"}}
),

reg_mapping_TPD as (
  select 
    *,
    'Third Party Deposits' as product_group
  from reg_mapping
  where cost_centre in ('3023','3798')
  and BU = 'Banking Prod'
  and FUM = 'Deposits and other borrowings'
  and division_name = 'Australia Retail Division'
  and Retail_Mapping in ('TD Portfolio')
),

reg_mapping_homeloan as (
  select 
    * EXCEPT(bu),
    'Home Loans Offsets' as bu,
    'Home Loans Offsets' as product_group
  from reg_mapping
  where cost_centre not in ('3023','3798')
  and BU = 'HOME LOANS'
  and FUM = 'Deposits and other borrowings'
  and division_name = 'Australia Retail Division'
),

reg_mapping_retail_dep as (
  select 
    *,
    CASE
      WHEN 
      psgl_code = 'TD0001' and sub_product_code in ('AA', 'AB', 'AC', 'AD', 'AE', 'AG', 'AO', 'AR', 'AU') 
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
  from reg_mapping
  where cost_centre not in ('3023','3798')
  and BU = 'Banking Prod'
  and FUM = 'Deposits and other borrowings'
  and division_name = 'Australia Retail Division'
  and Retail_Mapping in ('TD Portfolio','Savings Portfolio','Transaction Portfolio')
),

reg_mapping_rt_lending_HL as (
  select
    *,
    Retail_Mapping as product_group
  from reg_mapping
  where 1=1
  and BU = 'HOME LOANS'
  and FUM = 'Net loans and advances'
  and division_name = 'Australia Retail Division'
),
/* adding retail lending - Consumer Cards
Consumer Cards  4.821
*/
reg_mapping_rt_lending_CC as(
  select *, BU  as product_group
  from reg_mapping
  where 1=1
  and BU = 'Consumer Cards'
  and FUM = 'Net loans and advances'
  and division_name = 'Australia Retail Division'
),
/* adding retail lending -  Personal Lending
*/
reg_mapping_rt_lending_PL as(
  select *, BU  as product_group
  from reg_mapping
    where 1=1
    and BU = 'Personal Lending'
    and FUM = 'Net loans and advances'
    and division_name = 'Australia Retail Division'
),
reg_mapping_union_broze AS (
  SELECT * FROM reg_mapping_TPD
  UNION ALL
  SELECT * FROM reg_mapping_homeloan
  UNION ALL
  SELECT * FROM reg_mapping_retail_dep
  UNION ALL
  SELECT * FROM reg_mapping_rt_lending_HL
  UNION ALL
  SELECT * FROM reg_mapping_rt_lending_CC
  UNION ALL
  SELECT * FROM reg_mapping_rt_lending_PL
),

reg_mapping_union_silver AS (
  SELECT 
    * EXCEPT(Retail_Mapping),
    CASE
      WHEN bu IN ('Consumer Cards', 'HOME LOANS', 'Personal Lending', 'Home Loans Offsets') THEN bu
      ELSE Retail_Mapping
    END AS Retail_Mapping
  FROM reg_mapping_union_broze
),

reg_mapping_union AS (
  SELECT DISTINCT * from reg_mapping_union_silver
)

Select * from reg_mapping_union;