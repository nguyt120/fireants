-- ref: https://confluence.service.anz/display/ABT/Researching+Fum-Flow+logic
WITH
raw AS (
  SELECT 
    division_name,
    fum,
    bu,
    retail_mapping,
    commercial_mapping,
    psgl_product_code psgl_code,
    LEFT(CAST(profit_centre AS string),4) AS cost_center,
    IF(psgl_product_code="V2PLUS","DDA",deal_type) AS product_code,
    IF(psgl_product_code="V2PLUS","V2",deal_subtype) AS sub_product_code
  FROM  {{ ref("seed_reg_data_mapping") }}
),

--retail_deposit_td_portfolio
retail_deposit_td_portfolio_td_retail AS (
  SELECT 
    "Retail Deposits" AS v0,
    "TD Portfolio" AS portfolio,
    "Advance Notice TD - Retail" AS product_group,
    * EXCEPT(retail_mapping, commercial_mapping, psgl_code)
  FROM raw
  WHERE 1=1
    AND division_name = "Australia Retail Division"
    AND fum = "Deposits and other borrowings"
    AND retail_mapping = "TD Portfolio" 
    AND bu = "Banking Prod"
    AND psgl_code = "TD0003"
    AND cost_center NOT IN ("3023", "3798")
),
retail_deposit_td_portfolio_retail AS (
  SELECT 
    "Retail Deposits" AS v0,
    "TD Portfolio" AS portfolio,
    "Retail" AS product_group,
    * EXCEPT(retail_mapping, commercial_mapping, psgl_code)
  FROM raw
  WHERE 1=1
    AND division_name = "Australia Retail Division"
    AND fum = "Deposits and other borrowings"
    AND retail_mapping = "TD Portfolio" 
    AND bu = "Banking Prod"
    AND psgl_code = "TD0001"
    AND cost_center NOT IN ("3023", "3798")
),
retail_deposit_td_portfolio_tpd AS (
  SELECT 
    "Retail Deposits" AS v0,
    "TD Portfolio" AS portfolio,
    "Third Party Deposits" AS product_group,
    * EXCEPT(retail_mapping, commercial_mapping, psgl_code)
  FROM raw
  WHERE 1=1
    AND division_name = "Australia Retail Division"
    AND fum = "Deposits and other borrowings"
    AND retail_mapping = "TD Portfolio" 
    AND bu = "Banking Prod"
    AND psgl_code IN ("TD0001", "TD0003")
    AND cost_center IN ("3023", "3798")
),

--retail_deposit_savings_portfolio
retail_deposit_savings_portfolio AS (
  SELECT 
    "Retail Deposits" AS v0,
    "Savings Portfolio" AS portfolio,
    CASE 
      WHEN psgl_code = "DDASAZ" AND sub_product_code = "SA" THEN "Progress Saver"
      WHEN psgl_code = "ONLSAV" AND sub_product_code = "ED" THEN "Online Saver"
      WHEN psgl_code = "ONLSAV" AND sub_product_code IN ("XD|SAVING01", "XD|SAVING02") THEN "ANZ Plus Save"
      WHEN psgl_code = "V2PLUS" AND sub_product_code = "V2" THEN "V2 Plus"
      WHEN psgl_code = "CMA002" AND sub_product_code = "ET" THEN "ANZ Share Investing"
      WHEN psgl_code = "CMA002" AND sub_product_code = "EB" THEN "CMA"
      WHEN psgl_code = "SMSFCH"                             THEN "SMSF Cash Hub Account"
    END AS product_group,
    * EXCEPT(retail_mapping, commercial_mapping, psgl_code)
  FROM raw
  WHERE 1=1
    AND division_name = "Australia Retail Division"
    AND fum = "Deposits and other borrowings"
    AND retail_mapping = "Savings Portfolio" 
    AND bu = "Banking Prod"
    AND psgl_code IN ("DDASAZ", "ONLSAV", "V2PLUS", "CMA002", "SMSFCH")
),

-- Retail Deposits Transaction Portfolio
retail_deposit_trx_portfolio AS (
  SELECT 
    "Retail Deposits" AS v0,
    "Transaction Portfolio" AS portfolio,
    CASE 
      WHEN psgl_code = "ACC003" AND sub_product_code IN ("CX", "PT")    THEN "Access"
      WHEN psgl_code = "ACC003" AND sub_product_code = "XD|TRANSACT01"  THEN "ANZ Plus Transact"
      WHEN psgl_code IN ("ACC002", "HPP001")                            THEN "Pensioner Advantage"
      WHEN psgl_code = "RSVBAZ"                                         THEN "HPP"
      WHEN psgl_code IN ("CABI03", "CANBI2", "DDAS2Z")                  THEN "CABI"
    END AS product_group,
    * EXCEPT(retail_mapping, commercial_mapping, psgl_code)
  FROM raw
  WHERE 1=1
    AND division_name = "Australia Retail Division"
    AND fum = "Deposits and other borrowings"
    AND retail_mapping = "Transaction Portfolio" 
    AND bu = "Banking Prod"
    AND psgl_code IN ("ACC003", "ACC002", "HPP001", "RSVBAZ", "CABI03", "CANBI2", "DDAS2Z")
),

-- Retail Deposits - Home Loans Offsets
retail_deposit_hl_offsets AS (
  SELECT 
    "Retail Deposits" AS v0,
    "Home Loans Offsets" AS portfolio,
    "Home Loans Offsets" AS product_group,
    * EXCEPT(retail_mapping, commercial_mapping, psgl_code)
  FROM raw
  WHERE 1=1
    AND division_name = "Australia Retail Division"
    AND fum = "Deposits and other borrowings"
    AND bu = "HOME LOANS"
),

-- Commercial Deposits - Small to Medium Enterprise Banking
com_deposit_SMEB AS (
  SELECT 
    "Commercial Deposits" AS v0,
    "Small to Medium Enterprise Banking" AS portfolio,
    IF(commercial_mapping="TD","TDs",commercial_mapping) product_group,
    * EXCEPT(retail_mapping, commercial_mapping, psgl_code)
  FROM raw
  WHERE 1=1
    AND division_name = "Commercial Division"
    AND fum = "Deposits and other borrowings"
    AND bu = "Small to Medium Enterprise Banking"
    AND commercial_mapping IN ("CABI", "CANBI", "TD")
),

-- Commercial Deposits - Diversified & Specialist Industries
com_deposit_DSI AS (
  SELECT 
    "Commercial Deposits" AS v0,
    "Diversified & Specialist Industries" AS portfolio,
    IF(commercial_mapping="TD","TDs",commercial_mapping) product_group,
    * EXCEPT(retail_mapping, commercial_mapping, psgl_code)
  FROM raw
  WHERE 1=1
    AND division_name = "Commercial Division"
    AND fum = "Deposits and other borrowings"
    AND bu = "Diversified & Specialist Industries"
    AND commercial_mapping IN ("CABI", "CANBI", "TD")
),

-- Commercial Deposits - Private Bank and Advice
com_deposit_PBA AS (
  SELECT 
    "Commercial Deposits" AS v0,
    "Private Bank and Advice" AS portfolio,
    IF(commercial_mapping="TD","TDs",commercial_mapping) product_group,
    * EXCEPT(retail_mapping, commercial_mapping, psgl_code)
  FROM raw
  WHERE 1=1
    AND division_name = "Commercial Division"
    AND fum = "Deposits and other borrowings"
    AND bu = "Private Bank and Advice"
    AND commercial_mapping IN ("CABI", "CANBI", "TD")
),

-- Retail Lending - Home Loans
retail_lending_HL AS (
  SELECT 
    "Retail Lending" AS v0,
    "Home Loans" AS portfolio,
    retail_mapping AS product_group,
    * EXCEPT(retail_mapping, commercial_mapping, psgl_code)
  FROM raw
  WHERE 1=1
    AND division_name = "Australia Retail Division"
    AND fum = "Net loans and advances"
    AND bu = "HOME LOANS"
    AND retail_mapping IN ("Variable", "Fixed", "Equity", "Unproductive") 
),

-- Retail Lending - Cards and Payments
retail_lending_cards_payment AS (
  SELECT 
    "Retail Lending" AS v0,
    "Cards and Payments" AS portfolio,
    bu AS product_group,
    * EXCEPT(retail_mapping, commercial_mapping, psgl_code)
  FROM raw
  WHERE 1=1
    AND division_name = "Australia Retail Division"
    AND fum = "Net loans and advances"
    AND bu IN ("Personal Lending", "Consumer Cards")
),

-- Commercial Lending
com_lending AS (
  SELECT 
    "Commercial Lending" AS v0,
    "Commercial Lending" AS portfolio,
    bu AS product_group,
    * EXCEPT(retail_mapping, commercial_mapping, psgl_code)
  FROM raw
  WHERE 1=1
    AND division_name = "Commercial Division"
    AND fum = "Net loans and advances"
    AND commercial_mapping <> "N/A"
    AND bu IN ("Small to Medium Enterprise Banking", "Diversified & Specialist Industries", "Private Bank and Advice", "Asset Finance", "Asset Finance Dry")
),

-- Commercial Products
com_prod AS (
  SELECT 
    "Others" AS v0,
    "Commercial Products" AS portfolio,
    commercial_mapping AS product_group,
    * EXCEPT(retail_mapping, commercial_mapping, psgl_code)
  FROM raw
  WHERE 1=1
    AND division_name = "Commercial Division"
    AND fum = "Net loans and advances"
    AND bu IN ("Commercial", "Diversified & Specialist Industries", "Private Bank and Advice", "Asset Finance", "Asset Finance Dry")
    AND commercial_mapping IN ("Variable", "Fixed", "TCF", "Overdrafts", "Commercial Bills", "Commercial Cards", "Hire Purchase", "Lease")
),

-- Private Bank and Advice Products
PBAP AS (
  SELECT 
    "Others" AS v0,
    "Private Bank and Advice Products" AS portfolio,
    commercial_mapping AS product_group,
    * EXCEPT(retail_mapping, commercial_mapping, psgl_code)
  FROM raw
  WHERE 1=1
    AND division_name = "Commercial Division"
    AND fum = "Net loans and advances"
    AND bu IN ("Commercial", "Private Bank and Advice")
    AND commercial_mapping IN ("Variable", "Fixed", "TCF", "Overdrafts", "Commercial Bills", "Commercial Cards")
),

final_mapping As (
  SELECT * FROM retail_deposit_td_portfolio_td_retail
  UNION ALL
  SELECT * FROM retail_deposit_td_portfolio_retail
  UNION ALL
  SELECT * FROM retail_deposit_td_portfolio_tpd
  UNION ALL
  SELECT * FROM retail_deposit_savings_portfolio
  UNION ALL
  SELECT * FROM retail_deposit_trx_portfolio
  UNION ALL
  SELECT * FROM retail_deposit_hl_offsets
  UNION ALL
  SELECT * FROM com_deposit_SMEB
  UNION ALL
  SELECT * FROM com_deposit_DSI
  UNION ALL
  SELECT * FROM com_deposit_PBA
  UNION ALL
  SELECT * FROM retail_lending_HL
  UNION ALL
  SELECT * FROM retail_lending_cards_payment
  UNION ALL
  SELECT * FROM com_lending
  UNION ALL
  SELECT * FROM com_prod
  UNION ALL
  SELECT * FROM PBAP
)

SELECT DISTINCT * FROM final_mapping
ORDER BY v0, portfolio, product_group

