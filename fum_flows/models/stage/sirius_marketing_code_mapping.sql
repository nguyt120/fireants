SELECT
  distinct 
  Account_Number as account_number,
  Marketing_Code as marketing_code
FROM {{source("sirius_reporting", "ACCOUNT_CURRENT_VW")}}