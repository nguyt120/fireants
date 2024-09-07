{{
    config(
        materialized="view"
    )
}}

SELECT 'aaa' row_key, 'aaa' hash_diff, DATE('2024-01-01') transaction_date, 'John Doe' name, 'john.doe@gmail.com' email, '123 Fake Street, Sydney, NSW, 2000' address UNION ALL
SELECT 'bbb' row_key, 'bbb' hash_diff, DATE('2024-01-01') transaction_date, 'Jane Smith' name, 'jane.smith@gmail.com' email, '123 Fake Street, Melbourne, VIC, 3000' address  UNION ALL
SELECT 'ccc' row_key, 'ccc' hash_diff, DATE('2024-01-01') transaction_date, 'David Johnson' name, 'david.johnson@gmail.com' email, '123 Fake Street, Adelaide, SA, 5000' address
