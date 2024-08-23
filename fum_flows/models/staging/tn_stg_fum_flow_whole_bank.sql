{{ config(materialized='table', tags=["daily"]) }}

-- day 1: 2024-08-01
SELECT DATE('2024-08-01') last_updated_date, DATE('2024-07-20') transaction_date, 'aaa' key_hash, 'John Doe' acc_name, '2000' amount UNION ALL
SELECT DATE('2024-08-01') last_updated_date, DATE('2024-07-21') transaction_date, 'bbb' key_hash, 'Jane Smith' acc_name, '3000' amount  UNION ALL
SELECT DATE('2024-08-01') last_updated_date, DATE('2024-08-01') transaction_date, 'ccc' key_hash, 'David Johnson' acc_name, '5001' amount

-- day 2: 2024-08-02 -- Insert these new records
-- SELECT DATE('2024-08-02') last_updated_date, DATE('2024-08-01') transaction_date, 'ddd' key_hash, 'Tam Nguyen' acc_name, '1000' amount UNION ALL
-- SELECT DATE('2024-08-02') last_updated_date, DATE('2024-08-01') transaction_date, 'zzz' key_hash, 'Duc Nguyen' acc_name, '1011' amount

-- day 3: 2024-08-03 -- Update ccc id
-- SELECT DATE('2024-08-03') last_updated_date, DATE('2024-08-01') transaction_date, 'ccc' key_hash, 'David Johnson' acc_name, '5000' amount

-- day 4: 2024-08-04 -- Update ddd id and insert news
-- SELECT DATE('2024-08-04') last_updated_date, DATE('2024-08-01') transaction_date, 'ddd' key_hash, 'Tam Nguyen' acc_name, '2000' amount UNION ALL
-- SELECT DATE('2024-08-04') last_updated_date, DATE('2024-08-04') transaction_date, 'kkk' key_hash, 'Khoi Nguyen' acc_name, '9000' amount


-- day 5: 2024-08-05
-- SELECT DATE('2024-08-05') last_updated_date, DATE('2024-08-01') transaction_date, 'ccc' key_hash, 'David Johnson' acc_name, '7000' amount UNION ALL
-- SELECT DATE('2024-08-05') last_updated_date, DATE('2024-07-20') transaction_date, 'aaa' key_hash, 'John Doe' acc_name, '2000' amount UNION ALL
-- SELECT DATE('2024-08-05') last_updated_date, DATE('2024-08-01') transaction_date, 'ddd' key_hash, 'Tam Nguyen' acc_name, '2000' amount UNION ALL
-- SELECT DATE('2024-08-05') last_updated_date, DATE('2024-07-20') transaction_date, 'aaa' key_hash, 'John Doe' acc_name, '2000' amount

-- day 5: 2024-08-06
-- SELECT DATE('2024-08-05') last_updated_date, DATE('2024-08-01') transaction_date, 'ccc' key_hash, 'David Johnson' acc_name, '7000' amount UNION ALL
-- SELECT DATE('2024-08-05') last_updated_date, DATE('2024-07-20') transaction_date, 'aaa' key_hash, 'John Doe' acc_name, '2000' amount UNION ALL
-- SELECT DATE('2024-08-05') last_updated_date, DATE('2024-08-01') transaction_date, 'ddd' key_hash, 'Tam Nguyen' acc_name, '2000' amount UNION ALL
-- SELECT DATE('2024-08-05') last_updated_date, DATE('2024-07-20') transaction_date, 'aaa' key_hash, 'John Doe' acc_name, '2000' amount