{{ config(materialized="view") }}

SELECT * FROM {{ ref("df_transaction") }}
UNION ALL
SELECT * FROM {{ ref("td_transaction") }}
UNION ALL
SELECT * FROM {{ ref("consumer_card_transaction") }}
