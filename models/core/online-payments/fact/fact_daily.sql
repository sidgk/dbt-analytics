{{config(
        tags = ["2hourly"],
        materialized='table'
       )
}}

SELECT 
  DATE(transaction_happened_at) AS transaction_happend_at_date,
  COUNT(*) AS total_transactions,
  COUNTIF(status = 'accepted') AS accepted_transactions,
  COUNTIF(status = 'refused') AS refused_transactions,
  COUNTIF(status = 'cancelled') AS cancelled_transactions,
  SUM(amount ) AS transaction_amount,
  COUNT(distinct CASE WHEN status = 'accepted' then product_name end) AS total_product_sold
FROM 
  {{ ref('dim_transactions') }}
GROUP BY 
  transaction_happend_at_date
ORDER BY 1