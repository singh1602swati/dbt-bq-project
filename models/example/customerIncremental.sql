-- customer_orders_incremental.sql
{{ config(
  materialized='incremental',
  unique_key='customer_id'
) }}

WITH latest_orders AS (
  SELECT
    BuyerInfo_BuyerEmail AS customer_id,
    MIN(RequestStartDate) AS customer_acquisition_date,
    MAX(RequestStartDate) AS customer_last_order_date,
    COUNTIF(OrderStatus = 'Successful') AS successful_orders,
    COUNTIF(ShipServiceLevel = 'Standard') AS standard_shipment_orders,
    COUNTIF(ShipServiceLevel = 'Express') AS express_shipment_orders
  FROM
    {{ ref('viewmodel') }}
  GROUP BY
    BuyerInfo_BuyerEmail
)
SELECT
  CONCAT('customer_', TO_HEX(MD5(customer_id))) AS customer_key,
  customer_id,
  customer_acquisition_date,
  customer_last_order_date,
  successful_orders,
  standard_shipment_orders,
  express_shipment_orders
FROM
  latest_orders
  WHERE
customer_acquisition_date > (SELECT MAX(customer_last_order_date) FROM {{ ref('customer') }})
