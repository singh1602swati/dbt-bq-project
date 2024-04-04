{{ config(
  materialized='table',
  unique_key='customer_id'
) }}
SELECT
  BuyerInfo_BuyerEmail AS customer_id,
  MIN(RequestStartDate) AS customer_acquisition_date,
  MAX(RequestEndDate) AS customer_last_order_date,
  COUNTIF(OrderStatus = 'Successful') AS successful_orders,
  COUNTIF(ShipmentServiceLevel = 'Standard') AS standard_shipment_orders,
  COUNTIF(ShipmentServiceLevel = 'Expedited') AS express_shipment_orders
FROM
  {{ ref('bq_dataset') }}
GROUP BY
  BuyerInfo_BuyerEmail;