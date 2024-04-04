-- customer.sql
-- first model created for the use case and the name is customer
SELECT
  BuyerInfo_BuyerEmail AS customer_id,
  MIN(RequestStartDate) AS customer_acquisition_date,
  MAX(RequestStartDate) AS customer_last_order_date,
  COUNTIF(OrderStatus = 'Shipped') AS successful_orders,
  COUNTIF(ShipServiceLevel = 'Standard') AS standard_shipment_orders,
  COUNTIF(ShipServiceLevel = 'Express') AS express_shipment_orders
FROM
  {{ ref('viewmodel') }}
GROUP BY
  BuyerInfo_BuyerEmail
