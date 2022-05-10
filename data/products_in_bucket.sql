select
  aod.order_date,
  aod.order_id,
  aod.product_id,
  aod.product_name,
  IF(row_number() over(partition by aod.order_id, aod.product_name)=1, aod.unit_price, null) unit_price,
  IF(row_number() over(partition by aod.order_id, aod.product_name)=1, aod.quantity, null) quantity,
  IF(row_number() over(partition by aod.order_id, aod.product_name)=1, aod.sales, null) sales,
  aod2.product_name product_name_in_bucket,
  aod2.unit_price unit_price_in_bucket,
  aod2.quantity quantity_in_bucket,
  aod2.sales sales_in_bucket,
  
  1 count_products_in_bucket,
from `indicium-349201.Northwind.all_orders_details` aod
left join `indicium-349201.Northwind.all_orders_details` aod2 ON aod2.order_id = aod.order_id
where 1=1
and aod.product_name != aod2.product_name