WITH churn_ressurect_active_users AS (
  WITH sales_by_order AS (
    select  
      order_date order_date,
      order_id order_id,
      week_of_month,
      customer_id,
      employee_id,
      lag(order_date) over(partition by customer_id order by order_date ASC) last_order_date,
      ROW_NUMBER() over(partition by customer_id order by order_date ASC) count_orders,
      FIRST_VALUE(order_date IGNORE NULLS) over(partition by customer_id order by order_date ASC) first_value,
      1 orders_count,
      SUM(sales) sales,
    from `indicium-349201.Northwind.all_orders_details`
    group by 1,2,3,4,5
  )
    , date_range AS (
        SELECT date, 
        FROM 
          UNNEST(
            (
              SELECT GENERATE_DATE_ARRAY(MIN(sbo.order_date), MAX(sbo.order_date)) date 
              FROM sales_by_order sbo
            )
          ) date
    )
    , customers_by_orders AS (
      select 
        ad.date,
        ci.all_customer_id,
        ci.first_value,
        sbo.order_date,
        sbo.last_order_date,
        SUM(orders_count) OVER(partition BY ci.all_customer_id ORDER BY ad.date ASC) sum_by_orders_customers,
        CONCAT(ci.all_customer_id, SUM(orders_count) OVER(partition BY ci.all_customer_id ORDER BY ad.date ASC)) customer_concat,
        sbo.customer_id,
        sbo.order_id,
        sbo.week_of_month,
        sbo.employee_id,
        sbo.sales,
      from 
        date_range ad
      cross join (select distinct customer_id all_customer_id, first_value from sales_by_order) ci
      left join sales_by_order sbo ON sbo.order_date = ad.date AND sbo.customer_id = ci.all_customer_id
    )

  SELECT 
    cbo.*,
    IF(cbo.first_value <= cbo.date, ROW_NUMBER() OVER(PARTITION BY cbo.customer_concat ORDER BY cbo.date ASC), null) count_days_until_purchase,
    CASE 
      WHEN IF(cbo.first_value <= cbo.date, ROW_NUMBER() OVER(PARTITION BY cbo.customer_concat ORDER BY cbo.date ASC), null) >= @churn_days THEN "Churn"
      ELSE 'Active'
    END user_churn_active,
  FROM customers_by_orders cbo
)
SELECT 
  crau.*,
  IF(date = first_value, 1, 0) count_converted,
  LAG(user_churn_active) OVER(PARTITION BY customer_id ORDER BY date ASC) last_user_churn_active,
  CASE 
    WHEN user_churn_active = 'Churn' THEN 'Churn'
    WHEN LAG(user_churn_active) OVER(PARTITION BY customer_id ORDER BY date ASC) = 'Churn' AND user_churn_active = 'Active' THEN 'Ressurect'
    ELSE 'Active'
  END user_status,
  IF(
    CASE 
      WHEN user_churn_active = 'Churn' THEN 'Churn'
      WHEN LAG(user_churn_active) OVER(PARTITION BY customer_id ORDER BY date ASC) = 'Churn' AND user_churn_active = 'Active' THEN 'Ressurect'
      ELSE 'Active'
    END = 'Churn'  AND customer_concat is not null, all_customer_id, null
  ) churn_user,
  IF(
    CASE 
      WHEN user_churn_active = 'Churn' THEN 'Churn'
      WHEN LAG(user_churn_active) OVER(PARTITION BY customer_id ORDER BY date ASC) = 'Churn' AND user_churn_active = 'Active' THEN 'Ressurect'
      ELSE 'Active'
    END = 'Ressurect'  AND customer_concat is not null, all_customer_id, null
  ) ressurect_user,
  IF(
    CASE 
      WHEN user_churn_active = 'Churn' THEN 'Churn'
      WHEN LAG(user_churn_active) OVER(PARTITION BY customer_id ORDER BY date ASC) = 'Churn' AND user_churn_active = 'Active' THEN 'Ressurect'
      ELSE 'Active'
    END = 'Active'  AND customer_concat is not null, all_customer_id, null
  ) active_user,
FROM churn_ressurect_active_users crau
