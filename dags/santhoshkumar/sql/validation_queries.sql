USE santhosh_retail_analytics;

-- 1. Monthly Sales
SELECT 
    DATE_FORMAT(order_date, '%Y-%m') AS sales_month,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(quantity) AS total_items_sold,
    SUM(total_amount) AS total_revenue
FROM fact_orders
GROUP BY DATE_FORMAT(order_date, '%Y-%m')
ORDER BY sales_month;

-- 2. Sales by City
SELECT 
    dc.city,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    SUM(fo.quantity) AS total_items_sold,
    SUM(fo.total_amount) AS total_revenue
FROM fact_orders fo
JOIN dim_customer dc 
  ON fo.customer_key = dc.customer_key
GROUP BY dc.city
ORDER BY total_revenue DESC;

-- 3. Sales by Customer
SELECT 
    dc.customer_id,
    dc.customer_name,
    dc.city,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    SUM(fo.total_amount) AS total_spend
FROM fact_orders fo
JOIN dim_customer dc 
  ON fo.customer_key = dc.customer_key
GROUP BY dc.customer_id, dc.customer_name, dc.city
ORDER BY total_spend DESC;