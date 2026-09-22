USE santhosh_retail_analytics;

-- 1. Populate Dimension Table
INSERT INTO dim_customer (customer_id, customer_name, city, email, created_date)
SELECT DISTINCT
    customer_id,
    customer_name,
    city,
    email,
    CAST(created_date AS DATE)
FROM stg_customers;

-- 2. Populate Fact Table (look up surrogate key)
INSERT INTO fact_orders (order_id, customer_key, order_date, quantity, unit_price, total_amount)
SELECT 
    ot.order_id,
    dc.customer_key,
    CAST(ot.order_date AS DATE),
    ot.quantity,
    ot.unit_price,
    ot.total_amount
FROM orders_transformed ot
JOIN dim_customer dc 
  ON ot.customer_id = dc.customer_id;