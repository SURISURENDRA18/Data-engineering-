USE santhosh_retail_analytics;

-- Customer Dimension Table
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key BIGINT NOT NULL AUTO_INCREMENT,
    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(100),
    city VARCHAR(100),
    email VARCHAR(150),
    created_date DATE
)
UNIQUE KEY(customer_key)
DISTRIBUTED BY HASH(customer_key) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- Orders Fact Table
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id VARCHAR(50) NOT NULL,
    customer_key BIGINT NOT NULL,
    order_date DATE,
    quantity INT,
    unit_price DECIMAL(10, 2),
    total_amount DECIMAL(12, 2)
)
UNIQUE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 3
PROPERTIES ("replication_num" = "1");