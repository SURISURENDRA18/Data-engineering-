from datetime import datetime
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
import pymysql

SCHEMA = "santhosh_retail_analytics"


def get_doris_conn():
    conn_info = BaseHook.get_connection("santhosh_doris")
    conn = pymysql.connect(
        host=conn_info.host,
        port=int(conn_info.port),
        user=conn_info.login,
        password=conn_info.password or "",
        autocommit=True,
    )
    with conn.cursor() as cur:
        cur.execute(f"USE `{SCHEMA}`;")
    return conn


@dag(
    dag_id="santhosh_task2_warehouse_load",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["santhosh", "task2", "warehouse"],
)
def task2_warehouse_dag():

    @task
    def load_dim_customer():
        conn = get_doris_conn()
        try:
            with conn.cursor() as cur:
                # 1. Ensure table exists
                cur.execute("""
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
                """)
                # 2. Insert distinct customers from staging
                insert_sql = """
                    INSERT INTO dim_customer (customer_id, customer_name, city, email, created_date)
                    SELECT DISTINCT
                        customer_id,
                        customer_name,
                        city,
                        email,
                        CAST(created_date AS DATE)
                    FROM stg_customers;
                """
                cur.execute(insert_sql)
                print("Successfully loaded dim_customer.")
        finally:
            conn.close()

    @task
    def load_fact_orders():
        conn = get_doris_conn()
        try:
            with conn.cursor() as cur:
                # 1. Ensure table exists
                cur.execute("""
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
                """)
                # 2. Insert orders joined with dim_customer for surrogate key
                insert_sql = """
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
                """
                cur.execute(insert_sql)
                print("Successfully loaded fact_orders.")
        finally:
            conn.close()

    @task
    def run_business_queries():
        conn = get_doris_conn()
        try:
            with conn.cursor() as cur:
                print("=== 1. Monthly Sales ===")
                cur.execute("""
                    SELECT 
                        DATE_FORMAT(order_date, '%Y-%m') AS sales_month,
                        COUNT(DISTINCT order_id) AS total_orders,
                        SUM(quantity) AS total_items,
                        SUM(total_amount) AS revenue
                    FROM fact_orders
                    GROUP BY DATE_FORMAT(order_date, '%Y-%m')
                    ORDER BY sales_month;
                """)
                for row in cur.fetchall():
                    print(row)

                print("\n=== 2. Sales by City ===")
                cur.execute("""
                    SELECT 
                        dc.city,
                        COUNT(DISTINCT fo.order_id) AS total_orders,
                        SUM(fo.total_amount) AS revenue
                    FROM fact_orders fo
                    JOIN dim_customer dc ON fo.customer_key = dc.customer_key
                    GROUP BY dc.city
                    ORDER BY revenue DESC;
                """)
                for row in cur.fetchall():
                    print(row)

                print("\n=== 3. Sales by Customer ===")
                cur.execute("""
                    SELECT 
                        dc.customer_id,
                        dc.customer_name,
                        SUM(fo.total_amount) AS spend
                    FROM fact_orders fo
                    JOIN dim_customer dc ON fo.customer_key = dc.customer_key
                    GROUP BY dc.customer_id, dc.customer_name
                    ORDER BY spend DESC;
                """)
                for row in cur.fetchall():
                    print(row)
        finally:
            conn.close()

    # Step sequence
    load_dim_customer() >> load_fact_orders() >> run_business_queries()


task2_warehouse_dag()