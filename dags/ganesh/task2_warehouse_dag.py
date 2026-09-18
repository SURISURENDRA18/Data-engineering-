"""
Task 2 — Data Warehouse Design

Loads the dimensional model (dim_customer, fact_orders) from the curated
customers/orders tables populated by Task 1, then runs the three
required business queries and prints their results to the task log
(visible in Airflow UI and, for the actual result sets, also runnable
directly in DBeaver via sql/transformations.sql).
"""
from datetime import datetime

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

SCHEMA = "database_ganesh"


def _doris_conn():
    import pymysql
    conn = BaseHook.get_connection("doris_default")
    return pymysql.connect(
        host=conn.host,
        port=conn.port,
        user=conn.login,
        password=conn.password or "",
        database=SCHEMA,
        autocommit=True,
    )


@dag(
    dag_id="ganesh_task2_warehouse_load",
    schedule=None,
    start_date=datetime(2026, 9, 18),
    catchup=False,
    tags=["de102", "task2", "warehouse"],
)
def task2_warehouse_load():

    @task
    def load_dim_customer() -> None:
        conn = _doris_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dim_customer (customer_sk, customer_id, customer_name, city, email, created_date)
                    SELECT COALESCE(d.customer_sk, 0), c.customer_id, c.customer_name, c.city, c.email, c.created_date
                    FROM customers c
                    LEFT JOIN dim_customer d ON d.customer_id = c.customer_id
                """)
        finally:
            conn.close()

    @task
    def load_fact_orders() -> None:
        conn = _doris_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO fact_orders (order_id, customer_sk, order_date, quantity, unit_price, total_amount)
                    SELECT o.order_id, d.customer_sk, o.order_date, o.quantity, o.unit_price, o.total_amount
                    FROM orders o
                    LEFT JOIN dim_customer d ON d.customer_id = o.customer_id
                """)
        finally:
            conn.close()

    @task
    def run_business_queries() -> None:
        conn = _doris_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DATE_FORMAT(order_date, '%Y-%m') AS sales_month,
                           SUM(total_amount) AS total_sales, COUNT(*) AS order_count
                    FROM fact_orders GROUP BY DATE_FORMAT(order_date, '%Y-%m') ORDER BY sales_month
                """)
                print("Monthly sales:", cur.fetchall())

                cur.execute("""
                    SELECT d.city, SUM(f.total_amount) AS total_sales, COUNT(*) AS order_count
                    FROM fact_orders f JOIN dim_customer d ON d.customer_sk = f.customer_sk
                    GROUP BY d.city ORDER BY total_sales DESC
                """)
                print("Sales by city:", cur.fetchall())

                cur.execute("""
                    SELECT d.customer_id, d.customer_name, SUM(f.total_amount) AS total_sales, COUNT(*) AS order_count
                    FROM fact_orders f JOIN dim_customer d ON d.customer_sk = f.customer_sk
                    GROUP BY d.customer_id, d.customer_name ORDER BY total_sales DESC
                """)
                print("Sales by customer:", cur.fetchall())
        finally:
            conn.close()

    load_dim_customer() >> load_fact_orders() >> run_business_queries()


task2_warehouse_load()
