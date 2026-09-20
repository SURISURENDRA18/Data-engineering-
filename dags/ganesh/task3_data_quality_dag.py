"""
Task 3 — Data Quality

Reads staging tables (stg_customers/stg_orders, populated by Task 1's
load_staging) and applies the required checks. Design decision (see
sql/validation_queries.sql header for full rationale):

  - Row-level failures (null/dup ids, bad qty/price, bad dates, orphan FK)
    are ROUTED to dq_rejects and do not fail the DAG -- one bad row must
    not block the rest of a file.
  - CRITICAL failures (final curated tables end up empty) DO fail the
    Airflow task, because that means the whole pipeline produced nothing
    to analyze.

This DAG is intended to be run against a deliberately invalid file
(customers_invalid.csv / orders_invalid.csv) loaded into staging first,
to demonstrate the checks catching real violations.
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


REJECT_QUERIES = [
    (
        "NULL_CUSTOMER_ID",
        """
        SELECT 'stg_customers', CAST(customer_id AS VARCHAR(100)), 'NULL_CUSTOMER_ID',
               CONCAT_WS('|', CAST(customer_id AS VARCHAR(50)), customer_name, city, email, created_date), NOW()
        FROM stg_customers WHERE customer_id IS NULL
        """,
    ),
    (
        "DUPLICATE_CUSTOMER_ID",
        """
        SELECT 'stg_customers', CAST(customer_id AS VARCHAR(100)), 'DUPLICATE_CUSTOMER_ID',
               CONCAT_WS('|', CAST(customer_id AS VARCHAR(50)), customer_name, city, email, created_date), NOW()
        FROM stg_customers
        WHERE customer_id IN (SELECT customer_id FROM stg_customers GROUP BY customer_id HAVING COUNT(*) > 1)
        """,
    ),
    (
        "INVALID_CREATED_DATE",
        """
        SELECT 'stg_customers', CAST(customer_id AS VARCHAR(100)), 'INVALID_CREATED_DATE',
               CONCAT_WS('|', CAST(customer_id AS VARCHAR(50)), customer_name, city, email, created_date), NOW()
        FROM stg_customers
        WHERE created_date IS NOT NULL AND STR_TO_DATE(created_date, '%Y-%m-%d') IS NULL
        """,
    ),
    (
        "NULL_OR_MISSING_ORDER_ID",
        """
        SELECT 'stg_orders', order_id, 'NULL_OR_MISSING_ORDER_ID',
               CONCAT_WS('|', order_id, CAST(customer_id AS VARCHAR(50)), order_date, CAST(quantity AS VARCHAR(20)), CAST(unit_price AS VARCHAR(20))), NOW()
        FROM stg_orders WHERE order_id IS NULL OR order_id = '' OR NOT order_id REGEXP '^[0-9]+$'
        """,
    ),
    (
        "DUPLICATE_ORDER_ID",
        """
        SELECT 'stg_orders', order_id, 'DUPLICATE_ORDER_ID',
               CONCAT_WS('|', order_id, CAST(customer_id AS VARCHAR(50)), order_date, CAST(quantity AS VARCHAR(20)), CAST(unit_price AS VARCHAR(20))), NOW()
        FROM stg_orders
        WHERE order_id IN (SELECT order_id FROM stg_orders GROUP BY order_id HAVING COUNT(*) > 1)
        """,
    ),
    (
        "QUANTITY_NOT_POSITIVE",
        """
        SELECT 'stg_orders', order_id, 'QUANTITY_NOT_POSITIVE',
               CONCAT_WS('|', order_id, CAST(customer_id AS VARCHAR(50)), order_date, CAST(quantity AS VARCHAR(20)), CAST(unit_price AS VARCHAR(20))), NOW()
        FROM stg_orders WHERE order_id REGEXP '^[0-9]+$' AND (quantity IS NULL OR quantity <= 0)
        """,
    ),
    (
        "UNIT_PRICE_NEGATIVE",
        """
        SELECT 'stg_orders', order_id, 'UNIT_PRICE_NEGATIVE',
               CONCAT_WS('|', order_id, CAST(customer_id AS VARCHAR(50)), order_date, CAST(quantity AS VARCHAR(20)), CAST(unit_price AS VARCHAR(20))), NOW()
        FROM stg_orders WHERE order_id REGEXP '^[0-9]+$' AND (unit_price IS NULL OR unit_price < 0)
        """,
    ),
    (
        "INVALID_ORDER_DATE",
        """
        SELECT 'stg_orders', order_id, 'INVALID_ORDER_DATE',
               CONCAT_WS('|', order_id, CAST(customer_id AS VARCHAR(50)), order_date, CAST(quantity AS VARCHAR(20)), CAST(unit_price AS VARCHAR(20))), NOW()
        FROM stg_orders WHERE order_id REGEXP '^[0-9]+$' AND order_date IS NOT NULL AND STR_TO_DATE(order_date, '%Y-%m-%d') IS NULL
        """,
    ),
    (
        "ORPHAN_CUSTOMER_ID",
        """
        SELECT 'stg_orders', o.order_id, 'ORPHAN_CUSTOMER_ID',
               CONCAT_WS('|', o.order_id, CAST(o.customer_id AS VARCHAR(50)), o.order_date, CAST(o.quantity AS VARCHAR(20)), CAST(o.unit_price AS VARCHAR(20))), NOW()
        FROM stg_orders o
        LEFT JOIN stg_customers c ON c.customer_id = o.customer_id
        WHERE o.order_id REGEXP '^[0-9]+$' AND o.customer_id IS NOT NULL AND c.customer_id IS NULL
        """,
    ),
]


@dag(
    dag_id="ganesh_task3_data_quality",
    schedule=None,
    start_date=datetime(2026, 9, 18),
    catchup=False,
    tags=["de102", "task3", "data-quality"],
)
def task3_data_quality():

    @task
    def run_checks_and_route() -> dict:
        conn = _doris_conn()
        results = {}
        try:
            with conn.cursor() as cur:
                for rule_name, query in REJECT_QUERIES:
                    cur.execute(f"INSERT INTO dq_rejects (source_table, record_id, rule_violated, raw_payload, rejected_at) {query}")
                    cur.execute(
                        "SELECT COUNT(*) FROM dq_rejects WHERE rule_violated = %s "
                        "AND rejected_at >= NOW() - INTERVAL 1 MINUTE",
                        (rule_name,),
                    )
                    results[rule_name] = cur.fetchone()[0]
        finally:
            conn.close()
        print("Rejected row counts by rule:", results)
        return results

    @task
    def check_final_load_nonempty() -> None:
        conn = _doris_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM customers")
                customers_count = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM orders")
                orders_count = cur.fetchone()[0]
        finally:
            conn.close()
        print(f"customers={customers_count}, orders={orders_count}")
        if customers_count == 0 or orders_count == 0:
            raise ValueError(
                "CRITICAL: final curated tables are empty after data quality routing "
                f"(customers={customers_count}, orders={orders_count})."
            )

    run_checks_and_route() >> check_final_load_nonempty()


task3_data_quality()
