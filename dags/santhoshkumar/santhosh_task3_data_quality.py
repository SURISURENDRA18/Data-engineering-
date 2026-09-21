from datetime import datetime
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
import pymysql

SCHEMA = "santhosh_retail_analytics"
DORIS_CONN_ID = "santhosh_doris"

def _get_doris_connection():
    conn_info = BaseHook.get_connection(DORIS_CONN_ID)
    return pymysql.connect(
        host=conn_info.host,
        port=int(conn_info.port),
        user=conn_info.login,
        password=conn_info.password or "",
        database=SCHEMA,
        autocommit=True,
    )

REJECT_QUERIES = [
    (
        "NULL_CUSTOMER_ID",
        """
        SELECT 'stg_customers_bad', CAST(customer_id AS VARCHAR(100)), 'NULL_CUSTOMER_ID',
               CONCAT_WS('|', CAST(customer_id AS VARCHAR(50)), customer_name, city, email, created_date), NOW()
        FROM stg_customers_bad WHERE customer_id IS NULL OR customer_id = ''
        """,
    ),
    (
        "DUPLICATE_CUSTOMER_ID",
        """
        SELECT 'stg_customers_bad', CAST(customer_id AS VARCHAR(100)), 'DUPLICATE_CUSTOMER_ID',
               CONCAT_WS('|', CAST(customer_id AS VARCHAR(50)), customer_name, city, email, created_date), NOW()
        FROM stg_customers_bad
        WHERE customer_id IN (SELECT customer_id FROM stg_customers_bad WHERE customer_id IS NOT NULL GROUP BY customer_id HAVING COUNT(*) > 1)
        """,
    ),
    (
        "INVALID_CREATED_DATE",
        """
        SELECT 'stg_customers_bad', CAST(customer_id AS VARCHAR(100)), 'INVALID_CREATED_DATE',
               CONCAT_WS('|', CAST(customer_id AS VARCHAR(50)), customer_name, city, email, created_date), NOW()
        FROM stg_customers_bad
        WHERE created_date IS NOT NULL AND STR_TO_DATE(created_date, '%Y-%m-%d') IS NULL
        """,
    ),
    (
        "NULL_OR_MISSING_ORDER_ID",
        """
        SELECT 'stg_orders_bad', order_id, 'NULL_OR_MISSING_ORDER_ID',
               CONCAT_WS('|', order_id, CAST(customer_id AS VARCHAR(50)), order_date, CAST(quantity AS VARCHAR(20)), CAST(unit_price AS VARCHAR(20))), NOW()
        FROM stg_orders_bad WHERE order_id IS NULL OR order_id = '' OR NOT order_id REGEXP '^[0-9]+$'
        """,
    ),
    (
        "DUPLICATE_ORDER_ID",
        """
        SELECT 'stg_orders_bad', order_id, 'DUPLICATE_ORDER_ID',
               CONCAT_WS('|', order_id, CAST(customer_id AS VARCHAR(50)), order_date, CAST(quantity AS VARCHAR(20)), CAST(unit_price AS VARCHAR(20))), NOW()
        FROM stg_orders_bad
        WHERE order_id IN (SELECT order_id FROM stg_orders_bad WHERE order_id REGEXP '^[0-9]+$' GROUP BY order_id HAVING COUNT(*) > 1)
        """,
    ),
    (
        "QUANTITY_NOT_POSITIVE",
        """
        SELECT 'stg_orders_bad', order_id, 'QUANTITY_NOT_POSITIVE',
               CONCAT_WS('|', order_id, CAST(customer_id AS VARCHAR(50)), order_date, CAST(quantity AS VARCHAR(20)), CAST(unit_price AS VARCHAR(20))), NOW()
        FROM stg_orders_bad WHERE order_id REGEXP '^[0-9]+$' AND (quantity IS NULL OR quantity <= 0)
        """,
    ),
    (
        "UNIT_PRICE_NEGATIVE",
        """
        SELECT 'stg_orders_bad', order_id, 'UNIT_PRICE_NEGATIVE',
               CONCAT_WS('|', order_id, CAST(customer_id AS VARCHAR(50)), order_date, CAST(quantity AS VARCHAR(20)), CAST(unit_price AS VARCHAR(20))), NOW()
        FROM stg_orders_bad WHERE order_id REGEXP '^[0-9]+$' AND (unit_price IS NULL OR unit_price < 0)
        """,
    ),
    (
        "INVALID_ORDER_DATE",
        """
        SELECT 'stg_orders_bad', order_id, 'INVALID_ORDER_DATE',
               CONCAT_WS('|', order_id, CAST(customer_id AS VARCHAR(50)), order_date, CAST(quantity AS VARCHAR(20)), CAST(unit_price AS VARCHAR(20))), NOW()
        FROM stg_orders_bad WHERE order_id REGEXP '^[0-9]+$' AND order_date IS NOT NULL AND STR_TO_DATE(order_date, '%Y-%m-%d') IS NULL
        """,
    ),
    (
        "ORPHAN_CUSTOMER_ID",
        """
        SELECT 'stg_orders_bad', o.order_id, 'ORPHAN_CUSTOMER_ID',
               CONCAT_WS('|', o.order_id, CAST(o.customer_id AS VARCHAR(50)), o.order_date, CAST(o.quantity AS VARCHAR(20)), CAST(o.unit_price AS VARCHAR(20))), NOW()
        FROM stg_orders_bad o
        LEFT JOIN stg_customers_bad c ON c.customer_id = o.customer_id
        WHERE o.order_id REGEXP '^[0-9]+$' AND o.customer_id IS NOT NULL AND c.customer_id IS NULL
        """,
    ),
]

@dag(
    dag_id="santhosh_task3_data_quality",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["santhosh", "task3", "data-quality"],
)
def task3_data_quality():

    @task
    def route_invalid_records_to_quarantine() -> dict:
        conn = _get_doris_connection()
        summary = {}
        try:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE santhosh_dq_rejects;")
                for rule_label, query in REJECT_QUERIES:
                    cur.execute(f"""
                        INSERT INTO santhosh_dq_rejects (source_table, record_id, rule_violated, raw_payload, rejected_at)
                        {query}
                    """)
                    cur.execute(
                        "SELECT COUNT(*) FROM santhosh_dq_rejects WHERE rule_violated = %s",
                        (rule_label,),
                    )
                    count = cur.fetchone()[0]
                    summary[rule_label] = count
        finally:
            conn.close()

        print("=== QUARANTINE AUDIT SUMMARY ===")
        for rule, count in summary.items():
            print(f"Rule [{rule}]: {count} records quarantined")
        return summary

    @task
    def verify_curated_tables_nonempty() -> None:
        conn = _get_doris_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM dim_customer")
                cust_count = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM fact_orders")
                ord_count = cur.fetchone()[0]
        finally:
            conn.close()

        print(f"Curated check: dim_customer={cust_count}, fact_orders={ord_count}")
        if cust_count == 0 or ord_count == 0:
            raise ValueError(
                f"CRITICAL GATE FAILED: Curated tables empty (dim_customer={cust_count}, fact_orders={ord_count})"
            )

    route_invalid_records_to_quarantine() >> verify_curated_tables_nonempty()

task3_data_quality()