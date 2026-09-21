from datetime import datetime

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

SCHEMA = "santhosh_retail_analytics"
DORIS_CONN_ID = "santhosh_doris"


def _doris_conn():
    import pymysql

    conn = BaseHook.get_connection(DORIS_CONN_ID)
    return pymysql.connect(
        host=conn.host,
        port=int(conn.port),
        user=conn.login,
        password=conn.password or "",
        database=SCHEMA,
        autocommit=True,
    )


@dag(
    dag_id="santhosh_task7_cdc_apply",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["santhosh", "task7", "cdc"],
)
def task7_cdc_apply():

    @task
    def setup_tables_and_seed():
        """Creates target, staging, and event log tables and seeds baseline data."""
        conn = _doris_conn()
        try:
            with conn.cursor() as cur:
                # 1. Target table
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS santhosh_cdc_customers_target (
                        customer_id BIGINT,
                        name VARCHAR(100),
                        city VARCHAR(100),
                        email VARCHAR(100),
                        is_deleted TINYINT,
                        last_change_time DATETIME,
                        last_event_id VARCHAR(50)
                    )
                    UNIQUE KEY(customer_id)
                    DISTRIBUTED BY HASH(customer_id) BUCKETS 1;
                """
                )

                # 2. Staging table
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS santhosh_cdc_customers_staging (
                        event_id VARCHAR(50),
                        customer_id BIGINT,
                        name VARCHAR(100),
                        city VARCHAR(100),
                        email VARCHAR(100),
                        operation VARCHAR(20),
                        change_time DATETIME
                    )
                    DUPLICATE KEY(event_id)
                    DISTRIBUTED BY HASH(event_id) BUCKETS 1;
                """
                )

                # 3. Event log for idempotency
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS santhosh_cdc_event_log (
                        event_id VARCHAR(50),
                        customer_id BIGINT,
                        operation VARCHAR(20),
                        change_time DATETIME,
                        applied_at DATETIME
                    )
                    UNIQUE KEY(event_id)
                    DISTRIBUTED BY HASH(event_id) BUCKETS 1;
                """
                )

                # Seed baseline target data if empty
                cur.execute("SELECT COUNT(*) FROM santhosh_cdc_customers_target")
                if cur.fetchone()[0] == 0:
                    cur.execute(
                        """
                        INSERT INTO santhosh_cdc_customers_target (customer_id, name, city, email, is_deleted, last_change_time, last_event_id) VALUES
                        (1, 'John', 'Mumbai', 'john@example.com', 0, NOW(), 'INIT_01'),
                        (2, 'Mary', 'Delhi', 'mary@example.com', 0, NOW(), 'INIT_02'),
                        (3, 'Peter', 'Bangalore', 'peter@example.com', 0, NOW(), 'INIT_03')
                    """
                    )

                # Seed staging change set if empty
                cur.execute("SELECT COUNT(*) FROM santhosh_cdc_customers_staging")
                if cur.fetchone()[0] == 0:
                    cur.execute(
                        """
                        INSERT INTO santhosh_cdc_customers_staging (event_id, customer_id, name, city, email, operation, change_time) VALUES
                        ('EVT_001', 4, 'Alice', 'Chennai', 'alice@example.com', 'INSERT', NOW()),
                        ('EVT_002', 2, 'Mary', 'Kolkata', 'mary@example.com', 'UPDATE', NOW()),
                        ('EVT_003', 3, 'Peter', 'Bangalore', 'peter@example.com', 'DELETE', NOW())
                    """
                    )
        finally:
            conn.close()

    @task
    def apply_cdc_events() -> dict:
        conn = _doris_conn()
        counts = {"INSERT": 0, "UPDATE": 0, "DELETE": 0, "skipped_duplicates": 0}
        try:
            with conn.cursor() as cur:
                # Count already processed events
                cur.execute(
                    """
                    SELECT COUNT(*) 
                    FROM santhosh_cdc_customers_staging s
                    JOIN santhosh_cdc_event_log l ON l.event_id = s.event_id
                """
                )
                counts["skipped_duplicates"] = cur.fetchone()[0]

                # Apply INSERT
                cur.execute(
                    """
                    INSERT INTO santhosh_cdc_customers_target (customer_id, name, city, email, is_deleted, last_change_time, last_event_id)
                    SELECT s.customer_id, s.name, s.city, s.email, 0, s.change_time, s.event_id
                    FROM santhosh_cdc_customers_staging s
                    LEFT JOIN santhosh_cdc_event_log l ON l.event_id = s.event_id
                    WHERE s.operation = 'INSERT' AND l.event_id IS NULL
                """
                )
                counts["INSERT"] = cur.rowcount

                # Apply UPDATE
                cur.execute(
                    """
                    INSERT INTO santhosh_cdc_customers_target (customer_id, name, city, email, is_deleted, last_change_time, last_event_id)
                    SELECT s.customer_id, s.name, s.city, s.email, 0, s.change_time, s.event_id
                    FROM santhosh_cdc_customers_staging s
                    LEFT JOIN santhosh_cdc_event_log l ON l.event_id = s.event_id
                    WHERE s.operation = 'UPDATE' AND l.event_id IS NULL
                """
                )
                counts["UPDATE"] = cur.rowcount

                # Apply DELETE (soft delete)
                cur.execute(
                    """
                    INSERT INTO santhosh_cdc_customers_target (customer_id, name, city, email, is_deleted, last_change_time, last_event_id)
                    SELECT s.customer_id, t.name, t.city, t.email, 1, s.change_time, s.event_id
                    FROM santhosh_cdc_customers_staging s
                    JOIN santhosh_cdc_customers_target t ON t.customer_id = s.customer_id
                    LEFT JOIN santhosh_cdc_event_log l ON l.event_id = s.event_id
                    WHERE s.operation = 'DELETE' AND l.event_id IS NULL
                """
                )
                counts["DELETE"] = cur.rowcount

                # Record processed events into event log
                cur.execute(
                    """
                    INSERT INTO santhosh_cdc_event_log (event_id, customer_id, operation, change_time, applied_at)
                    SELECT s.event_id, s.customer_id, s.operation, s.change_time, NOW()
                    FROM santhosh_cdc_customers_staging s
                    LEFT JOIN santhosh_cdc_event_log l ON l.event_id = s.event_id
                    WHERE l.event_id IS NULL
                """
                )
        finally:
            conn.close()

        print("CDC apply summary:", counts)
        return counts

    @task
    def verify_target_state() -> None:
        conn = _doris_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT customer_id, name, city, is_deleted FROM santhosh_cdc_customers_target ORDER BY customer_id"
                )
                rows = cur.fetchall()
                cur.execute(
                    "SELECT COUNT(*) FROM santhosh_cdc_customers_target WHERE is_deleted = 0"
                )
                active = cur.fetchone()[0]
        finally:
            conn.close()
        print("Final target state:", rows)
        print(f"Active customers: {active}")

    setup_tables_and_seed() >> apply_cdc_events() >> verify_target_state()


task7_cdc_apply()