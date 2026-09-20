"""
Task 7 — CDC: INSERT / UPDATE / DELETE

Applies staged CDC events (cdc_customers_staging) to a Doris target
(cdc_customers_target), logging every applied event to cdc_event_log for
idempotency.

Idempotency: cdc_event_log.event_id is UNIQUE KEY. Every apply statement
LEFT JOINs staging to the event log and only processes rows whose
event_id is NOT already logged. Re-running this DAG (or receiving the
same event twice, e.g. from an at-least-once CDC source) is therefore a
safe no-op for events already applied.

DELETE is implemented as a soft delete (is_deleted=1) rather than a
physical row removal, so "how many active customers" can be answered
with a WHERE clause instead of losing history of who was removed and
when.
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
    dag_id="ganesh_task7_cdc_apply",
    schedule=None,
    start_date=datetime(2026, 9, 18),
    catchup=False,
    tags=["de102", "task7", "cdc"],
)
def task7_cdc_apply():

    @task
    def apply_cdc_events() -> dict:
        conn = _doris_conn()
        counts = {"INSERT": 0, "UPDATE": 0, "DELETE": 0, "skipped_duplicates": 0}
        try:
            with conn.cursor() as cur:
                # How many staged events are already applied (would be skipped)?
                cur.execute("""
                    SELECT COUNT(*) FROM cdc_customers_staging s
                    JOIN cdc_event_log l ON l.event_id = s.event_id
                """)
                counts["skipped_duplicates"] = cur.fetchone()[0]

                cur.execute("""
                    INSERT INTO cdc_customers_target (customer_id, name, city, email, is_deleted, last_change_time, last_event_id)
                    SELECT s.customer_id, s.name, s.city, s.email, 0, s.change_time, s.event_id
                    FROM cdc_customers_staging s
                    LEFT JOIN cdc_event_log l ON l.event_id = s.event_id
                    WHERE s.operation = 'INSERT' AND l.event_id IS NULL
                """)
                counts["INSERT"] = cur.rowcount

                cur.execute("""
                    INSERT INTO cdc_customers_target (customer_id, name, city, email, is_deleted, last_change_time, last_event_id)
                    SELECT s.customer_id, s.name, s.city, s.email, 0, s.change_time, s.event_id
                    FROM cdc_customers_staging s
                    LEFT JOIN cdc_event_log l ON l.event_id = s.event_id
                    WHERE s.operation = 'UPDATE' AND l.event_id IS NULL
                """)
                counts["UPDATE"] = cur.rowcount

                cur.execute("""
                    INSERT INTO cdc_customers_target (customer_id, name, city, email, is_deleted, last_change_time, last_event_id)
                    SELECT s.customer_id, t.name, t.city, t.email, 1, s.change_time, s.event_id
                    FROM cdc_customers_staging s
                    JOIN cdc_customers_target t ON t.customer_id = s.customer_id
                    LEFT JOIN cdc_event_log l ON l.event_id = s.event_id
                    WHERE s.operation = 'DELETE' AND l.event_id IS NULL
                """)
                counts["DELETE"] = cur.rowcount

                cur.execute("""
                    INSERT INTO cdc_event_log (event_id, customer_id, operation, change_time, applied_at)
                    SELECT s.event_id, s.customer_id, s.operation, s.change_time, NOW()
                    FROM cdc_customers_staging s
                    LEFT JOIN cdc_event_log l ON l.event_id = s.event_id
                    WHERE l.event_id IS NULL
                """)
        finally:
            conn.close()
        print("CDC apply summary:", counts)
        return counts

    @task
    def verify_target_state() -> None:
        conn = _doris_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT customer_id, name, city, is_deleted FROM cdc_customers_target ORDER BY customer_id")
                rows = cur.fetchall()
                cur.execute("SELECT COUNT(*) FROM cdc_customers_target WHERE is_deleted = 0")
                active = cur.fetchone()[0]
        finally:
            conn.close()
        print("Final target state:", rows)
        print(f"Active customers: {active}")

    apply_cdc_events() >> verify_target_state()


task7_cdc_apply()
