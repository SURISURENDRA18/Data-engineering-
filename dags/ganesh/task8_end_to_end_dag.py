"""
Task 8 — End-to-End Mini Project

Orchestrates the full pipeline in one DAG using TriggerDagRunOperator to
call the task-specific DAGs already built for Tasks 1, 2, 3, 6 and 7, in
the order the assignment's flow diagram specifies:

  MinIO raw -> Airflow -> Staging -> Transform/Apply changes ->
  Doris warehouse/lakehouse -> Data quality + analytics

Kept as separate triggered DAGs (rather than one giant flat DAG) so each
task's logic stays independently testable and re-runnable, matching how
they were built and verified one at a time; this DAG is the thin
orchestration layer proving they compose into one pipeline.

wait_for_completion=True on each trigger means a failure in an earlier
stage stops the chain — e.g. if Task 1's validate step fails (no records
loaded), Task 2's warehouse load never runs against empty tables.
"""
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
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
    dag_id="ganesh_task8_end_to_end",
    schedule=None,
    start_date=datetime(2026, 9, 18),
    catchup=False,
    tags=["de102", "task8", "end-to-end"],
)
def task8_end_to_end():

    incremental_load = TriggerDagRunOperator(
        task_id="incremental_load",
        trigger_dag_id="ganesh_task6_incremental_etl",
        wait_for_completion=True,
        poke_interval=5,
    )

    data_quality = TriggerDagRunOperator(
        task_id="data_quality_checks",
        trigger_dag_id="ganesh_task3_data_quality",
        wait_for_completion=True,
        poke_interval=5,
    )

    warehouse_load = TriggerDagRunOperator(
        task_id="warehouse_load",
        trigger_dag_id="ganesh_task2_warehouse_load",
        wait_for_completion=True,
        poke_interval=5,
    )

    cdc_apply = TriggerDagRunOperator(
        task_id="cdc_apply",
        trigger_dag_id="ganesh_task7_cdc_apply",
        wait_for_completion=True,
        poke_interval=5,
    )

    @task
    def answer_business_questions() -> dict:
        conn = _doris_conn()
        answers = {}
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DATE_FORMAT(order_date, '%Y-%m'), SUM(total_amount)
                    FROM fact_orders GROUP BY DATE_FORMAT(order_date, '%Y-%m') ORDER BY 1
                """)
                answers["total_sales_by_month"] = cur.fetchall()

                cur.execute("""
                    SELECT d.city, SUM(f.total_amount)
                    FROM fact_orders f JOIN dim_customer d ON d.customer_sk = f.customer_sk
                    GROUP BY d.city ORDER BY 2 DESC
                """)
                answers["total_sales_by_city"] = cur.fetchall()

                cur.execute("SELECT COUNT(*) FROM cdc_customers_target WHERE is_deleted = 0")
                answers["active_customers_after_cdc"] = cur.fetchone()[0]

                cur.execute("SELECT operation, COUNT(*) FROM cdc_event_log GROUP BY operation")
                answers["cdc_events_by_type"] = cur.fetchall()

                cur.execute("SELECT file_name, status FROM ingestion_tracker ORDER BY processed_at")
                answers["files_processed"] = cur.fetchall()

                cur.execute("SELECT rule_violated, COUNT(*) FROM dq_rejects GROUP BY rule_violated")
                answers["dq_rejects_by_rule"] = cur.fetchall()
        finally:
            conn.close()

        for question, result in answers.items():
            print(f"{question}: {result}")
        return answers

    incremental_load >> data_quality >> warehouse_load >> cdc_apply >> answer_business_questions()


task8_end_to_end()
