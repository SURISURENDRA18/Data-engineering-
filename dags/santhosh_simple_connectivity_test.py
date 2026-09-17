from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def test_connection():
    print("Santhosh Simple Connectivity Test")


with DAG(
    dag_id="santhosh_simple_connectivity_test",
    start_date=datetime(2026, 9, 17),
    schedule=None,
    catchup=False,
) as dag:

    test_task = PythonOperator(
        task_id="test_connection",
        python_callable=test_connection,
    )