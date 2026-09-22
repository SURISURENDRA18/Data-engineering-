from datetime import datetime
from airflow.decorators import dag, task

@dag(
    dag_id="ganesh_hello_world",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["test", "ganesh"],
)
def hello_world_dag():
    @task
    def say_hello():
        print("Hello")

    say_hello()

hello_world_dag()
