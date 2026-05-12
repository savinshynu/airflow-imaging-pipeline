from airflow import DAG
from airflow.decorators import task
from datetime import datetime


with DAG(
    dag_id="test_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,   # manual trigger
    catchup=False,
    tags=["test"]
) as dag:

    @task
    def step1():
        print("Step 1 running")

    @task
    def step2():
        print("Step 2 running")

    step1() >> step2()
