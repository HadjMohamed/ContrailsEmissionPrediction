from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='test_hello_world',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=["test"]
) as dag:

    @task
    def say_hello():
        print("Hello from Airflow 3!")

    say_hello()
