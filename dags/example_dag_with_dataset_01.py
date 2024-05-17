import json
import time

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow import Dataset

default_args = {
    'owner': 'tanle'
}

@dag(default_args=default_args, schedule_interval="@daily", start_date=days_ago(2), tags=['learn'], catchup=False)
def example_dag_with_dataset_01():

    @task()
    def example_dag_with_dataset_01():
        print("Hello from example_dag_with_dataset_01")
        time.sleep(5)

    @task(outlets=Dataset('/tmp/example_dag_trigger_dataset.txt'))
    def trigger_example_dag_with_dataset():
        return True

    start_process = example_dag_with_dataset_01()
    trigger_example_dag_dataset = trigger_example_dag_with_dataset()
    start_process >> trigger_example_dag_dataset


example_dag_with_dataset_01 = example_dag_with_dataset_01()
