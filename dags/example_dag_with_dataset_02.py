from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow import Dataset
from airflow.sensors.external_task import ExternalTaskSensor


# Define DAG details
default_args = {
    'owner': 'tanle',
}

@dag(default_args=default_args, schedule=[Dataset('/tmp/example_dag_trigger_dataset.txt')], start_date=days_ago(2), tags=['learn'], catchup=False)
def example_dag_with_dataset_02():
    @task()
    def start_task_dataset_02():
        print("Hello from start_task_dataset_02")


    start_task_dataset_02()


example_dag_with_dataset_02 = example_dag_with_dataset_02()