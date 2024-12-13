from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'run_main_dag',
    default_args=default_args,
    description='Run main.py with virtual environment and dependencies',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    run_main_with_venv = BashOperator(
        task_id='run_main_script_with_dependencies',
        bash_command='pip install kafka-python google-cloud-bigquery && python /opt/airflow/dags/repo/scripts/main.py --one',
    )