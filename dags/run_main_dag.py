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
    description='Run main.py with Kafka host from secret',
    schedule_interval=timedelta(seconds=30)
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task to run the main script with the Kafka host from Airflow variables
    run_main_with_secret = BashOperator(
        task_id='run_main_script_with_secret',
        bash_command=(
            'pip install kafka-python && '
            'python /opt/airflow/dags/repo/scripts/main.py --kafka_host "{{ var.value.KAFKA_HOST }}"'
        )
    )