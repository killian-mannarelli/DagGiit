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
    description='Run main.py with dynamic Kafka host',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task to run the main script with the secret file
    run_main_with_secret = BashOperator(
        task_id='run_main_script_with_dependencies',
        bash_command=(
            'echo "{{ var.value.GCP_SERVICE_ACCOUNT }}" | base64 -d > /tmp/service-account.json && '
            'echo "Secret file created at /tmp/service-account.json" && '
            'pip install kafka-python && '
            'python /opt/airflow/dags/repo/scripts/main.py --kafka_host "{{ dag_run.conf["kafka_host"] }}"'
        )
    )