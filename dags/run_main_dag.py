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
    description='Run main.py with virtual environment and GCP secret file',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task to write the GCP service account secret to a file
    create_secret_file = BashOperator(
        task_id='create_service_account_file',
        bash_command=(
            'echo "{{ var.value.GCP_SERVICE_ACCOUNT }}" | base64 -d > /tmp/service-account.json && '
            'echo "Secret file created at /tmp/service-account.json"'
        )
    )

    # Task to run the main script with the secret file
    run_main_with_secret = BashOperator(
        task_id='run_main_script_with_dependencies',
        bash_command=(
            'pip install kafka-python google-cloud-bigquery && '
            'export GOOGLE_APPLICATION_CREDENTIALS=/tmp/service-account.json && '
            'python /opt/airflow/dags/repo/scripts/main.py'
        )
    )

    # Set task dependencies
    create_secret_file >> run_main_with_secret