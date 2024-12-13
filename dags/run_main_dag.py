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
    description='Run main.py with --one argument and install dependencies',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task to install dependencies
    install_dependencies = BashOperator(
        task_id='install_dependencies',
        bash_command='pip install -r /opt/airflow/dags/repo/scripts/requirements.txt',
    )

    # Task to run the main Python script
    run_main = BashOperator(
        task_id='run_main_script',
        bash_command='python /opt/airflow/dags/repo/scripts/main.py --one',
    )

    # Set task dependencies
    install_dependencies >> run_main
