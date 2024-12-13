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
    'infinite_loop_dag',
    default_args=default_args,
    description='A DAG to run an infinite process using BashOperator',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    run_infinite_loop = BashOperator(
        task_id='run_infinite_loop',
        bash_command='''
        echo "Starting infinite loop..."
        while true; do
            echo "Script is running... $(date)"
            sleep 10
        done
        '''
    )
