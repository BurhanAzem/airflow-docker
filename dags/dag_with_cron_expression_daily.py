from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'burhan',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='dag_with_cron_expression_v01',
    default_args=default_args,
    description='This is my first DAG',
    start_date=datetime(2024, 2, 15),
    schedule_interval='0 0 * * *'
) as dag:
    task1= BashOperator(
        task_id='task1',
        bash_command="echo dag with cron expression!"
    )
    task1