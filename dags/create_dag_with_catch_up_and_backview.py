from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'burhan',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='dag_with_catch_up_and_backview_v04',
    default_args=default_args,
    description='dag with catch up and backview',
    start_date=datetime(2024, 2, 25, 2),
    schedule_interval='@daily',
    catchup=False
) as dag:
    task1= BashOperator(
        task_id='first_task',
        bash_command="echo this is a simple bash command!"
    )
    task1
    # task2= BashOperator(
    #     task_id='second_task',
    #     bash_command="echo hello world, this is the second task!"
    # )
    # task1.set_downstream(task2)
    

    # task3= BashOperator(
    #     task_id='third_task',
    #     bash_command="echo hello world, this is the third task!"
    # )

    # task4= BashOperator(
    #     task_id='four_task',
    #     bash_command="echo hello world, this is the four task!"
    # )
    # task2.set_downstream(task3)
    # task2.set_downstream(task4)