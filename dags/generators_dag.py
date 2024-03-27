import os
import time
import sys
sys.path.append('/opt/airflow/dags/programs/WsSimulator_v01')
sys.path.append('/opt/airflow/dags/programs/FTSimulator_optimized_v02')

from programs.FTSimulator_optimized_v02.main_run import ft_dir_run
from programs.WsSimulator_v01.WSDataGenerator import WSDataGenerator

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'burhan',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def generator_1():
    t1 = time.time()
    # owned code  ---------------
    t2 = time.time()
    print(f'Overall run time {t2 - t1}')

def generator_2():
    t1 = time.time()
    # owned code  ---------------
    t2 = time.time()
    print(f"Overall run took {t2 - t1:.4f} seconds")
    print('Finished')


with DAG(
        default_args=default_args,
        dag_id='generators_dag_v02',
        description='first dag with python operator',
        start_date=datetime(2024, 3, 27),
        schedule_interval='@daily'
) as dag:
    generator1 = PythonOperator(
        task_id='generator_1',
        python_callable=generator_1,
    )

    generator2 = PythonOperator(
        task_id='generator_2',
        python_callable=generator_2,
    )

    generator1 >> generator2