from airflow.decorators import dag, task
from datetime import datetime
import time
import random
from airflow.operators.python_operator import PythonOperator

# Define heavy, mid, and light task functions
@task(task_id="heavy_task", pool="generator_pool", pool_slots=1)
def heavy_task_function():
    time.sleep(20)

@task(task_id="mid_task", pool="generator_pool", pool_slots=1)   
def mid_task_function():
    time.sleep(12)

@task(task_id="light_task", pool="generator_pool", pool_slots=1)
def light_task_function():
    time.sleep(3)

# Define DAG
@dag(
    start_date=datetime(2024, 4, 15),
    schedule_interval=None,
    catchup=False,
)
def one_worker_with_random_heavy_mid_light_tasks_in_one_horizontal_levels():
    all_tasks = []
    all_tasks.extend([heavy_task_function() for _ in range(5)])
    all_tasks.extend([mid_task_function() for _ in range(5)])
    all_tasks.extend([light_task_function() for _ in range(5)])

    all_tasks_random_indices = random.sample(range(len(all_tasks)), len(all_tasks))

    for i in range(len(all_tasks) - 1):
        all_tasks[all_tasks_random_indices[i]] >> all_tasks[all_tasks_random_indices[i + 1]]

    return all_tasks

dag_definition = one_worker_with_random_heavy_mid_light_tasks_in_one_horizontal_levels()
