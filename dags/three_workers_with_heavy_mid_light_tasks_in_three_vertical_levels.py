from airflow.decorators import dag, task
from datetime import datetime
import time

# Define heavy, mid, and light task functions
@task(task_id="heavy_task", pool="pool_one", pool_slots=1)
def heavy_task_function():
    time.sleep(20)

@task(task_id="mid_task", pool="pool_two", pool_slots=1)   
def mid_task_function():
    time.sleep(12)

@task(task_id="light_task", pool="pool_three", pool_slots=1)
def light_task_function():
    time.sleep(3)

# Define DAG
@dag(
    start_date=datetime(2024, 4, 15),
    schedule_interval=None,
    catchup=False,
)
def three_workers_with_heavy_mid_light_tasks_in_three_vertical_levels():
    heavy_tasks = [heavy_task_function() for _ in range(5)]
    mid_tasks = [mid_task_function() for _ in range(5)]
    light_tasks = [light_task_function() for _ in range(5)]

   
    for  i in range(len(heavy_tasks)):
        heavy_tasks[i] >> mid_tasks[i] >> light_tasks[i]

dag_definition = three_workers_with_heavy_mid_light_tasks_in_three_vertical_levels()
