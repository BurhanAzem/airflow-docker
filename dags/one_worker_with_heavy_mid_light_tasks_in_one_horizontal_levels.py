from airflow.decorators import dag, task
from datetime import datetime
import time
from datetime import datetime, timedelta
import random
# Define heavy, mid, and light task functions
@task(task_id="heavy_task", pool="generator_pool", pool_slots=1, trigger_rule="all_done")
def heavy_task_function():
    time.sleep(20)

@task(task_id="mid_task", pool="generator_pool", pool_slots=1, trigger_rule="all_done")   
def mid_task_function():
    time.sleep(12)

@task(task_id="light_task", pool="generator_pool", pool_slots=1, trigger_rule="all_done")
def light_task_function():
    time.sleep(3)

# Define DAG
@dag(
    start_date=datetime(2024, 4, 15),
    catchup=False,
    # schedule_interval=timedelta(seconds=2),
    # max_active_runs=1 
)
def one_worker_with_heavy_mid_light_tasks_in_one_horizontal_levels():
    heavy_tasks = [heavy_task_function() for _ in range(5)]
    mid_tasks = [mid_task_function() for _ in range(5)]
    light_tasks = [light_task_function() for _ in range(5)]

    heavy_tasks[0] >> mid_tasks[0] >> light_tasks[0] >> heavy_tasks[1] >> mid_tasks[1] >> light_tasks[1] >> heavy_tasks[2] >> mid_tasks[2] >> light_tasks[2] >> heavy_tasks[3] >> mid_tasks[3] >> light_tasks[3] >> heavy_tasks[4] >> mid_tasks[4] >> light_tasks[4]  

        
dag_definition = one_worker_with_heavy_mid_light_tasks_in_one_horizontal_levels()
