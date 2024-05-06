from airflow.decorators import dag, task
from datetime import datetime
import time

# Define heavy, mid, and light task functions
@task(task_id="heavy_task_level_one", pool="generator_pool", pool_slots=1)
def heavy_task_level_one_function():
    time.sleep(20)

@task(task_id="mid_task_level_one", pool="generator_pool", pool_slots=1)   
def mid_task_level_one_function():
    time.sleep(12)

@task(task_id="light_task_level_one", pool="generator_pool", pool_slots=1)
def light_task_level_one_function():
    time.sleep(3)
    
    
@task(task_id="heavy_task_level_two", pool="generator_pool", pool_slots=1)
def heavy_task_level_two_function():
    time.sleep(20)

@task(task_id="mid_task_level_two", pool="generator_pool", pool_slots=1)   
def mid_task_level_two_function():
    time.sleep(12)

@task(task_id="light_task_level_two", pool="generator_pool", pool_slots=1)
def light_task_level_two_function():
    time.sleep(3)

# Define DAG
@dag(
    start_date=datetime(2024, 4, 15),
    schedule_interval=None,
    catchup=False,
)
def one_worker_with_heavy_mid_light_tasks_in_two_vertical_levels():
    heavy_tasks_level_one = [heavy_task_level_one_function() for _ in range(5)]
    mid_tasks_level_one = [mid_task_level_one_function() for _ in range(5)]
    light_tasks_level_one = [light_task_level_one_function() for _ in range(5)]
    
    heavy_tasks_level_two = [heavy_task_level_two_function() for _ in range(5)]
    mid_tasks_level_two = [mid_task_level_two_function() for _ in range(5)]
    light_tasks_level_two = [light_task_level_two_function() for _ in range(5)]

    
    for  i in range(len(light_tasks_level_two)):
        heavy_tasks_level_one[i] >> heavy_tasks_level_two[i]
        mid_tasks_level_one[i] >> mid_tasks_level_two[i]
        light_tasks_level_one[i] >> light_tasks_level_two[i]
        
       

dag_definition = one_worker_with_heavy_mid_light_tasks_in_two_vertical_levels()
