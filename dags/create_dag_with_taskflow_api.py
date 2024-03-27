from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'burhan',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)    
}

@dag(dag_id='dag_with_taskflow_api_v01', 
     default_args=default_args,
     start_date=days_ago(1),
     schedule_interval='@daily')
def first_dag_with_taskflow_api():
    
    @task()    
    def get_name():
        return {
            'first_name': 'Burhan',
            'last_name': 'Azem'
        }
    
    @task()
    def get_age():
        return 23
    
    @task()
    def greet(name_dict, age):
        first_name = name_dict['first_name']
        last_name = name_dict['last_name']
        print(f"Hello World! My first name is {first_name}, last name is {last_name}"
              f", and I am {age} year old!")
    
    name_dict = get_name()
    age = get_age()
    greet(name_dict=name_dict, age=age)

greet_dag = first_dag_with_taskflow_api()
