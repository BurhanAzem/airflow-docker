from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'burhan',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='dag_with_postgres_operator_v01',
    default_args=default_args,
    description='dag with postgres operator',
    start_date=datetime(2024, 3, 20),
    schedule_interval='0 0 * * *',
    catchup=False,  # Set to False if you don't want to backfill old DAG runs
) as dag:

    create_postgres_table = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id="postgres_localhost",
        sql="""
            CREATE TABLE IF NOT EXISTS dag_runs (
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            );
        """
    )

    # Dummy operator for better visualization in the web UI
    create_postgres_table
