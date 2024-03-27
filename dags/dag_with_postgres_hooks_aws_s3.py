import csv
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from psycopg2.errors import UniqueViolation
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from tempfile import NamedTemporaryFile
import pandas as pd
import sys
sys.path.append('/opt/airflow/data/Orders')


default_args = {
    'owner': 'burhan',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}

def load_data_to_postgres():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_localhost')
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()

    with open('/opt/airflow/data/Orders.csv', 'r') as file:
        next(file)  # Skip header row
        for line in file:
            try:
                cursor.copy_expert(f"COPY orders FROM STDIN WITH CSV HEADER", file)
            except UniqueViolation as e:
                # Log the error or handle it in another way
                print(f"Error: {e}")
                # Optionally, you can continue processing the rest of the rows
                continue

    connection.commit()
    connection.close()

def postgres_to_s3(ds_nodash, next_ds_nodash):
    # step 1: query data from postgressql db and save into text file
    hook = PostgresHook(postgres_conn_id='postgres_localhost')
    connection = hook.get_conn()
    cursor = connection.cursor()

    cursor.execute("select * from orders where date >= %s and date < %s",
                   (ds_nodash, next_ds_nodash))
    with NamedTemporaryFile(mode='w', suffix=f"{ds_nodash}_file_{next_ds_nodash}") as f:
    # with open(f"dags/get_orders_{ds_nodash}.txt", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
        f.flush()
        cursor.close()
        connection.close()
        logging.info("Saved orders data in text file: %s", f"dags/get_orders_{ds_nodash}.txt")
    # step 2: upload text file into S3
        s3_hook = S3Hook(aws_conn_id="minio_conn")
        s3_hook.load_file(filename=f.name,
                            key=f"orders/{datetime.today()}_file.txt",
                            bucket_name="airflow",
                            replace=True)
        logging.info("Orders file %s has been pushed to S3!", f.name)

with DAG(
    dag_id='dag_with_postgres_hooks_aws_s3_v01',
    default_args=default_args,
    description='dag_with_postgres_hooks_aws_s3',
    start_date=datetime(2024, 3, 23),
    schedule_interval='0 0 * * *',
    catchup=False,
) as dag:

    create_postgres_table_task = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id="postgres_localhost",
        sql="""
            CREATE TABLE IF NOT EXISTS public.orders (
                order_id character varying,
                date date,
                product_name character varying,
                quantity integer,
                primary key (order_id)
            );
        """
    )

    load_data_to_postgres_task = PythonOperator(
        task_id='load_data_to_postgres',
        python_callable=load_data_to_postgres
    )

    postgres_to_s3_task = PythonOperator(
        task_id='postgres_to_s3',
        python_callable=postgres_to_s3
    )



    create_postgres_table_task >> load_data_to_postgres_task >> postgres_to_s3_task


# docker run -dt -p 9000:9000 -p 9001:9001 -v PATH:/mnt/data -v /etc/default/minio:/etc/config.env -e "MINIO_CONFIG_ENV_FILE=/etc/config.env" --name "minio_local" minio server --console-address "docker run \:9001"