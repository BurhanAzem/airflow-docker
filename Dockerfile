FROM apache/airflow:2.8.1

RUN pip install --upgrade pip
RUN pip install polars numpy pandas billiard
RUN pip install apache-airflow-providers-amazon