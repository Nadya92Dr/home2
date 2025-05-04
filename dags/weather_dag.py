from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from dotenv import load_dotenv
from weather import append_weather_data

load_dotenv(".env")
API_KEY = os.getenv("API_KEY")

default_args = {"owner": "airflow", "retries": 2, "retry_delay": timedelta(seconds=30)}

with DAG(
    dag_id="weather_data_pipeline",
    default_args=default_args,
    description="Сбор погодных данных каждую минуту",
    start_date=datetime(2023, 1, 1),
    schedule_interval="* * * * *",
    catchup=False,
) as dag:

    fetch_and_save_task = PythonOperator(
        task_id="fetch_and_save_weather",
        python_callable=append_weather_data,
        op_kwargs={"api_key": API_KEY, "city": "Moscow"},
    )

fetch_and_save_task
