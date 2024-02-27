from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'deena',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='my_first_dag_1',
    default_args=default_args,
    start_date=datetime(2024, 1, 29),
    schedule_interval='50 * * * *'
) as dag:

    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello, this is the first task!",
        dag=dag
    )
    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo hey, this is task2 !",
        dag=dag
    )
    task3 = BashOperator(
        task_id='third_task',
        bash_command="echo hi, this is task3!",
        dag=dag
    )

   



    task1 >> task2 >> task3   # Task dependencies
