from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dag_generator import dag_gen

def run_dag_generator():
    dag_gen()

dag = DAG(
    'dag_generator_dag',
    description='DAG to execute dag_generator.py',
    schedule_interval=None,
    start_date=datetime(2024, 2, 21),
    catchup=False
)

execute_dag_generator_task = PythonOperator(
    task_id='execute_dag_generator',
    python_callable=run_dag_generator,
    dag=dag
)