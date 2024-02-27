from airflow import DAG
from random import randint
from airflow.decorators import dag,task
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime,timedelta
from airflow.models import Variable


default_args = {
    'owner': 'deena',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='sample_dag',
    default_args=default_args,
    start_date=datetime(2024,2,12),
    schedule_interval="8 20 * * * ",
)as dag:

    def get_variable()->str:
     user_name = Variable.get("sample")
     return f"user name = {user_name}"

    task_1=PythonOperator(
        task_id='get_name',
        python_callable=get_variable
    )
  
get_variable()
  

   


