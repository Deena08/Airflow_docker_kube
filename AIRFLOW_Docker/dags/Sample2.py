from airflow import DAG
from random import randint
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime

def _train_model():
    return randint(1,10)

def _best_model(ti):
    acc = ti.xcom_pull(task_ids=[
        "train_model_A",
        "train_model_B",
        "train_model_C"
    ])

    best_acc=max(acc)

    if (best_acc < 8):
      return 'inaccurate'
    else:
        return 'accurate' 
        

with DAG("my_dag", start_date=datetime(2024, 1, 31)
    ,schedule_interval="* * * * * ",catchup=False) as dag:

       train_model_A=PythonOperator(
           task_id="train_model_A",
           python_callable=_train_model
       )

       train_model_B=PythonOperator(
           task_id="train_model_B",
           python_callable=_train_model
        )

       train_model_C=PythonOperator(
           task_id="train_model_C",
           python_callable=_train_model
        )
    
       choose_best_model=BranchPythonOperator(
          task_id="_best_model",
          python_callable=_best_model
       )   
       
       accurate = BashOperator(
             task_id="accurate",
             bash_command="echo 'accurate'"
       )

       inaccurate = BashOperator(
             task_id="inaccurate",
             bash_command="echo 'inaccurate'"
       )

       [train_model_A, train_model_B, train_model_C] >> choose_best_model >> [accurate, inaccurate]
