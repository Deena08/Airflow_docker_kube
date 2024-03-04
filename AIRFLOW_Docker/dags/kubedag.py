from airflow import DAG
from datetime import datetime
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

dag = DAG(
    'my_kubernetes_dag',
    default_args=default_args,
    schedule_interval='@once',
)

task1 = KubernetesPodOperator(
    namespace='default',
    image="my_image",
    cmds=["echo", "Hello from the KubernetesPodOperator"],
    name="task1",
    task_id="task1",
    get_logs=True,
    dag=dag
)

task1
