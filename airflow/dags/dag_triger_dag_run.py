from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import datetime
from call_back.notify import succes_callback,failure_callback

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    "email":["abhinavk1236@gmail.com"],
    "email_on_failure": True
}

dags=DAG(
    dag_id='dag_TriggerDagRun',
    schedule_interval='@daily',
    start_date=datetime(2021,8,8,0,0,0),
    default_args=args,
    tags=['example'],
    on_success_callback=succes_callback,
    on_failure_callback=failure_callback,
    catchup=False
)
with dags as dag:
    start_task=DummyOperator(task_id="start_task")
    external_task_sensor=TriggerDagRunOperator(task_id="dag_trigger_dag_run",
                                             trigger_dag_id="dag_pythonoperator_xcom",
                                             poke_interval=600)
    start_task>>external_task_sensor