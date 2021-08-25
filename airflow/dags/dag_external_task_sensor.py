from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from call_back.notify import succes_callback,failure_callback
args = {
    'owner': 'airflow',
    'depends_on_past': False,
    "email":["abhinavk1236@gmail.com"],
    "email_on_failure": True
    
}

dags=DAG(
    dag_id='dag_external_task_sensor',
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
    external_task_sensor=ExternalTaskSensor(task_id="external_task_sensor",
                                            external_dag_id="dag_branchpython_operator",
                                            external_task_id="dummy_task_1",
                                            timeout = 240, soft_fail= False,
                                            mode='reschedule', poke_interval=60
                                            )
    end_task=DummyOperator(task_id="end_task")
    start_task>>external_task_sensor >> end_task