from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from call_back.notify import succes_callback,failure_callback
from call_back.test_call_back import test_callback
args = {
    'owner': 'airflow',
    'depends_on_past': False,
    "email":["abhinavk1236@gmail.com"],
    "email_on_failure": True
    
}

dags=DAG(
    dag_id='dag_trigger_rule',
    schedule_interval='@daily',
    start_date=datetime(2021,8,8,0,0,0),
    default_args=args,
    tags=['example'],
    on_success_callback=succes_callback,
    on_failure_callback=failure_callback,
    catchup=False
)
with dags as dag:
    start = ShortCircuitOperator(task_id="start", python_callable=lambda: False)
    skip_task=DummyOperator(task_id="skip_task")
    trigger_rule = DummyOperator(task_id="join_1", trigger_rule="none_failed_or_skipped")
    task_1 = DummyOperator(task_id="true_1")
    end = DummyOperator(task_id="false_1")

    start>>skip_task>>trigger_rule>>task_1>>end
    