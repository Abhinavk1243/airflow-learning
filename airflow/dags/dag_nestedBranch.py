from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from call_back.notify import succes_callback,failure_callback
args = {
    'owner': 'airflow',
    'depends_on_past': False,
    "email":["abhinavk1236@gmail.com"],
    "email_on_failure": True
    
}

dags=DAG(
    dag_id='dag_nested_branch',
    schedule_interval='@daily',
    start_date=datetime(2021,8,8,0,0,0),
    default_args=args,
    tags=['example'],
    on_success_callback=succes_callback,
    on_failure_callback=failure_callback,
    catchup=False
)
with dags as dag:
    branch_1 = BranchPythonOperator(task_id="branch_1", python_callable=lambda: "false_1")
    join_1 = DummyOperator(task_id="join_1", trigger_rule="none_failed_or_skipped")
    true_1 = DummyOperator(task_id="true_1")
    false_1 = DummyOperator(task_id="false_1")
    branch_2 = BranchPythonOperator(task_id="branch_2", python_callable=lambda: "true_2")
    join_2 = DummyOperator(task_id="join_2", trigger_rule="none_failed_or_skipped")
    true_2 = DummyOperator(task_id="true_2")
    false_2 = DummyOperator(task_id="false_2")
    false_3 = DummyOperator(task_id="false_3")

    branch_1 >> true_1 >> join_1
    branch_1 >> false_1 >> branch_2 >> [true_2, false_2] >> join_2 >> false_3 >> join_1