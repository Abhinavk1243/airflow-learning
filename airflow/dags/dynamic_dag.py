
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import importlib.util
spec = importlib.util.spec_from_file_location("module.name", "/home/wittybrains/airflow/call_back/notify.py")
foo = importlib.util.module_from_spec(spec)
spec.loader.exec_module(foo)

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    "email":["abhinavk1236@gmail.com"],
    "email_on_failure": True
}

jobs=[{"dag_id":"dynamic_1","schedule":"@daily"},{"dag_id":"dynamic_2","schedule":"@monthly"}]
for job in jobs:
    dags=DAG(
        dag_id=f"dag_{job['dag_id']}",
        schedule_interval=job["schedule"],
        start_date=datetime(2021,8,8,0,0,0),
        default_args=args,
        tags=['example'],
        on_success_callback=foo.succes_callback,
        on_failure_callback=foo.failure_callback,
        catchup=False
    )
    with dags as dag:
        start=DummyOperator(task_id=f"run_{job['dag_id']}")
        start
    
    globals()[str(job['dag_id'])] = dag