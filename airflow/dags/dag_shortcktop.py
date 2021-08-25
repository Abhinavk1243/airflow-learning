import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import ShortCircuitOperator
from datetime import datetime,timedelta
from airflow.models import Variable
import json
from call_back.notify import succes_callback,failure_callback

from airflow.operators.email_operator import EmailOperator

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    "email":["abhinavk1236@gmail.com"],
    "email_on_failure": True
    
}

def give_decision_downstreamSkip(decision):
    decision=json.loads(decision)
    return decision["decision"]
   

dags=DAG(
    dag_id='dag_ShortCktOp',
    schedule_interval='@daily',
    start_date=datetime(2021,8,8,0,0,0),
    default_args=args,
    on_success_callback=succes_callback,
    on_failure_callback=failure_callback,
    tags=['example'],
    catchup=False
)
task_no=Variable.get("task_no",deserialize_json=True)
# decsion=task_no["decision"]
with dags as dag:

    start=DummyOperator(task_id="start")
    print("===================here===================")
    print(type(task_no["decision"]))
    short_ckt=ShortCircuitOperator(task_id="short_ckt_op",
                                   python_callable=give_decision_downstreamSkip,
                                   op_args=["{{ (dag_run.conf['task_no'])|tojson if dag_run.conf else (var.json.task_no)|tojson}}"]
                                )

    downstream1=DummyOperator(task_id="DS1")

    start>>short_ckt>>downstream1
