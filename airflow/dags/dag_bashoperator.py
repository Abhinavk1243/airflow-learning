import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta, datetime
from airflow.models import Variable
from call_back.notify import succes_callback,failure_callback
from custom_operators.sc_bashoperator import sc_BashOperator
queue="bash_test"
args = {
    'owner': 'airflow',
    'depends_on_past': False,
    
    "email":["abhinavk1236@gmail.com"],
    "email_on_failure": True
    
}


dags=DAG(
    dag_id='dag_bashoperator',
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
    # make_csv_file=BashOperator(task_id="make_csv_file", 
    #                            bash_command='/usr/bin/python3 /home/wittybrains/airflow/dags/test.py')
    # test_bashcomm=BashOperator(task_id="test_bash_command",
    #                         bash_command="cd /home/wittybrains/airflow_learning && python3 -m scripts.test_bash -ti {{ ti.task_id }} -dag {{ ti.dag_id }} -ji {{ ti.job_id }} -ex {{ ti.execution_date }}")
    test_custom_bashcommand=sc_BashOperator(task_id="custom_bash_command",
                            bash_command="python3 -m scripts.test_bash")
    start_task>>test_custom_bashcommand