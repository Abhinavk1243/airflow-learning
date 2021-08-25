from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.edgemodifier import Label
from call_back.notify import succes_callback,failure_callback
args = {
    'owner': 'airflow',
    'depends_on_past': False,
    "email":["abhinavk1236@gmail.com"],
    "email_on_failure": True
    
}


def should_run(task_number):
    """
    Determine which dummy_task should be run based on if the execution date minute is even or odd.

    :param dict kwargs: Context
    :return: Id of the task to run
    :rtype: str
    """
    # print(
    #     '------------- exec dttm = {} and minute = {}'.format(
    #         kwargs['execution_date'], kwargs['execution_date'].minute
    #     )
    # )
    if task_number=="first":
        return ["dummy_task_1","dummy_task_2"]
    else:
        return ["dummy_task_3"]
    


with DAG(
    dag_id='dag_branchpython_operator',
    schedule_interval='@daily',
    start_date=datetime(2021,8,8,0,0,0),
    default_args=args,
    tags=['example'],
    on_success_callback=succes_callback,
    on_failure_callback=failure_callback,
    catchup=False
) as dag:

    cond = BranchPythonOperator(
        task_id='condition',
        python_callable=should_run,
        op_args=["{{ (dag_run.conf['task_no']['branch']) if dag_run.conf else (var.json.task_no['branch'])}}"]
     )
    
    dummy_task_1 = DummyOperator(task_id='dummy_task_1')
    dummy_task_2 = DummyOperator(task_id='dummy_task_2')
    dummy_task_3 = DummyOperator(task_id='dummy_task_3')
    
    cond >>Label("task_no : first ")>> [dummy_task_1, dummy_task_2]
    cond >>Label("task_no : second ")>> dummy_task_3
