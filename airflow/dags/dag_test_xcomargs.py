import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
from numpy import add
import pandas as pd
import json
from call_back.notify import succes_callback,failure_callback

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    "email":["abhinavk1236@gmail.com"],
    "email_on_failure": True,
    
    
}

dag = DAG(
    dag_id="dag_test_xcom",
    schedule_interval='@daily',
    start_date=datetime(2021,8,8,0,0,0),
    default_args=args,
    tags=['example'],
    on_success_callback=succes_callback,
    on_failure_callback=failure_callback,
    catchup=False,
    render_template_as_native_obj=True
)


def extract():
    data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
    return json.loads(data_string)

def transform(order_data):
    # order_data=json.loads(order_data)
    total_order_value=0
    
    for value in order_data.values():
        total_order_value += value
    return {"total_order_value": total_order_value}

with dag as dags:
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    
    transform_task = PythonOperator(
        task_id="transform", op_args=['{{ti.xcom_pull("extract")}}'],
        python_callable=transform
    )

extract_task >> transform_task