import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
from numpy import add
import pandas as pd
from call_back.notify import succes_callback,failure_callback

def save_csv(path):
    df=pd.DataFrame(data={"ID":[1,2,3],
                      "name":["abhinav","Abhishek","Aakash"]})
    df["date"]=datetime.now()
    df.to_csv(path,index=False,sep="|")
    
    if "role" in df.columns:
        return True
    else:
        return False

def add_column_role(path,decision):
    role=decision
    if role==False:
        df=pd.read_csv(path,sep="|")
        df["role"]="student"
        df.to_csv(path,index=False,sep="|")
    
    else:
        df=pd.read_csv(path,sep="|")
        df["role"]="admin"
        df.to_csv(path,index=False,sep="|")

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    "email":["abhinavk1236@gmail.com"],
    # "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
    
}

dags=DAG(
    dag_id='dag_pythonoperator_xcom',
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
    path="/home/wittybrains/Desktop/csv_files/xcom.csv"
    save_csv_task=PythonOperator(task_id="pyton_operator_xcomPUSH",
                            python_callable=save_csv,
                            op_args=[path]
                            # provide_context=True
                            )
    
    add_column_role_task=PythonOperator(task_id="pyton_operator_xcomPULL",
                                        python_callable=add_column_role,
                                        op_args=[path,'{{ti.xcom_pull("pyton_operator_xcomPUSH")}}']
                                        # provide_context=True
                                        )
    start_task>>save_csv_task>>add_column_role_task





