import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.weekday import DayOfWeekSensor
from airflow.sensors.python import PythonSensor
from datetime import timedelta, datetime
from call_back.notify import succes_callback,failure_callback,dag_sla_miss_callback
import random

def decision():
    random_num=random.randint(1,10)
    if random_num%2==0:
        return True
    else:
        return False


args = {
    'owner': 'airflow',
    'depends_on_past': False,
    "email":["abhinavk1236@gmail.com"],
    "sla": timedelta(seconds=500),
    "email_on_failure": True
    
}


dags=DAG(
    dag_id='dag_sensor',
    schedule_interval='@daily',
    start_date=datetime(2021,8,8,0,0,0),
    default_args=args,
    tags=['example'],
    on_success_callback=succes_callback,
    on_failure_callback=failure_callback,
    sla_miss_callback=dag_sla_miss_callback,
    catchup=False
)


with dags as dag:
    start=DummyOperator(task_id="start")
    file_sensor=FileSensor(task_id="file_sensor",
                           filepath="/home/wittybrains/Desktop/csv_files/test1234.csv",
                           timeout =660, soft_fail= False, mode='reschedule', poke_interval=60
                           )
    weekend_check = DayOfWeekSensor(task_id='weekend_check',
                                    week_day='Friday',
                                    use_task_execution_day=True,
                                    timeout = 660, soft_fail= True, mode='reschedule', poke_interval=60
                                    )
    python_sensor=PythonSensor(task_id="python_sensor",
                               python_callable=decision,
                               timeout = 660, soft_fail= True, mode='reschedule')
    end=DummyOperator(task_id="end")

    start>>file_sensor>>weekend_check>>python_sensor>>end