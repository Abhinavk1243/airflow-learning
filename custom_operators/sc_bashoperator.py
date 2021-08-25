from airflow.models.baseoperator import BaseOperator
from airflow.operators.bash_operator import BashOperator
import os

class sc_BashOperator(BashOperator):
    def __init__(self,bash_command,*args,**kwargs):
        super(sc_BashOperator,self).__init__(bash_command=bash_command, *args, **kwargs)
        self.bash_command=bash_command

    def execute(self,context):
        ti = context['ti']
        taskid = ti.task_id
        dagid = ti.dag_id
        bash_command_new=f'cd {os.path.expanduser("~")}/airflow_learning && {self.bash_command} -ti  {context["ti"].task_id} -dag {context["ti"].dag_id } -ji { context["ti"].job_id} -ex { context["ti"].execution_date} '
        self.bash_command = bash_command_new
        return super(sc_BashOperator, self).execute(context=context)