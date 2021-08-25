from datetime import datetime,timedelta
from re import template
import smtplib
import pandas as pd
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email import encoders
from jinja2 import Environment, FileSystemLoader
import configparser
import mysql.connector
import logging as lg

class send_email:
    def __init__(self):
        self.env=self.get_config("execution_mode","environment")

    def get_logger(self):
        logger = lg.getLogger(__name__)
        logger.setLevel(lg.DEBUG)
        formatter = lg.Formatter('%(asctime)s : %(name)s : %(filename)s : %(levelname)s\
        :%(funcName)s :%(lineno)d : %(message)s ')
        file_handler =lg.FileHandler("/home/wittybrains/airflow_learning/log_file.log")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        return logger

    def get_config(self,section,key):
        parser = configparser.ConfigParser()
        parser.read('/home/wittybrains/airflow_learning/config/sqlcred.cfg')
        return parser.get(section,key)

    def get_db_conn(self,section):
        dbconfig={'host' :  self.get_config(section,"host"),
        'database' :  self.get_config(section,"database"),
        'user' :  self.get_config(section,"user"),
        'password' :  self.get_config(section,"password"),}
        conn=mysql.connector.connect(**dbconfig)
        return conn

    def send_mail(self,receivers_email,mail_content=None,subject=None,data=None,html_template=None):
        logger=self.get_logger()
        logger.debug("in send_mail")
        try:
            logger.debug("in function")
            mail = smtplib.SMTP('smtp.gmail.com', 587)
            mail.starttls()
            message = MIMEMultipart('mixed')
            message['From'] = self.get_config("email","sender")
            message['To'] = ",".join([str(i) for i in receivers_email])
            message['Subject'] = subject
            if mail_content!=None:
                message.attach(MIMEText(mail_content, 'plain'))

            if html_template is not None:
                enviornment_var = Environment(loader=FileSystemLoader('/home/wittybrains/airflow_learning/templates/'))
                    
                content_data=data
                mail_template = enviornment_var.get_template(f"{html_template}.html")
                html = mail_template.render(content_data=content_data)
                message.attach(MIMEText(html, 'html'))
            logger.debug("Html")
            body = message.as_string()
            mail.login(self.get_config("email","sender"),self.get_config("email","password"))
            mail.sendmail(self.get_config("email","sender"), receivers_email,body)
            mail.quit()   
        except Exception as error:
            logger.error(error)
    
    def get_last_five_exe(self,dag_id,content_data):
        logger=self.get_logger()
        try:
            dag_run_query = f"""SELECT execution_date,start_date,end_date,state,external_trigger FROM airflow_db.dag_run
                            where dag_id='{dag_id}' order by dag_id desc limit 5;"""
            logger.debug("get airflow con to fetch last 5 execution")
            airflow_con=self.get_db_conn(section="airflow_db")
            last_5_exec = pd.read_sql(sql=dag_run_query, con=airflow_con)
            last_5_exec["execution_date"]= pd.to_datetime(last_5_exec["execution_date"]) 
            last_5_exec["execution_date"]=last_5_exec["execution_date"].dt.strftime('%Y-%m-%d %H:%M:%S')
            last_5_exec["start_date"]= pd.to_datetime(last_5_exec["start_date"]) 
            last_5_exec["start_date"]=last_5_exec["start_date"].dt.strftime('%Y-%m-%d %H:%M:%S') 
            last_5_exec["end_date"]= pd.to_datetime(last_5_exec["end_date"]) 
            last_5_exec["end_date"]=last_5_exec["end_date"].dt.strftime('%Y-%m-%d %H:%M:%S')
            last_5_exec["external_trigger"]=last_5_exec["external_trigger"].apply(lambda x : "triggered" if x==1 else "scheduled")
            
                    
            content_data["last_5_exec"]= last_5_exec.to_dict(orient='records') if not last_5_exec.empty else []
            return content_data
        except Exception as error:
            logger.error(error)


def succes_callback(context):
    sn=send_email()
    logger=sn.get_logger()
    try:
        logger.debug(f"context objext as {context}")
            
        dag_id = context["dag"].dag_id
        execution_date=context['execution_date']+timedelta(hours=5,minutes=30)
        execution_date = execution_date.strftime('%Y-%m-%d %H:%M:%S.%f')
        
        content_data = {}
        content_data["dag_id"] = dag_id
        content_data["execenviron"] = sn.env
        content_data["state"] = "completed"
        content_data["serverurl"] = "sn.serverurl"
            
        task_instance_query=f"""select dag_id,task_id,start_date,end_date,try_number,duration from airflow_db.task_instance 
                                where  execution_date='{execution_date}' and
                                dag_id='{dag_id}' """
                                    
            
        dag_run_query = f"""SELECT execution_date,start_date,end_date,state,external_trigger FROM airflow_db.dag_run
                        where dag_id='{dag_id}' order by dag_id desc limit 5;"""
        logger.debug("get airflow_db conn")
        airflow_con=sn.get_db_conn(section="airflow_db")
        
        content_data=sn.get_last_five_exe(dag_id,content_data)
                
        df_retry = pd.read_sql(sql=task_instance_query, con=airflow_con)
        df_retry["start_date"]= pd.to_datetime(df_retry["start_date"]) 
        df_retry["start_date"]=df_retry["start_date"].dt.strftime('%Y-%m-%d %H:%M:%S') 
        df_retry["end_date"]= pd.to_datetime(df_retry["end_date"]) 
        df_retry["end_date"]=df_retry["end_date"].dt.strftime('%Y-%m-%d %H:%M:%S') 
        cols=list(df_retry.columns)
        content_data['retries_data'] = df_retry.to_dict(orient='records') if not df_retry.empty else []
        data={"content_data":content_data,"cols":cols}
        # print(data)
        mail_content=f'''dag  with dag id  {dag_id} is success'''
        subject=f"[SUCCESS] DataPlatform-sn.execenviron:{dag_id} : Completed successfully"
        # print(content_data)
        logger.debug("send mail")
        receiver=["abhinavk1236@gmail.com"]
        sn.send_mail(receiver,mail_content=mail_content,subject=subject,html_template="dag_success",data=data)
                
                
    except Exception as error:
        logger.error(f"exception as {error}")

    
def failure_callback(context):
    sn=send_email()
    logger=sn.get_logger()
    try:
        dag_id = context["dag"].dag_id
        execution_date=context['execution_date']+timedelta(hours=5,minutes=30)
        execution_date = execution_date.strftime('%Y-%m-%d %H:%M:%S.%f')
        
        airflow_con=sn.get_db_conn("airflow_db")
        dag_id=str(dag_id) 
        content_data = {}
        content_data["dag_id"] = dag_id
        content_data["execenviron"] = sn.env
        content_data["state"] = "Failed"
        content_data["serverurl"] = "sn.serverurl"
        content_data=sn.get_last_five_exe(dag_id,content_data)
        task_instance_query = f"""select task_id, job_id as airflow_job_id, start_date, end_date, state, 
                                try_number, duration from task_instance where dag_id='{dag_id}' and 
                                execution_date='{execution_date}' and state!='success';"""
        df_retry = pd.read_sql(sql=task_instance_query, con=airflow_con)  
        df_retry["start_date"]= pd.to_datetime(df_retry["start_date"]) 
        df_retry["start_date"]=df_retry["start_date"].dt.strftime('%Y-%m-%d %H:%M:%S') 
        df_retry["end_date"]= pd.to_datetime(df_retry["end_date"]) 
        df_retry["end_date"]=df_retry["end_date"].dt.strftime('%Y-%m-%d %H:%M:%S') 


        cols=list(df_retry.columns)
        # df_retry['start_date'] = df_retry['start_date'].dt.tz_localize('GMT').dt.tz_convert('America/New_York')
        content_data['retries_data'] = df_retry.to_dict(orient='records') if not df_retry.empty else []
        data={"content_data":content_data,"cols":cols}
        mail_content=f'''dag  with dag id  {context['dag'].dag_id} is FAILED'''
        subject=f"[FAILED] DataPlatform-sn.execenviron:{dag_id} : FAILED"
        logger.debug("send mail")
        receiver=["abhinavk1236@gmail.com"]
        sn.send_mail(receiver,mail_content=mail_content,subject=subject,html_template="dag_failed",data=data)
                
    except Exception as error:
        logger.error(error)


def dag_sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    sn=send_email()
    logger=sn.get_logger()
    try:
        logger.debug(f"SLA was missed on DAG {dag}s by task id {blocking_tis}s with task list {task_list}s which are blocking {blocking_task_list}s")
        logger.debug(f"context values: {dag}, {task_list}, {blocking_task_list}, {slas}, {blocking_tis}")
        dag_id = slas[0].dag_id
        task_id = slas[0].task_id
        execution_date = slas[0].execution_date.isoformat()
        dag_id=str(dag_id)
        task_id=str(task_id)
        msg='task has failed for '+dag_id+'  '+task_id
        content_data = {}
        content_data["dag_id"] = dag_id
        content_data["execenviron"] = sn.env
        content_data["state"] = "completed"
        content_data["serverurl"] = "devop"
        content_data["task_id"] = task_id
        data={"content_data":content_data}
        template="dag_sla_miss"
        subject=f"[SLA MISS] DataPlatform-sn.execenviron:{dag_id} : SLA MISS"
        receiver=["abhinavk1236@gmail.com"]
        mail_content=f'''dag  with dag id  {dag_id} MISS SLA'''
        sn.send_mail(receiver,mail_content=mail_content,subject=subject,html_template=template,data=data)
  
    except Exception as error:
        logger.error(f"Exception raised : {error}")




