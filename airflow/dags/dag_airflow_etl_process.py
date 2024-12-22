from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from datetime import datetime
import requests
from sqlalchemy import create_engine
from datetime import datetime,timedelta
import pandas as pd
import pendulum
import os
import re
import sys
import glob
import time
import psycopg2
import numpy as np
from psycopg2 import sql

# abspath = os.path.dirname(__file__) # get directory path of current file
abspath = " /Desktop/hieult_pycode/ "
main_cwd = re.sub('hieult_pycode.*','hieult_pycode',abspath) # modify the path  
os.chdir(main_cwd) # change current working directory to the modified path
sys.path.append(main_cwd) # add the modified path to system path

# set-up connect credentials
host = 'localhost'
name = 'postgres'
passwd = 'matkhau2908'
db = 'postgres'

# set-up dag info
dag_id = 'dag_airflow_etl'
email_to = 'luutrunghieu298@gmail.com'
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

def report_failure(context):
    """
    This function is used to send an email notification when the DAG fails.
    """
    subject = f"Airflow alert: DAG {context['dag'].dag_id} failed"
    body = f"DAG {context['dag'].dag_id} failed at {context['execution_date']}."
    send_email = EmailOperator(
        task_id='send_email',
        to=email_to,
        subject=subject,
        html_content=body,
        dag=dag,
        trigger_rule='one_failed'
    )
    send_email.execute(context)

default_args = {
    'owner': 'hieult',
    'start_date': datetime(2023, 1, 1, tzinfo=local_tz),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': report_failure
}

dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    schedule='0 22 * * *', # run every day at midnight
    description= 'test etl airflow',
    catchup=False
)

# I want to create a task to run the load_excel_file.py file
# task2 = PythonOperator(
#     task_id = 'load_excel_file',
#     python_callable = lambda: exec(open(r'C:\Users\USER\Desktop\hieult_pycode\dag_airflow\load_data\load_excel_file.py').read()),
#     dag = dag,
# )

cmd_command_0 = 'python /Desktop/hieult_pycode/dag_airflow/load_data/load_excel_file.py'

task0 = BashOperator(
    task_id = 'load_excel_file',
    bash_command = cmd_command_0,
    dag = dag
)

# FileSensor
# task1 = FileSensor(
# 	task_id = 'file_available',
# 	filepath = r'C:\Users\USER\Desktop\data\SI\SI_092024.xlsx', # đường dẫn đến tệp cần kiểm tra
# 	poke_interval = 60, # (mặc định là 60 giây), chu kỳ kiểm tra sự tồn tại của tệp
# 	timeout = 400, # thời gian chờ tối đa cho đến khi tệp xuất hiện
#     dag = dag
# )

# task2 = BashOperator(
#     task_id = 'load_excel_file',
#     bash_command = r"python C:\Users\USER\Desktop\hieult_pycode\dag_airflow\load_data\load_excel_file.py",
#     dag = dag
# )

# task3 = BashOperator(
#     task_id = 'run_procedure',
#     bash_command = r"python C:\Users\USER\Desktop\hieult_pycode\dag_airflow\load_data\run_procedure.py",
#     dag = dag
# )

# task2 >> task3




