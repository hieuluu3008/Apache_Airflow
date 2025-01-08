from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task_group
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import pytz
import pendulum






abs_path = '/opt/airflow/dags/BI/Y4A_BA_Team'
local_tz = pendulum.timezone("Asia/Hong_Kong")
email_to = 'hieult@yes4all.com'


def report_failure(context):
    # include this check if you only want to get one email per DAG
    send_email = EmailOperator(
    task_id="email_on_failure",
    to= email_to,
    subject=f'Airflow alert: {dag.dag_id} failed',
    html_content=f'Hi,<br><br>Something went wrong with the <b> {dag.dag_id} </b> DAG. Please check the logs for more details.<br> Details: http://172.30.15.30:8090/dags/{dag.dag_id} <br><br>Thanks,<br>BI Team',
    dag=dag,
    trigger_rule='all_done')
    send_email.execute(context)


default_args = {
    'owner': 'BI Team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=5),
    'start_date': datetime(2024, 11, 20, tzinfo=pytz.timezone("Asia/Hong_Kong")),
    "on_failure_callback": report_failure,
    'execution_timeout': timedelta(hours=1),
    'catchup': False
}
 
# dag = DAG(
#     dag_id='dag_service_daily',
#     default_args=default_args,
#     description='revenue daily',
#     schedule_interval='0 19 * * *'
# )


with DAG(
    dag_id='dag_service_daily',
    default_args=default_args,
    description='service_daily',
    schedule_interval='0 19 * * *',
    tags=['service', 'daily']
) as dag:


    task0 = BashOperator(
        task_id='master_data_erp',
        bash_command= f'python {abs_path}/BI_Team/HieuLT/ERP/master_data.py',
        dag=dag,
        retries = 1,
        retry_delay = timedelta(minutes=5)
    )


# Revenue group
    @task_group(group_id='revenue_group')
    def tg1():
        task1 = BashOperator(
            task_id='bls_daily_revenue',
            bash_command=f'python {abs_path}/BI_Team/HieuLT/ERP/Service/Bluestars/revenue_bls_daily.py',
            dag=dag,
            retries = 1,
            retry_delay = timedelta(minutes=5)
        )


        task2 = BashOperator(
            task_id = 'vitox_daily_revenue',
            bash_command = f'python {abs_path}/BI_Team/HieuLT/ERP/Service/Vitox/revenue_vitox_daily.py',
            dag=dag,
            retries = 1,
            retry_delay = timedelta(minutes=5)
        )
        task1 >> task2


    @task_group(group_id='return_group')
    def tg2():  
        task3 = BashOperator(
            task_id='bitis_return',
            bash_command= f'python {abs_path}/BI_Team/HieuLT/ERP/Service/Bitis/return_bitis.py',
            dag=dag,
            retries = 1,
            retry_delay = timedelta(minutes=5)
        )


        task4 = BashOperator(
            task_id='bluestars_return',
            bash_command= f'python {abs_path}/BI_Team/HieuLT/ERP/Service/Bluestars/return_bluestars.py',
            dag=dag,
            retries = 1,
            retry_delay = timedelta(minutes=5)
        )


        task5 = BashOperator(
            task_id='hmd_return',
            bash_command= f'python {abs_path}/BI_Team/HieuLT/ERP/Service/HMD/return_hmd.py',
            dag=dag,
            retries = 1,
            retry_delay = timedelta(minutes=5)
        )


        task6 = BashOperator(
            task_id='vitox_return',
            bash_command= f'python {abs_path}/BI_Team/HieuLT/ERP/Service/Vitox/return_vitox.py',
            dag=dag,
            retries = 1,
            retry_delay = timedelta(minutes=5)
        )
        task3 >> task4 >> task5 >> task6


    task7 = BashOperator(
        task_id='storage_fee_service_companies',
        bash_command= f'python {abs_path}/BI_Team/HieuLT/ERP/Service/storage_fee_fifo_for_service_company.py',
        dag=dag,
        retries = 1,
        retry_delay = timedelta(minutes=5)
    )


    @task_group(group_id='promo_cp')
    def tg3():
        task8 = BashOperator(
            task_id='update_pro_cpn_bitis',
            bash_command= f'python {abs_path}/BI_Team/HieuLT/ERP/Service/Bitis/Bitis_update_promo_coupon.py',
            dag=dag,
            retries = 1,
            retry_delay = timedelta(minutes=5)
        )


        task9 = BashOperator(
            task_id='update_pro_cpn_bluestars',
            bash_command= f'python {abs_path}/BI_Team/HieuLT/ERP/Service/Bluestars/Bluestars_update_promo_coupon.py',
            dag=dag,
            retries = 1,
            retry_delay = timedelta(minutes=5)
        )


        task10 = BashOperator(
            task_id = 'update_pro_cpn_gu',
            bash_command= f'python {abs_path}/BI_Team/HieuLT/ERP/Service/GU/gu_update_promo_coupon.py',
            dag=dag,
            retries = 1,
            retry_delay = timedelta(minutes=5)
        )


        task11 = BashOperator(
            task_id='update_pro_cpn_hmd',
            bash_command= f'python {abs_path}/BI_Team/HieuLT/ERP/Service/HMD/HMD_update_promo_coupon.py',
            dag=dag,
            retries = 1,
            retry_delay = timedelta(minutes=5)
        )


        task12 = BashOperator(
            task_id='update_pro_cpn_vitox',
            bash_command= f'python {abs_path}/BI_Team/HieuLT/ERP/Service/Vitox/Vitox_update_promo_coupon.py',
            dag=dag,
            retries = 1,
            retry_delay = timedelta(minutes=5)
        )
        task8 >> task9 >> task10 >> task11 >> task12


    @task_group(group_id='act_ads_inv')
    def tg4():
        task13 = BashOperator(
            task_id='update_actual_ads_spends_gu',
            bash_command= f'python {abs_path}/BI_Team/HieuLT/ERP/Service/GU/GU_ads_actual_spend.py',
            dag=dag,
            retries = 1,
            retry_delay = timedelta(minutes=5)
        )


        task14 = BashOperator(
            task_id='update_actual_ads_spends_hmd',
            bash_command= f'python {abs_path}/BI_Team/HieuLT/ERP/Service/HMD/HMD_ads_actual_spend.py',
            dag=dag,
            retries = 1,
            retry_delay = timedelta(minutes=5)
        )
        task13 >> task14


    task0 >> tg1() >> tg2() >> task7 >> tg3() >> tg4()
