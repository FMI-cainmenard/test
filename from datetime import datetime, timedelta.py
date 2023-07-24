from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.email import send_email
import os
import configparser

default_args = {
    'owner': 'FMI',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 26),
    'email_on_failure': False, # Keep this false to use custom email functions
    'email_on_retry': False,
    'email': ['jericho.cruz@fmicorp.com', 'cain.menard@fmicorp.com', 'irenka.hammell@fmicorp.com', 'Rondi.Brock@fmicorp.com', 'Denise.Danisewich@fmicorp.com', 'jake.howlett@fmicorp.com'],
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

def check_s3_for_dcf():
    import boto3
    import configparser
    config = configparser.ConfigParser()
    config.read('/opt/airflow/extraction/configuration.conf')
    bucket_name = config.get('aws', 's3_bucket')
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    datasets = bucket.objects.filter(Prefix='data_capture_forms/')
    valid_extensions = ['.xlsm', '.xlsx']
    xl_objects = [obj.key for obj in datasets if any(obj.key.endswith(ext) for ext in valid_extensions)]
    if len(xl_objects) > 0:
        print(f"Found {len(xl_objects)} Excel objects (.xlsm and .xlsx) in S3:")
        for obj_key in xl_objects:
            print(obj_key)
        return True
    else:
        print("No Excel objects (.xlsm and .xlsx) found in S3")
        return False
    
def notify_email_excel_fsa_financial_cast_extract(contextDict):
    task_id = contextDict['task_instance'].task_id
    email_list = contextDict['task_instance'].task.dag.default_args['email']
    title = f"Airflow Failure: Task {task_id}"
    log_url = contextDict.get('task_instance').log_url
    exception = contextDict.get('exception')
    body = f"""<b>This is an automated message.</b> <br><br>

There has been an error in task: {task_id} <br>
<b>*** Please check all recent FSA DCFs for data validation and try again. ***</b> <br><br>

Exception: {exception} <br>
For log details, <a href="{log_url}">click here</a> <br><br>

As this is an automated process, no reply is necessary. <br>
For any concerns, please contact the Airflow System Administrator."""
    
    send_email(to=email_list, subject=title, html_content=body)

def notify_email(contextDict):
    task_id = contextDict['task_instance'].task_id
    email_list = contextDict['task_instance'].task.dag.default_args['email']
    title = f"Airflow Failure: Task {task_id}"
    log_url = contextDict.get('task_instance').log_url
    exception = contextDict.get('exception')
    body = f"""<b>This is an automated message.</b> <br><br>

There has been an error in task: {task_id}. <br><br>

Exception: {exception} <br>
For log details, <a href="{log_url}">click here</a>. <br><br>

As this is an automated process, no reply is necessary. <br>
For any concerns, please contact the Airflow System Administrator."""


with DAG('fsa_dcf_workflow', 
         default_args=default_args, 
         schedule_interval='0 0,6,12,18 * * *',
         is_paused_upon_creation=True,
         catchup=False) as dag:

    check_s3_for_dcf_task = PythonOperator(
        task_id='check_s3_for_dcf_task',
        python_callable=check_s3_for_dcf,
    )

    short_circuit = ShortCircuitOperator(
        task_id='short_circuit',
        python_callable=check_s3_for_dcf,
    )

    empty_task = BashOperator(
        task_id='empty_task',
        bash_command='echo "Short-circuit condition met. No downstream tasks to run."',
    )

    backup_command = 'python /opt/airflow/extraction/backup-mysql-tables.py'

    backup_tables = BashOperator(
        task_id='backup_tables',
        bash_command=backup_command,
        on_failure_callback=notify_email,
    )

    insights_fsa_main_extract = BashOperator(
        task_id='insights_fsa_main_extract',
        bash_command='python /opt/airflow/extraction/insights-fsa-main-extract.py',
        trigger_rule='all_done',
        on_failure_callback=notify_email,
    )

    excel_fsa_financial_cast_extract = BashOperator(
        task_id='excel_fsa_financial_cast_extract',
        bash_command='python /opt/airflow/extraction/excel-fsa-financial-cast-extract.py',
        trigger_rule='all_done',
        on_failure_callback=notify_email_excel_fsa_financial_cast_extract,
    )

    fsa_append = BashOperator(
        task_id='fsa_append',
        bash_command='python /opt/airflow/extraction/fsa-append.py',
        trigger_rule='all_done',
        on_failure_callback=notify_email,
    )

    load_to_mysql = BashOperator(
        task_id='load_to_mysql',
        bash_command='python /opt/airflow/extraction/load-to-mysql.py',
        trigger_rule='all_done',
        on_failure_callback=notify_email,
    )

    # set task dependencies
    check_s3_for_dcf_task >> short_circuit >> empty_task
    short_circuit >> backup_tables >> insights_fsa_main_extract >> excel_fsa_financial_cast_extract >> fsa_append >> load_to_mysql

    # set configuration path for scripts
    os.environ['CONFIG_PATH'] = '/opt/airflow/extraction/configuration.conf'