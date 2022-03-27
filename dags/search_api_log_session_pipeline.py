from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from utils.logging import LOG_LEVEL, LOG_FILE
from services.file_serivce import move_files_from_dir

###############################################
# Parameters
###############################################
run_key = "{{run_id.split('T')[0]}}"
res_folder = '/usr/local/spark/resources'
app_folder = '/usr/local/spark/app'
target_folder = f'{res_folder}/data/sessions'
done_folder = f'{res_folder}/done'

import_items = {
    f'{res_folder}/api_log'
}
###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}
env_vars = {
    "LOG_FILE": LOG_FILE,
    "LOG_LEVEL": LOG_LEVEL
}

dag = DAG(
    dag_id="search-api-log-session-pipeline",
    description="This DAG runs a python app to download files in parallel manner, parse them, transform the data and aggregate it",
    default_args=default_args,
    schedule_interval=timedelta(1)
)


prev_task = None
for path in import_items:
    obj_name = path.split('/')[-1].split('.')[0].lower()
    task_id = f'spark_process_{obj_name}'
    spark_extract_task = SparkSubmitOperator(
        task_id=task_id,
        application=f'{app_folder}/api_log_processor.py',
        name=task_id,
        conn_id='spark_default',
        application_args=[path, target_folder],
        env_vars=env_vars,
        dag=dag)

    move_file_task = PythonOperator(
    task_id=f'move_{obj_name}',
    python_callable=move_files_from_dir,
    op_kwargs={
        'source_folder': path,
        'destination_folder': done_folder
    },
    dag=dag)

    spark_extract_task >> move_file_task

    if prev_task:
        prev_task >> spark_extract_task
    prev_task = spark_extract_task



