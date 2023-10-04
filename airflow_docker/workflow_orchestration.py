from airflow import DAG 
from airflow.operators.python import PythonOperator 
from datetime import datetime, timedelta
import os 

default_args = {
    'owner': 'pratyush',
    'retries': 5,
    'retry_delay': timedelta(minutes = 5)
}

dag = DAG(
    dag_id = 'main_workflow_dag_v05',
    description = 'transformation of raw bangalore air traffic data',
    start_date = datetime(2023, 10, 1),
    schedule = '@daily'
)

project_path = '/home/pratyush/bangalore_air_traffic/code'

upload_to_postgres_task = PythonOperator(
    task_id = 'upload_to_postgres',
    python_callable = lambda: os.system(f'python {project_path}/upload_to_postgres.py'),
    dag=dag
)

spark_read_postgres_task = PythonOperator(
    task_id = 'spark_read_postgres',
    python_callable = lambda: os.system(f'python {project_path}/spark_read_postgres.py'),
    dag=dag
)

transform_task = PythonOperator(
    task_id = 'transform',
    python_callable = lambda: os.system(f'python {project_path}/transform.py'),
    dag=dag
)

final_upload_task = PythonOperator(
    task_id = 'final_upload',
    python_callable = lambda: os.system(f'python {project_path}/final_upload.py'),
    dag=dag
)

upload_to_postgres_task.set_downstream(spark_read_postgres_task)
spark_read_postgres_task.set_downstream(transform_task)
transform_task.set_downstream(final_upload_task)
