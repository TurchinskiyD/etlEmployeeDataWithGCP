from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 26),
    'depends_on_past': False,
    'email': ['dmturchdev@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG('employees_data',
          default_args=default_args,
          description='Runs an external Python script',
          schedule_interval='@hourly',
          catchup=False)

with dag:
    extract_data = BashOperator(
        task_id='task_extract_data',
        bash_command='python /home/airflow/gcs/dags/scripts/extract.py',
    )

    start_data_pipeline = CloudDataFusionStartPipelineOperator(
        task_id="task_start_data_pipeline",
        location="us-central1",
        pipeline_name="data_pipeline",
        instance_name="for-de-project-df",
    )

    extract_data >> start_data_pipeline



