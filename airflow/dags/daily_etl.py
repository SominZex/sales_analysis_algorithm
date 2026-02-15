"""
Daily ETL and Analysis DAG - FIXED
"""
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'azureuser',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_etl_analysis_pipeline',
    default_args=default_args,
    description='Daily ETL, Analysis, and WhatsApp notification pipeline (IST timezone)',
    schedule='21 00 * * *',
    catchup=False,
    tags=['etl', 'analysis', 'daily', 'ist'],
)

etl_pipeline = BashOperator(
    task_id='run_etl_pipeline',
    bash_command='cd /opt/etl && python3 etl_pip.py',
    dag=dag,
)

product_update = BashOperator(
    task_id='update_products',
    bash_command='cd /opt/etl && python3 product_update.py',
    dag=dag,
)


etl_pipeline >> product_update

