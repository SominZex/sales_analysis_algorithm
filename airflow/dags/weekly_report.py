"""
Weekly Reports DAG
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
    'weekly_reports_pipeline',
    default_args=default_args,
    description='Generate and email weekly reports every Monday IST',
    schedule='30 4 * * 1',
    catchup=False,
    tags=['reports', 'weekly', 'email', 'ist'],
)

generate_weekly_reports = BashOperator(
    task_id='generate_weekly_reports',
    bash_command='cd /opt/analysis && python3 weekly_reports.py',
    dag=dag,
)

send_weekly_emails = BashOperator(
    task_id='send_weekly_emails',
    bash_command='cd /opt/analysis && python3 mail.py',
    dag=dag,
)

generate_weekly_reports >> send_weekly_emails
