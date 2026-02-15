"""
Monthly Reports DAG - UTC Timezone
Replaces cron jobs (converted from IST to UTC):
- 50 7 1 * * monthly_reports.sh (IST 1st 7:50 AM = UTC 1st 2:20 AM)
- 20 10 1 * * monthly_mail.sh (IST 1st 10:20 AM = UTC 1st 4:50 AM)

Note: IST (India Standard Time) is UTC+5:30
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import os

# Default arguments
default_args = {
    'owner': 'azureuser',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

# Paths
ANALYSIS_DIR = '/opt/analysis'
VENV_PYTHON = f'{ANALYSIS_DIR}/vmac/bin/python'
LOG_DIR = f'{ANALYSIS_DIR}/logs'

# Create DAG
# Schedule: 20 2 1 * * (UTC 1st 2:20 AM = IST 1st 7:50 AM)
dag = DAG(
    'monthly_reports_pipeline',
    default_args=default_args,
    description='Generate and email monthly reports on 1st of each month IST',
    schedule='20 2 1 * *',  # UTC 1st 2:20 AM = IST 1st 7:50 AM
    catchup=False,
    tags=['reports', 'monthly', 'email', 'ist'],
)


def ensure_log_directory():
    """Ensure logs directory exists"""
    os.makedirs(LOG_DIR, exist_ok=True)


def run_monthly_reports():
    """
    Generate monthly reports
    """
    ensure_log_directory()
    
    script_path = f'{ANALYSIS_DIR}/monthly_reports.py'
    log_path = f'{ANALYSIS_DIR}/cron_monthly.log'
    
    with open(log_path, 'a') as log:
        log.write(f"\n{'='*50}\n")
        log.write(f"Starting monthly reports at {datetime.now()}\n")
        log.write(f"{'='*50}\n\n")
        
        result = subprocess.run(
            [VENV_PYTHON, script_path],
            stdout=log,
            stderr=subprocess.STDOUT,
            cwd=ANALYSIS_DIR
        )
        
        log.write(f"\n{'='*50}\n")
        log.write(f"Finished at {datetime.now()} with exit code: {result.returncode}\n")
        log.write(f"{'='*50}\n\n")
        
    if result.returncode != 0:
        raise Exception(f"Monthly reports generation failed with exit code {result.returncode}")
    
    return "Monthly reports generated successfully"


def send_monthly_emails():
    """
    Send monthly report emails
    """
    ensure_log_directory()
    
    script_path = f'{ANALYSIS_DIR}/monthly_mail.py'
    log_path = f'{LOG_DIR}/monthly_mail_cron.log'
    
    with open(log_path, 'a') as log:
        log.write(f"\n{'='*50}\n")
        log.write(f"Starting monthly email sender at {datetime.now()}\n")
        log.write(f"{'='*50}\n\n")
        
        result = subprocess.run(
            [VENV_PYTHON, script_path],
            stdout=log,
            stderr=subprocess.STDOUT,
            cwd=ANALYSIS_DIR
        )
        
        log.write(f"\n{'='*50}\n")
        log.write(f"Finished at {datetime.now()} with exit code: {result.returncode}\n")
        log.write(f"{'='*50}\n\n")
        
    if result.returncode != 0:
        raise Exception(f"Monthly email sending failed with exit code {result.returncode}")
    
    return "Monthly emails sent successfully"


# Task 1: Generate Monthly Reports (IST 1st 7:50 AM = UTC 1st 2:20 AM)
generate_monthly_reports = PythonOperator(
    task_id='generate_monthly_reports',
    python_callable=run_monthly_reports,
    execution_timeout=timedelta(hours=1),
    dag=dag,
)

# Task 2: Send Monthly Emails (runs after report generation)
send_monthly_emails_task = PythonOperator(
    task_id='send_monthly_emails',
    python_callable=send_monthly_emails,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

# Set task dependencies
generate_monthly_reports >> send_monthly_emails_task
