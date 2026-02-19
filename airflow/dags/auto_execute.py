from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

def check_weekly(**context):
    if context["logical_date"].weekday() == 0:
        return "weekly_reports"
    return "skip_weekly"

def check_monthly(**context):
    if context["logical_date"].day == 1:
        return "monthly_reports"
    return "skip_monthly"

with DAG(
    dag_id="sales_master_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="25 0 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["sales", "production", "automation"]
) as dag:

    # ---------------- DAILY ----------------

    etl = BashOperator(
        task_id="etl_pip",
        bash_command="/home/azureuser/etl/vmac/bin/python /home/azureuser/etl/etl_pip.py "
    )

    product_update = BashOperator(
        task_id="product_update",
        bash_command="/home/azureuser/etl/vmac/bin/python /home/azureuser/etl/product_update.py "
    )

    analysis = BashOperator(
        task_id="daily_analysis",
        bash_command="bash /home/azureuser/azure_analysis_algorithm/run_analysis.sh "
    )

    daily_whatsapp = BashOperator(
        task_id="daily_whatsapp",
        bash_command="/home/azureuser/azure_analysis_algorithm/vmac/bin/python /home/azureuser/azure_analysis_algorithm/wa_sender.py "
    )

    # ---------------- WEEKLY ----------------

    weekly_branch = BranchPythonOperator(
        task_id="check_weekly",
        python_callable=check_weekly
    )

    weekly_reports = BashOperator(
        task_id="weekly_reports",
        bash_command="/home/azureuser/azure_analysis_algorithm/vmac/bin/python /home/azureuser/azure_analysis_algorithm/weekly_reports.py "
    )

    weekly_mail = BashOperator(
        task_id="weekly_mail",
        bash_command="/home/azureuser/azure_analysis_algorithm/vmac/bin/python /home/azureuser/azure_analysis_algorithm/mail.py "
    )

    skip_weekly = EmptyOperator(task_id="skip_weekly")

    # ---------------- MONTHLY ----------------

    monthly_branch = BranchPythonOperator(
        task_id="check_monthly",
        python_callable=check_monthly
    )

    monthly_reports = BashOperator(
        task_id="monthly_reports",
        bash_command="/home/azureuser/azure_analysis_algorithm/vmac/bin/python /home/azureuser/azure_analysis_algorithm/monthly_reports.py "
    )

    monthly_mail = BashOperator(
        task_id="monthly_mail",
        bash_command="/home/azureuser/etl/vmac/bin/python /home/azureuser/azure_analysis_algorithm/monthly_mail.py "
    )

    skip_monthly = EmptyOperator(task_id="skip_monthly")

    # ---------------- DEPENDENCIES ----------------

    # Daily core chain
    etl >> product_update >> analysis >> daily_whatsapp >> weekly_branch

    # Weekly chain
    weekly_branch >> weekly_reports >> weekly_mail >> monthly_branch
    weekly_branch >> skip_weekly >> monthly_branch

    # Monthly chain
    monthly_branch >> monthly_reports >> monthly_mail
    monthly_branch >> skip_monthly