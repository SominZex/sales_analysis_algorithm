from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import os

default_args = {
    "owner": "Somin",
    "retries": 1,
    "retry_delay": timedelta(minutes=7),
    "email": ["sominzex21@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}

# ──────────────────────────── BRANCH LOGIC ────────────────────────────

def check_weekly(**context):
    if context["logical_date"].weekday() == 0:
        return ["rtv_report", "stock"]
    return "skip_weekly"

def check_monthly(**context):
    if context["logical_date"].day == 1:
        return "monthly_reports"
    return "skip_monthly"

# ──────────────────────────── VALIDATION (CRITICAL) ────────────────────────────

def validate_dir(path):
    if not os.path.exists(path):
        raise Exception(f"{path} does not exist")

    files = os.listdir(path)

    if not files:
        raise Exception(f"No files found in {path}")

    # Optional: ensure non-empty files
    for f in files:
        full_path = os.path.join(path, f)
        if os.path.getsize(full_path) == 0:
            raise Exception(f"Empty file detected: {full_path}")

# ──────────────────────────── DAG ────────────────────────────

with DAG(
    dag_id="sales_master_pipeline",
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
    schedule="25 0 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["sales", "production", "automation"],
) as dag:

    # ──────────────────────────── DAILY ────────────────────────────

    etl = BashOperator(
        task_id="etl_pip",
        bash_command=(
            "/base/url/vmac/bin/python "
            "/base/url/etl/etl_pip.py "
        ),
    )

    product_update = BashOperator(
        task_id="product_update",
        bash_command=(
            "/base/url/vmac/bin/python "
            "/base/url/etl/product_update.py "
        ),
    )

    # ──────────────────────────── WEEKLY ────────────────────────────

    weekly_branch = BranchPythonOperator(
        task_id="check_weekly",
        python_callable=check_weekly,
    )

    rtv_report = BashOperator(
        task_id="rtv_report",
        bash_command=(
            "/base/url/vmac/bin/python "
            "/base/url/rtv_report.py "
            "--execution_date {{ ds }}"
        ),
    )

    stock = BashOperator(
        task_id="stock",
        bash_command=(
            "/base/url/vmac/bin/python "
            "/base/url/stock.py "
            "--execution_date {{ ds }}"
        ),
    )

    # 🔥 VALIDATION TASKS (REAL SYNC BARRIER)

    validate_rtv = PythonOperator(
        task_id="validate_rtv",
        python_callable=lambda: validate_dir(
            "/base/url/store_rtv"
        ),
    )

    validate_stock = PythonOperator(
        task_id="validate_stock",
        python_callable=lambda: validate_dir(
            "/base/url/store_stocks"
        ),
    )

    weekly_reports = BashOperator(
        task_id="weekly_reports",
        trigger_rule="all_success",
        bash_command=(
            "/base/url/vmac/bin/python "
            "/base/url/weekly_llm.py "
        ),
    )

    weekly_mail = BashOperator(
        task_id="weekly_mail",
        retries=2,
        retry_delay=timedelta(minutes=5),
        bash_command=(
            "/base/url/vmac/bin/python "
            "/base/url/mail.py "
        ),
    )

    skip_weekly = EmptyOperator(task_id="skip_weekly")

    # ──────────────────────────── MONTHLY ────────────────────────────

    monthly_branch = BranchPythonOperator(
        task_id="check_monthly",
        python_callable=check_monthly,
        trigger_rule="none_failed_min_one_success",
    )

    monthly_reports = BashOperator(
        task_id="monthly_reports",
        trigger_rule="none_failed_min_one_success",
        bash_command=(
            "/base/url/vmac/bin/python "
            "/base/url/monthly_llm.py "
        ),
    )

    monthly_mail = BashOperator(
        task_id="monthly_mail",
        retries=2,
        retry_delay=timedelta(minutes=5),
        trigger_rule="none_failed_min_one_success",
        bash_command=(
            "/base/url/vmac/bin/python "
            "/base/url/monthly_mail.py "
        ),
    )

    skip_monthly = EmptyOperator(task_id="skip_monthly")

    # ──────────────────────────── DEPENDENCIES ────────────────────────────

    # DAILY FLOW
    etl >> product_update >> weekly_branch

    # WEEKLY FLOW (PARALLEL + HARD VALIDATION)

    weekly_branch >> [rtv_report, stock]
    weekly_branch >> skip_weekly

    rtv_report >> validate_rtv
    stock >> validate_stock

    # TRUE SYNC POINT
    [validate_rtv, validate_stock] >> weekly_reports

    weekly_reports >> weekly_mail >> monthly_branch
    skip_weekly >> monthly_branch

    # MONTHLY FLOW
    monthly_branch >> monthly_reports >> monthly_mail
    monthly_branch >> skip_monthly