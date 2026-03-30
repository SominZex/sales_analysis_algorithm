from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import os

default_args = {
    "owner": "Somin",
    "retries": 2,
    "retry_delay": timedelta(minutes=7),
    "email": ["sominzex21@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}

# ──────────────────────────── BRANCH LOGIC ────────────────────────────

def check_weekly(**context):
    # data_interval_end is the actual wall-clock date the schedule fired for
    execution_day = context["data_interval_end"].weekday()
    if execution_day == 0:
        return ["rtv_report", "stock"]
    return "skip_weekly"

def check_monthly(**context):
    if context["data_interval_end"].day == 1:
        return "report_cache_monthly"
    return "skip_monthly"

# ──────────────────────────── VALIDATION ────────────────────────────

def validate_dir(path):
    if not os.path.exists(path):
        raise Exception(f"{path} does not exist")
    files = os.listdir(path)
    if not files:
        raise Exception(f"No files found in {path}")
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
        retries=3,
        retry_delay=timedelta(minutes=5),
        bash_command=(
            "/base/dir/vmac/bin/python "
            "/base/dir/etl/core_pipeline.py "
        ),
    )

    product_update = BashOperator(
        task_id="product_update",
        retries=3,
        retry_delay=timedelta(minutes=5),
        bash_command=(
            "/base/dir/vmac/bin/python "
            "/base/dir/etl/product_update.py "
        ),
    )

    # ──────────────────────────── DAILY ANALYSIS ────────────────────────────

    run_analysis = BashOperator(
        task_id="run_analysis",
        retries=3,
        retry_delay=timedelta(minutes=5),
        bash_command=(
            "/base/dir/run_analysis.sh "
        ),
    )

    # ──────────────────────────── WEEKLY ────────────────────────────

    weekly_branch = BranchPythonOperator(
        task_id="check_weekly",
        python_callable=check_weekly,
    )

    rtv_report = BashOperator(
        task_id="rtv_report",
        retries=3,
        retry_delay=timedelta(minutes=5),
        bash_command=(
            "/base/dir/vmac/bin/python "
            "/base/dir/rtv_report.py "
            "--execution_date {{ ds }}"
        ),
    )

    stock = BashOperator(
        task_id="stock",
        retries=3,
        retry_delay=timedelta(minutes=5),
        bash_command=(
            "/base/dir/vmac/bin/python "
            "/base/dir/stock.py "
            "--execution_date {{ ds }}"
        ),
    )

    validate_rtv = PythonOperator(
        task_id="validate_rtv",
        python_callable=lambda: validate_dir(
            "/base/dir/store_rtv"
        ),
    )

    validate_stock = PythonOperator(
        task_id="validate_stock",
        python_callable=lambda: validate_dir(
            "/base/dir/store_stocks"
        ),
    )

    report_cache = BashOperator(
        task_id="report_cache",
        # Skipped upstreams (non-Friday) must not block or retry this task.
        # none_failed_min_one_success passes through on Friday (both validations
        # succeed) and is skipped cleanly on other days.
        trigger_rule="none_failed_min_one_success",
        bash_command=(
            "/base/dir/vmac/bin/python "
            "/base/dir/report_cache.py "
        ),
    )

    weekly_reports = BashOperator(
        task_id="weekly_reports",
        trigger_rule="all_success",
        bash_command=(
            "/base/dir/vmac/bin/python "
            "/base/dir/weekly_azure_llm.py "
        ),
    )

    weekly_mail = BashOperator(
        task_id="weekly_mail",
        retries=2,
        retry_delay=timedelta(minutes=5),
        bash_command=(
            "/base/dir/vmac/bin/python "
            "/base/dir/mail.py "
        ),
    )

    skip_weekly = EmptyOperator(task_id="skip_weekly")

    # ──────────────────────────── MONTHLY ────────────────────────────
    # Completely independent of the weekly branch.
    # Both weekly_branch and monthly_branch fork directly off product_update.

    monthly_branch = BranchPythonOperator(
        task_id="check_monthly",
        python_callable=check_monthly,
    )

    report_cache_monthly = BashOperator(
        task_id="report_cache_monthly",
        trigger_rule="none_failed_min_one_success",
        bash_command=(
            "/base/dir/vmac/bin/python "
            "/base/dir/report_cache_monthly.py "
        ),
    )

    monthly_reports = BashOperator(
        task_id="monthly_reports",
        trigger_rule="none_failed_min_one_success",
        bash_command=(
            "/base/dir/vmac/bin/python "
            "/base/dir/monthly_azure_llm.py "
        ),
    )

    monthly_mail = BashOperator(
        task_id="monthly_mail",
        retries=2,
        retry_delay=timedelta(minutes=5),
        trigger_rule="none_failed_min_one_success",
        bash_command=(
            "/base/dir/vmac/bin/python "
            "/base/dir/monthly_mail.py "
        ),
    )

    skip_monthly = EmptyOperator(task_id="skip_monthly")

    # ──────────────────────────── DEPENDENCIES ────────────────────────────

    # DAILY — fans out to both branches and daily analysis independently
    etl >> product_update >> [weekly_branch, monthly_branch, run_analysis]

    # WEEKLY FLOW
    weekly_branch >> [rtv_report, stock]
    weekly_branch >> skip_weekly

    rtv_report >> validate_rtv
    stock >> validate_stock

    [validate_rtv, validate_stock] >> report_cache >> weekly_reports >> weekly_mail

    # MONTHLY FLOW — zero connection to weekly
    monthly_branch >> report_cache_monthly >> monthly_reports >> monthly_mail
    monthly_branch >> skip_monthly