"""
monitoring/metrics.py
─────────────────────────────────────────────────────────────────────────────
Central Prometheus metrics module for the Sales Analysis pipeline.
Covers every DAG task:

  DAG task          Script               metrics.py call
  ──────────────    ─────────────────    ──────────────────────────────────
  etl_pip           etl_pip.py           task_timer, record_etl_rows
  product_update    product_update.py    task_timer, record_etl_rows
  daily_analysis    analysis.py          task_timer, record_db_*
  weekly_reports    weekly_llm.py        report_timer, record_report,
                                         record_db_*, record_stock_counts
  weekly_mail       mail.py              mail_timer, record_mail_sent
  monthly_reports   monthly_llm.py       report_timer, record_report,
                                         record_db_*
  monthly_mail      monthly_mail.py      mail_timer, record_mail_sent
  (cron) stock      stock.py             record_stock_fetch
  (cron) rtv        rtv_report.py        record_rtv
  (cron) wa_alert   wa_stock_alert.py    record_wa_sent, record_wa_error
  (lib)  llm        llm_recommender.py   record_llm_call

Install:  pip install prometheus-client
─────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import os
import time
import logging
import traceback
from contextlib import contextmanager

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    push_to_gateway,
)

log = logging.getLogger(__name__)

PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL", "http://localhost:9091")
JOB_NAME        = os.getenv("PROMETHEUS_JOB", "sales_pipeline")

_registry = CollectorRegistry()


# ══════════════════════════════════════════════════════════════════════════════
# ── METRIC DEFINITIONS ────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

# ── Generic — every script ────────────────────────────────────────────────────
task_runs_total = Counter(
    "task_runs_total",
    "Total task executions by script and status",
    ["script", "status"],
    registry=_registry,
)

task_duration_seconds = Gauge(
    "task_duration_seconds",
    "Wall-clock time for a full script run in seconds",
    ["script"],
    registry=_registry,
)

task_errors_total = Counter(
    "task_errors_total",
    "Unhandled exceptions by script and Python error class",
    ["script", "error_type"],
    registry=_registry,
)

# ── ETL (etl_pip.py, product_update.py) ───────────────────────────────────────
etl_rows_loaded = Gauge(
    "etl_rows_loaded",
    "Rows loaded into analytics DB per table per run",
    ["table"],
    registry=_registry,
)

etl_api_errors_total = Counter(
    "etl_api_errors_total",
    "API call failures during ETL data fetch",
    ["endpoint"],
    registry=_registry,
)

# ── DB queries (analysis, weekly_llm, monthly_llm) ────────────────────────────
db_query_duration_seconds = Gauge(
    "db_query_duration_seconds",
    "Database query duration in seconds",
    ["script", "query_label"],
    registry=_registry,
)

db_query_errors_total = Counter(
    "db_query_errors_total",
    "Database query failures with retries exhausted",
    ["script", "query_label"],
    registry=_registry,
)

db_retries_total = Counter(
    "db_retries_total",
    "Database query retry attempts triggered",
    ["script"],
    registry=_registry,
)

# ── Report generation (weekly_llm.py, monthly_llm.py) ────────────────────────
report_total = Counter(
    "report_generation_total",
    "Reports generated per store, type, and status",
    ["store", "report_type", "status"],
    registry=_registry,
)

report_duration_seconds = Gauge(
    "report_duration_seconds",
    "Time to generate one store report in seconds",
    ["store", "report_type"],
    registry=_registry,
)

stores_processed = Gauge(
    "stores_processed_total",
    "Stores processed in a report run by status",
    ["report_type", "status"],
    registry=_registry,
)

# ── LLM calls (llm_recommender.py) ───────────────────────────────────────────
llm_calls_total = Counter(
    "llm_calls_total",
    "LLM API calls by provider and status",
    ["provider", "status"],
    registry=_registry,
)

llm_duration_seconds = Gauge(
    "llm_call_duration_seconds",
    "LLM call latency in seconds",
    ["provider"],
    registry=_registry,
)

llm_fallbacks_total = Counter(
    "llm_fallbacks_total",
    "Times Groq failed and Ollama was used as fallback",
    registry=_registry,
)

llm_rate_limits_total = Counter(
    "llm_rate_limits_total",
    "Groq 429 rate-limit events",
    registry=_registry,
)

# ── Mail (mail.py, monthly_mail.py) ───────────────────────────────────────────
mail_sent_total = Counter(
    "mail_sent_total",
    "Emails sent by report type and status",
    ["report_type", "status"],
    registry=_registry,
)

mail_duration_seconds = Gauge(
    "mail_duration_seconds",
    "Time to complete a full mail distribution run in seconds",
    ["report_type"],
    registry=_registry,
)

# ── Stock & RTV (stock.py, rtv_report.py) ─────────────────────────────────────
stock_fetch_total = Counter(
    "stock_fetch_total",
    "Stock CSV fetch attempts per store and status",
    ["store", "status"],
    registry=_registry,
)

negative_stock_skus = Gauge(
    "negative_stock_skus",
    "SKUs with negative quantity (sold without GRN) per store",
    ["store"],
    registry=_registry,
)

low_stock_skus = Gauge(
    "low_stock_skus",
    "SKUs with quantity <= threshold per store",
    ["store"],
    registry=_registry,
)

rtv_lines_today = Gauge(
    "rtv_lines_today",
    "RTV return lines fetched today per store",
    ["store"],
    registry=_registry,
)

rtv_value_today = Gauge(
    "rtv_value_today_inr",
    "Total INR value of RTV returns today per store",
    ["store"],
    registry=_registry,
)

# ── WhatsApp (wa_sender.py, wa_stock_alert.py) ────────────────────────────────
wa_messages_sent_total = Counter(
    "wa_messages_sent_total",
    "WhatsApp messages sent by type and status",
    ["msg_type", "status"],
    registry=_registry,
)

wa_api_errors_total = Counter(
    "wa_api_errors_total",
    "WhatsApp API failures by message type",
    ["msg_type"],
    registry=_registry,
)


# ══════════════════════════════════════════════════════════════════════════════
# ── PUSH HELPER ───────────────────────────────────────════════════════════════
# ══════════════════════════════════════════════════════════════════════════════

def _push(grouping: dict | None = None) -> None:
    """Push all metrics to Pushgateway. Non-fatal — never breaks the pipeline."""
    try:
        push_to_gateway(
            PUSHGATEWAY_URL,
            job=JOB_NAME,
            registry=_registry,
            grouping_key=grouping or {},
        )
    except Exception as e:
        log.warning(f"[metrics] Pushgateway push failed (non-fatal): {e}")


# ══════════════════════════════════════════════════════════════════════════════
# ── PUBLIC API ────────────────────────────────────════════════════════════════
# ══════════════════════════════════════════════════════════════════════════════

# ── Generic ───────────────────────────────────────────────────────────────────

@contextmanager
def task_timer(script: str):
    """
    Times any script run and records success/failure automatically.
    Use in the __main__ block of every script.

        with task_timer("etl"):
            main()
    """
    start = time.time()
    try:
        yield
        task_runs_total.labels(script=script, status="success").inc()
    except Exception:
        task_runs_total.labels(script=script, status="failure").inc()
        raise
    finally:
        task_duration_seconds.labels(script=script).set(time.time() - start)
        _push({"script": script})


def record_task_error(script: str, error: Exception) -> None:
    """Record an unhandled exception with its Python class name."""
    error_type = type(error).__name__
    task_errors_total.labels(script=script, error_type=error_type).inc()
    log.error(f"[{script}] {error_type}: {error}\n{traceback.format_exc()}")
    _push({"script": script})


# ── ETL ───────────────────────────────────────────────────────────────────────

def record_etl_rows(table: str, row_count: int) -> None:
    """Call after loading data — table e.g. 'billing_data'."""
    etl_rows_loaded.labels(table=table).set(row_count)
    _push()


def record_etl_api_error(endpoint: str) -> None:
    """Call when an API fetch during ETL fails."""
    etl_api_errors_total.labels(endpoint=endpoint).inc()
    _push()


# ── DB queries ────────────────────────────────────────────────────────────────

def record_db_duration(script: str, query_label: str, duration: float) -> None:
    db_query_duration_seconds.labels(
        script=script, query_label=query_label[:50]
    ).set(duration)
    _push()


def record_db_error(script: str, query_label: str) -> None:
    db_query_errors_total.labels(
        script=script, query_label=query_label[:50]
    ).inc()
    _push()


def record_db_retry(script: str) -> None:
    db_retries_total.labels(script=script).inc()
    _push()


# ── Report generation ─────────────────────────────────────────────────────────

@contextmanager
def report_timer(store: str, report_type: str = "weekly"):
    """
    Times one store's report generation.

        with report_timer("East Of Kailash", "weekly"):
            generate_store_report("East Of Kailash")
    """
    start = time.time()
    try:
        yield
    finally:
        report_duration_seconds.labels(
            store=store, report_type=report_type
        ).set(time.time() - start)
        _push({"store": store})


def record_report(store: str, report_type: str, success: bool) -> None:
    status = "success" if success else "failure"
    report_total.labels(store=store, report_type=report_type, status=status).inc()
    _push({"store": store})


def record_stores_processed(report_type: str, success: int, failed: int) -> None:
    """Call once at the end of a full run with final counts."""
    stores_processed.labels(report_type=report_type, status="success").set(success)
    stores_processed.labels(report_type=report_type, status="failed").set(failed)
    _push()


# ── LLM ───────────────────────────────────────────────────────────────────────

def record_llm_call(
    provider: str,
    success: bool,
    duration_seconds: float,
    is_fallback: bool = False,
    rate_limited: bool = False,
) -> None:
    """
    Call in _get_recommendation() in llm_recommender.py.

        provider:     "groq" | "ollama"
        is_fallback:  True when Ollama used because Groq failed
        rate_limited: True when Groq returned 429
    """
    status = "success" if success else "failure"
    llm_calls_total.labels(provider=provider, status=status).inc()
    llm_duration_seconds.labels(provider=provider).set(duration_seconds)
    if is_fallback:
        llm_fallbacks_total.inc()
    if rate_limited:
        llm_rate_limits_total.inc()
    _push()


# ── Mail ──────────────────────────────────────────────────────────────────────

@contextmanager
def mail_timer(report_type: str):
    """Times a full mail distribution run."""
    start = time.time()
    try:
        yield
    finally:
        mail_duration_seconds.labels(report_type=report_type).set(
            time.time() - start
        )
        _push()


def record_mail_sent(report_type: str, success: bool) -> None:
    status = "success" if success else "failure"
    mail_sent_total.labels(report_type=report_type, status=status).inc()
    _push()


# ── Stock & RTV ───────────────────────────────────────────────────────────────

def record_stock_fetch(store: str, success: bool) -> None:
    status = "success" if success else "failure"
    stock_fetch_total.labels(store=store, status=status).inc()
    _push({"store": store})


def record_stock_counts(store: str, neg_count: int, low_count: int) -> None:
    negative_stock_skus.labels(store=store).set(neg_count)
    low_stock_skus.labels(store=store).set(low_count)
    _push({"store": store})


def record_rtv(store: str, lines: int, value_inr: float) -> None:
    rtv_lines_today.labels(store=store).set(lines)
    rtv_value_today.labels(store=store).set(value_inr)
    _push({"store": store})


# ── WhatsApp ──────────────────────────────────────────────────────────────────

def record_wa_sent(msg_type: str, success: bool) -> None:
    """msg_type: 'daily_report' | 'stock_alert' | 'weekly_report'"""
    status = "success" if success else "failure"
    wa_messages_sent_total.labels(msg_type=msg_type, status=status).inc()
    _push()


def record_wa_error(msg_type: str) -> None:
    wa_api_errors_total.labels(msg_type=msg_type).inc()
    _push()