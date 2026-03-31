"""
monitoring/metrics.py  (v3 — final)
─────────────────────────────────────────────────────────────────────────────
Central Prometheus metrics module for the Sales Analysis pipeline.

DAG task_id          Script (exact path)              task_timer label
─────────────────────────────────────────────────────────────────────────────
etl_pip              etl/core_pipeline.py             "etl_pip"
product_update       etl/product_update.py            "product_update"
run_analysis         run_analysis.sh → analysis.py    "run_analysis"
rtv_report           rtv_report.py                    "rtv_report"
stock                stock.py                         "stock"
report_cache         report_cache.py                  (no metrics — pure cache)
weekly_reports       weekly_azure_llm.py              "weekly_reports"
weekly_mail          mail.py                          "weekly_mail"
report_cache_monthly report_cache_monthly.py          (no metrics — pure cache)
monthly_reports      monthly_azure_llm.py             "monthly_reports"
monthly_mail         monthly_mail.py                  "monthly_mail"
(lib)  llm           llm_recommender.py               (library — no own timer)
wa_stock_alert       wa_stock_alert.py                "wa_stock_alert"
wa_sender            wa_sender.py                     "wa_sender" (DAG commented out)

FIXES:
  1. GROUPING KEY BUG — every _push() now includes {"script": _current_script}
     so each script writes to its own Pushgateway slot. Previously all scripts
     shared the same slot and each push wiped every other script's metrics.

  2. SCRIPT CONTEXT — task_timer() sets _current_script at entry so all
     intermediate record_*() calls push with the correct grouping key.

  3. SAFE IMPORT — prometheus_client import is wrapped in try/except so a
     missing package (e.g. wrong venv on DAG run) makes all calls no-ops
     instead of crashing the pipeline.

  4. rtv_value_today variable name consistent with Gauge metric name
     "rtv_value_today_inr".
─────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import os
import time
import logging
import traceback
from contextlib import contextmanager

log = logging.getLogger(__name__)

# ── Safe prometheus_client import ────────────────────────────────────────────
# If not installed in the active venv (common when DAG uses a different Python
# than the one you tested with manually), all metric calls become silent no-ops
# so the pipeline never crashes due to a missing monitoring dependency.
try:
    from prometheus_client import CollectorRegistry, Counter, Gauge, push_to_gateway
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    log.warning(
        "[metrics] prometheus_client not installed — metrics are no-ops. "
        "Fix: /base/dir/vmac/bin/pip install prometheus-client"
    )
    _PROMETHEUS_AVAILABLE = False

PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL", "http://localhost:9091")
JOB_NAME        = os.getenv("PROMETHEUS_JOB", "sales_pipeline")

_registry       = CollectorRegistry() if _PROMETHEUS_AVAILABLE else None
_current_script: str = "unknown"


def _set_script(name: str) -> None:
    global _current_script
    _current_script = name


# ══════════════════════════════════════════════════════════════════════════════
# METRIC DEFINITIONS
# ══════════════════════════════════════════════════════════════════════════════

if _PROMETHEUS_AVAILABLE:

    task_runs_total = Counter(
        "task_runs_total",
        "Total task executions by script and status",
        ["script", "status"], registry=_registry,
    )
    task_duration_seconds = Gauge(
        "task_duration_seconds",
        "Wall-clock time for a full script run in seconds",
        ["script"], registry=_registry,
    )
    task_errors_total = Counter(
        "task_errors_total",
        "Unhandled exceptions by script and error class",
        ["script", "error_type"], registry=_registry,
    )

    # ETL
    etl_rows_loaded = Gauge(
        "etl_rows_loaded", "Rows loaded into analytics DB per table per run",
        ["table"], registry=_registry,
    )
    etl_api_errors_total = Counter(
        "etl_api_errors_total", "API call failures during ETL data fetch",
        ["endpoint"], registry=_registry,
    )

    # DB queries
    db_query_duration_seconds = Gauge(
        "db_query_duration_seconds", "Database query duration in seconds",
        ["script", "query_label"], registry=_registry,
    )
    db_query_errors_total = Counter(
        "db_query_errors_total", "Database query failures with retries exhausted",
        ["script", "query_label"], registry=_registry,
    )
    db_retries_total = Counter(
        "db_retries_total", "Database query retry attempts triggered",
        ["script"], registry=_registry,
    )

    # Report generation
    report_total = Counter(
        "report_generation_total", "Reports generated per store, type, and status",
        ["store", "report_type", "status"], registry=_registry,
    )
    report_duration_seconds = Gauge(
        "report_duration_seconds", "Time to generate one store report in seconds",
        ["store", "report_type"], registry=_registry,
    )
    stores_processed = Gauge(
        "stores_processed_total", "Stores processed in a report run by status",
        ["report_type", "status"], registry=_registry,
    )

    # LLM
    llm_calls_total = Counter(
        "llm_calls_total", "LLM API calls by provider and status",
        ["provider", "status"], registry=_registry,
    )
    llm_duration_seconds = Gauge(
        "llm_call_duration_seconds", "LLM call latency in seconds",
        ["provider"], registry=_registry,
    )
    llm_fallbacks_total = Counter(
        "llm_fallbacks_total", "Times Groq failed and Ollama was used as fallback",
        registry=_registry,
    )
    llm_rate_limits_total = Counter(
        "llm_rate_limits_total", "Groq 429 rate-limit events",
        registry=_registry,
    )

    # Mail
    mail_sent_total = Counter(
        "mail_sent_total", "Emails sent by report type and status",
        ["report_type", "status"], registry=_registry,
    )
    mail_duration_seconds = Gauge(
        "mail_duration_seconds", "Time to complete a full mail run in seconds",
        ["report_type"], registry=_registry,
    )

    # Stock & RTV
    stock_fetch_total = Counter(
        "stock_fetch_total", "Stock CSV fetch attempts per store and status",
        ["store", "status"], registry=_registry,
    )
    negative_stock_skus = Gauge(
        "negative_stock_skus", "SKUs with negative quantity per store",
        ["store"], registry=_registry,
    )
    low_stock_skus = Gauge(
        "low_stock_skus", "SKUs with quantity <= threshold per store",
        ["store"], registry=_registry,
    )
    rtv_lines_today = Gauge(
        "rtv_lines_today", "RTV return lines fetched today per store",
        ["store"], registry=_registry,
    )
    rtv_value_today = Gauge(
        "rtv_value_today_inr", "Total INR value of RTV returns today per store",
        ["store"], registry=_registry,
    )

    # WhatsApp
    wa_messages_sent_total = Counter(
        "wa_messages_sent_total", "WhatsApp messages sent by type and status",
        ["msg_type", "status"], registry=_registry,
    )
    wa_api_errors_total = Counter(
        "wa_api_errors_total", "WhatsApp API failures by message type",
        ["msg_type"], registry=_registry,
    )


# ══════════════════════════════════════════════════════════════════════════════
# PUSH HELPER
# ══════════════════════════════════════════════════════════════════════════════

def _push(grouping: dict | None = None) -> None:
    """Push all metrics to Pushgateway. Non-fatal — never breaks the pipeline.
    Always includes {"script": _current_script} so each script has its own
    isolated Pushgateway slot and cannot overwrite another script's metrics."""
    if not _PROMETHEUS_AVAILABLE:
        return
    key = {"script": _current_script}
    if grouping:
        key.update(grouping)
    try:
        push_to_gateway(PUSHGATEWAY_URL, job=JOB_NAME, registry=_registry, grouping_key=key)
    except Exception as e:
        log.warning(f"[metrics] Pushgateway push failed (non-fatal): {e}")


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ══════════════════════════════════════════════════════════════════════════════

@contextmanager
def task_timer(script: str):
    """Times a script run and records success/failure. Wrap every __main__."""
    _set_script(script)
    start = time.time()
    try:
        yield
        if _PROMETHEUS_AVAILABLE:
            task_runs_total.labels(script=script, status="success").inc()
    except Exception:
        if _PROMETHEUS_AVAILABLE:
            task_runs_total.labels(script=script, status="failure").inc()
        raise
    finally:
        if _PROMETHEUS_AVAILABLE:
            task_duration_seconds.labels(script=script).set(time.time() - start)
        _push({"script": script})


def record_task_error(script: str, error: Exception) -> None:
    if not _PROMETHEUS_AVAILABLE:
        return
    task_errors_total.labels(script=script, error_type=type(error).__name__).inc()
    log.error(f"[{script}] {type(error).__name__}: {error}\n{traceback.format_exc()}")
    _push({"script": script})


def record_etl_rows(table: str, row_count: int) -> None:
    if not _PROMETHEUS_AVAILABLE:
        return
    etl_rows_loaded.labels(table=table).set(row_count)
    _push()


def record_etl_api_error(endpoint: str) -> None:
    if not _PROMETHEUS_AVAILABLE:
        return
    etl_api_errors_total.labels(endpoint=endpoint).inc()
    _push()


def record_db_duration(script: str, query_label: str, duration: float) -> None:
    if not _PROMETHEUS_AVAILABLE:
        return
    db_query_duration_seconds.labels(script=script, query_label=query_label[:50]).set(duration)
    _push()


def record_db_error(script: str, query_label: str) -> None:
    if not _PROMETHEUS_AVAILABLE:
        return
    db_query_errors_total.labels(script=script, query_label=query_label[:50]).inc()
    _push()


def record_db_retry(script: str) -> None:
    if not _PROMETHEUS_AVAILABLE:
        return
    db_retries_total.labels(script=script).inc()
    _push()


@contextmanager
def report_timer(store: str, report_type: str = "weekly"):
    """Times one store's report generation."""
    start = time.time()
    try:
        yield
    finally:
        if _PROMETHEUS_AVAILABLE:
            report_duration_seconds.labels(store=store, report_type=report_type).set(time.time() - start)
        _push({"store": store})


def record_report(store: str, report_type: str, success: bool) -> None:
    if not _PROMETHEUS_AVAILABLE:
        return
    status = "success" if success else "failure"
    report_total.labels(store=store, report_type=report_type, status=status).inc()
    _push({"store": store})


def record_stores_processed(report_type: str, success: int, failed: int) -> None:
    """Call ONCE after the task_timer block — NOT inside it."""
    if not _PROMETHEUS_AVAILABLE:
        return
    stores_processed.labels(report_type=report_type, status="success").set(success)
    stores_processed.labels(report_type=report_type, status="failed").set(failed)
    _push()


def record_llm_call(
    provider: str, success: bool, duration_seconds: float,
    is_fallback: bool = False, rate_limited: bool = False,
) -> None:
    if not _PROMETHEUS_AVAILABLE:
        return
    llm_calls_total.labels(provider=provider, status="success" if success else "failure").inc()
    llm_duration_seconds.labels(provider=provider).set(duration_seconds)
    if is_fallback:
        llm_fallbacks_total.inc()
    if rate_limited:
        llm_rate_limits_total.inc()
    _push()


@contextmanager
def mail_timer(report_type: str):
    """Times a full mail distribution run."""
    start = time.time()
    try:
        yield
    finally:
        if _PROMETHEUS_AVAILABLE:
            mail_duration_seconds.labels(report_type=report_type).set(time.time() - start)
        _push()


def record_mail_sent(report_type: str, success: bool) -> None:
    if not _PROMETHEUS_AVAILABLE:
        return
    mail_sent_total.labels(report_type=report_type, status="success" if success else "failure").inc()
    _push()


def record_stock_fetch(store: str, success: bool) -> None:
    if not _PROMETHEUS_AVAILABLE:
        return
    stock_fetch_total.labels(store=store, status="success" if success else "failure").inc()
    _push({"store": store})


def record_stock_counts(store: str, neg_count: int, low_count: int) -> None:
    if not _PROMETHEUS_AVAILABLE:
        return
    negative_stock_skus.labels(store=store).set(neg_count)
    low_stock_skus.labels(store=store).set(low_count)
    _push({"store": store})


def record_rtv(store: str, lines: int, value_inr: float) -> None:
    if not _PROMETHEUS_AVAILABLE:
        return
    rtv_lines_today.labels(store=store).set(lines)
    rtv_value_today.labels(store=store).set(value_inr)
    _push({"store": store})


def record_wa_sent(msg_type: str, success: bool) -> None:
    if not _PROMETHEUS_AVAILABLE:
        return
    wa_messages_sent_total.labels(msg_type=msg_type, status="success" if success else "failure").inc()
    _push()


def record_wa_error(msg_type: str) -> None:
    if not _PROMETHEUS_AVAILABLE:
        return
    wa_api_errors_total.labels(msg_type=msg_type).inc()
    _push()