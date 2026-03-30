"""
monitoring/metrics.py
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

FIXES (v2):
  1. GROUPING KEY BUG — ROOT CAUSE of "No Data" in Grafana.
     Pushgateway stores metrics under job + grouping_key. With the old
     _push(grouping={}) every script shared the SAME slot, so each push
     deleted every other script's metrics. Fixed: _push() now always merges
     {"script": _current_script} into the grouping key → each script gets
     its own isolated Pushgateway slot.

  2. SCRIPT CONTEXT via _set_script() / _current_script.
     task_timer() sets _current_script at entry so ALL intermediate
     record_*() calls inside the run automatically push under the correct
     key without any change at call sites.

  3. METRIC HEADER updated to reflect real DAG script names:
     weekly_azure_llm.py, monthly_azure_llm.py, etl/core_pipeline.py.
     The old header said weekly_llm.py / monthly_llm.py — those files do
     not exist in this DAG.

  4. rtv_value_today variable kept consistent with Gauge metric name
     "rtv_value_today_inr" to avoid silent metric-name mismatches.
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

# ── Current script context — set automatically by task_timer() ───────────────
_current_script: str = "unknown"


def _set_script(name: str) -> None:
    """
    Set the script context so every _push() call uses the correct grouping key.
    task_timer() calls this automatically — you never need to call it manually
    as long as __main__ is wrapped with task_timer().
    """
    global _current_script
    _current_script = name


# ══════════════════════════════════════════════════════════════════════════════
# METRIC DEFINITIONS
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

# ── ETL (etl/core_pipeline.py → etl_pip, etl/product_update.py → product_update)
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

# ── DB queries (analysis.py, weekly_azure_llm.py, monthly_azure_llm.py) ──────
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

# ── Report generation (weekly_azure_llm.py, monthly_azure_llm.py) ────────────
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

# ── LLM calls (llm_recommender.py — library called by weekly/monthly azure) ──
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

# ── Mail (mail.py → weekly_mail task | monthly_mail.py → monthly_mail task) ──
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

# ── Stock & RTV (stock.py → stock task | rtv_report.py → rtv_report task) ────
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

# Variable name matches the Prometheus metric name to avoid confusion
rtv_value_today = Gauge(
    "rtv_value_today_inr",
    "Total INR value of RTV returns today per store",
    ["store"],
    registry=_registry,
)

# ── WhatsApp (wa_stock_alert.py | wa_sender.py) ───────────────────────────────
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
# PUSH HELPER
# ══════════════════════════════════════════════════════════════════════════════

def _push(grouping: dict | None = None) -> None:
    """
    Push all metrics to Pushgateway. Non-fatal — never breaks the pipeline.

    FIX: Always merges {"script": _current_script} into the grouping key.
    Pushgateway replaces ALL metrics for a given job+grouping combination on
    every push. Without a per-script key every script wiped every other
    script's data — the root cause of "No Data" in Grafana.
    """
    key = {"script": _current_script}
    if grouping:
        key.update(grouping)
    try:
        push_to_gateway(
            PUSHGATEWAY_URL,
            job=JOB_NAME,
            registry=_registry,
            grouping_key=key,
        )
    except Exception as e:
        log.warning(f"[metrics] Pushgateway push failed (non-fatal): {e}")


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ══════════════════════════════════════════════════════════════════════════════

# ── Generic ───────────────────────────────────────────────────────────────────

@contextmanager
def task_timer(script: str):
    """
    Times any script run and records success/failure automatically.
    MUST wrap the __main__ block of every script.

    Use the DAG task_id as the script label — exact mapping:

        etl/core_pipeline.py    →  task_timer("etl_pip")
        etl/product_update.py   →  task_timer("product_update")
        analysis.py             →  task_timer("run_analysis")
        rtv_report.py           →  task_timer("rtv_report")
        stock.py                →  task_timer("stock")
        weekly_azure_llm.py     →  task_timer("weekly_reports")
        mail.py                 →  task_timer("weekly_mail")
        monthly_azure_llm.py    →  task_timer("monthly_reports")
        monthly_mail.py         →  task_timer("monthly_mail")
        wa_stock_alert.py       →  task_timer("wa_stock_alert")
        wa_sender.py            →  task_timer("wa_sender")
    """
    _set_script(script)
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
    """Call after each DB load. table e.g. 'billing_data', 'product_data'."""
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
    Used inside weekly_azure_llm.py and monthly_azure_llm.py.

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
    """
    Call ONCE after the task_timer block ends — NOT inside it.
    If called inside the loop, the per-store grouping key from the last
    record_report() push overwrites this aggregate count.
    """
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
    Call in _get_recommendation() inside llm_recommender.py.
    llm_recommender.py is a library called from within weekly_azure_llm.py
    and monthly_azure_llm.py. Those callers set the script context via
    task_timer — no separate task_timer needed inside the library itself.

        provider:     "groq" | "ollama"
        is_fallback:  True when Ollama is used because Groq failed
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
    """
    Times a full mail distribution run.
        mail.py          →  mail_timer("weekly")
        monthly_mail.py  →  mail_timer("monthly")
    """
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
    """Used in stock.py — DAG task: stock."""
    status = "success" if success else "failure"
    stock_fetch_total.labels(store=store, status=status).inc()
    _push({"store": store})


def record_stock_counts(store: str, neg_count: int, low_count: int) -> None:
    """
    Used in weekly_azure_llm.py inside generate_store_report()
    after load_stock_lookups().
    """
    negative_stock_skus.labels(store=store).set(neg_count)
    low_stock_skus.labels(store=store).set(low_count)
    _push({"store": store})


def record_rtv(store: str, lines: int, value_inr: float) -> None:
    """Used in rtv_report.py — DAG task: rtv_report."""
    rtv_lines_today.labels(store=store).set(lines)
    rtv_value_today.labels(store=store).set(value_inr)
    _push({"store": store})


# ── WhatsApp ──────────────────────────────────────────────────────────────────

def record_wa_sent(msg_type: str, success: bool) -> None:
    """
    msg_type:
        "stock_alert"   — wa_stock_alert.py  (DAG task: wa_stock_alert)
        "daily_report"  — wa_sender.py       (DAG: commented out)
    """
    status = "success" if success else "failure"
    wa_messages_sent_total.labels(msg_type=msg_type, status=status).inc()
    _push()


def record_wa_error(msg_type: str) -> None:
    wa_api_errors_total.labels(msg_type=msg_type).inc()
    _push()