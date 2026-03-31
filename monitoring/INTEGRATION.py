"""
monitoring/INTEGRATION.py  (v3 — final)
─────────────────────────────────────────────────────────────────────────────
Exact code to add to each script. Only additions — nothing existing changes.

═══════════════════════════════════════════════════════════════════════════════
WHY DAG RUNS DON'T CAPTURE METRICS (but manual runs do)
═══════════════════════════════════════════════════════════════════════════════
Your sys.path when Airflow's BashOperator runs a script:

    /usr/lib/python310.zip
    /usr/lib/python3.10
    /usr/lib/python3.10/lib-dynload
    /base/dir/vmac/lib/python3.10/site-packages

Notice: /base/dir is NOT in that list.
So `from monitoring.metrics import ...` raises ModuleNotFoundError before
main() runs — metrics never fire, but the script itself continues normally.

When you run manually from the project dir:
    cd /base/dir && python etl/core_pipeline.py
Python adds CWD to sys.path automatically → import succeeds → metrics work.

THE FIX: add this at the VERY TOP of every script (line 1, before everything):

    import sys; sys.path.insert(0, "/base/dir")

One-liner to add it to all scripts at once (safe — skips if already present):

    cd /base/dir
    for f in etl/core_pipeline.py etl/product_update.py analysis.py \\
              rtv_report.py stock.py weekly_azure_llm.py mail.py \\
              monthly_azure_llm.py monthly_mail.py wa_stock_alert.py \\
              wa_sender.py llm_recommender.py; do
        grep -q "azure_analysis_algorithm" "$f" || \\
        sed -i '1s/^/import sys; sys.path.insert(0, "\\/home\\/azureuser\\/azure_analysis_algorithm")\\n/' "$f"
        echo "patched $f"
    done

Also verify prometheus_client is in the vmac venv:
    /base/dir/vmac/bin/python -c "import prometheus_client"
If that errors:
    /base/dir/vmac/bin/pip install prometheus-client

═══════════════════════════════════════════════════════════════════════════════

DAG task_id → script (exact path) → task_timer label
─────────────────────────────────────────────────────────────────────────────
  etl_pip              etl/core_pipeline.py       "etl_pip"
  product_update       etl/product_update.py      "product_update"
  run_analysis         analysis.py *              "run_analysis"
  rtv_report           rtv_report.py              "rtv_report"
  stock                stock.py                   "stock"
  weekly_reports       weekly_azure_llm.py        "weekly_reports"
  weekly_mail          mail.py                    "weekly_mail"
  monthly_reports      monthly_azure_llm.py       "monthly_reports"
  monthly_mail         monthly_mail.py            "monthly_mail"
  wa_stock_alert       wa_stock_alert.py          "wa_stock_alert"
  wa_sender            wa_sender.py               "wa_sender" (DAG commented out)

  * run_analysis DAG task runs run_analysis.sh which calls analysis.py.
    Add the sys.path fix + task_timer("run_analysis") inside analysis.py.

  report_cache / report_cache_monthly — pure cache scripts, no metrics needed.
  llm_recommender.py — library, no task_timer (inherits context from caller).
─────────────────────────────────────────────────────────────────────────────
"""

PROJECT_ROOT = "/base/dir"

# ══════════════════════════════════════════════════════════════════════════════
# 1. etl/core_pipeline.py   (DAG task: etl_pip)
# ══════════════════════════════════════════════════════════════════════════════
"""
Line 1 of file:
    import sys; sys.path.insert(0, "/base/dir")

Then add imports:
    from monitoring.metrics import task_timer, record_task_error, record_etl_rows, record_etl_api_error

Wrap __main__:
    if __name__ == "__main__":
        with task_timer("etl_pip"):
            main()

After each DB insert/load inside main():
    record_etl_rows("billing_data", len(df))

On API call failures:
    record_etl_api_error("/api/endpoint-name")
"""


# ══════════════════════════════════════════════════════════════════════════════
# 2. etl/product_update.py   (DAG task: product_update)
# ══════════════════════════════════════════════════════════════════════════════
"""
Line 1 of file:
    import sys; sys.path.insert(0, "/base/dir")

Then add imports:
    from monitoring.metrics import task_timer, record_task_error, record_etl_rows, record_etl_api_error

Wrap __main__:
    if __name__ == "__main__":
        with task_timer("product_update"):
            main()

After each DB insert/load inside main():
    record_etl_rows("product_data", len(df))

On API call failures:
    record_etl_api_error("/api/endpoint-name")
"""


# ══════════════════════════════════════════════════════════════════════════════
# 3. analysis.py   (DAG task: run_analysis — called via run_analysis.sh)
# ══════════════════════════════════════════════════════════════════════════════
"""
Line 1 of file:
    import sys, time as _time; sys.path.insert(0, "/base/dir")

Then add imports:
    from monitoring.metrics import task_timer, record_task_error, record_db_duration, record_db_error, record_db_retry

Wrap __main__:
    if __name__ == "__main__":
        with task_timer("run_analysis"):
            main()

Wrap safe_read_sql (or equivalent):
    def safe_read_sql(query, params=None, retries=5, delay=3):
        label = str(query)[:50].replace("\\n", " ").strip()
        for attempt in range(retries):
            try:
                t0 = _time.time()
                with engine.connect() as conn:
                    result = pd.read_sql(query, conn, params=params)
                record_db_duration("run_analysis", label, _time.time() - t0)  # ← ADD
                return result
            except OperationalError as e:
                record_db_error("run_analysis", label)                         # ← ADD
                if attempt > 0:
                    record_db_retry("run_analysis")                            # ← ADD
                ... (rest unchanged)
"""


# ══════════════════════════════════════════════════════════════════════════════
# 4. rtv_report.py   (DAG task: rtv_report)
# ══════════════════════════════════════════════════════════════════════════════
"""
Line 1 of file:
    import sys; sys.path.insert(0, "/base/dir")

Then add imports:
    from monitoring.metrics import task_timer, record_rtv, record_task_error

After saving each store's RTV CSV:
    df = pd.read_csv(filename)
    df["totalAmount"] = pd.to_numeric(df["totalAmount"], errors="coerce").fillna(0)
    record_rtv(store_name, lines=len(df), value_inr=float(df["totalAmount"].sum()))  # ← ADD

Wrap __main__:
    if __name__ == "__main__":
        with task_timer("rtv_report"):
            main()
"""


# ══════════════════════════════════════════════════════════════════════════════
# 5. stock.py   (DAG task: stock)
# ══════════════════════════════════════════════════════════════════════════════
"""
Line 1 of file:
    import sys; sys.path.insert(0, "/base/dir")

Then add imports:
    from monitoring.metrics import task_timer, record_stock_fetch, record_task_error

In fetch_stock_report():
    if r.status_code == 200:
        ...save file...
        record_stock_fetch(store_name, True)   # ← ADD
    else:
        record_stock_fetch(store_name, False)  # ← ADD

Wrap __main__:
    if __name__ == "__main__":
        with task_timer("stock"):
            main()
"""


# ══════════════════════════════════════════════════════════════════════════════
# 6. weekly_azure_llm.py   (DAG task: weekly_reports)
# ══════════════════════════════════════════════════════════════════════════════
"""
Line 1 of file:
    import sys, time as _time; sys.path.insert(0, "/base/dir")

Then add imports:
    from monitoring.metrics import (
        task_timer, record_task_error,
        report_timer, record_report, record_stores_processed,
        record_db_duration, record_db_error, record_db_retry,
        record_stock_counts,
    )

Wrap safe_read_sql same as analysis.py, using script="weekly_reports".

Wrap __main__:
    if __name__ == "__main__":
        store_names = get_unique_stores()
        success_count = 0
        failed_count  = 0

        with task_timer("weekly_reports"):
            for store in store_names:
                try:
                    with report_timer(store, "weekly"):
                        generate_store_report(store)
                    record_report(store, "weekly", True)
                    success_count += 1
                    time.sleep(1)
                except Exception as e:
                    record_report(store, "weekly", False)
                    record_task_error("weekly_reports", e)
                    failed_count += 1
                    print(f"❌ Error for {store}: {e}")

        # OUTSIDE task_timer block:
        record_stores_processed("weekly", success_count, failed_count)

Inside generate_store_report(), after load_stock_lookups():
    stock_df = _load_stock_csv_raw(store_name)
    if not stock_df.empty:
        neg = int((stock_df["quantity"] < 0).sum())
        low = int(((stock_df["quantity"] > 0) & (stock_df["quantity"] <= LOW_STOCK_THRESHOLD)).sum())
        record_stock_counts(store_name, neg, low)
"""


# ══════════════════════════════════════════════════════════════════════════════
# 7. mail.py   (DAG task: weekly_mail)
# ══════════════════════════════════════════════════════════════════════════════
"""
Line 1 of file:
    import sys; sys.path.insert(0, "/base/dir")

Then add imports:
    from monitoring.metrics import task_timer, mail_timer, record_mail_sent, record_task_error

Wrap __main__:
    if __name__ == "__main__":
        with task_timer("weekly_mail"):
            with mail_timer("weekly"):
                main()

Per-recipient send loop:
    try:
        send_email(...)
        record_mail_sent("weekly", True)
    except Exception as e:
        record_mail_sent("weekly", False)
"""


# ══════════════════════════════════════════════════════════════════════════════
# 8. monthly_azure_llm.py   (DAG task: monthly_reports)
# ══════════════════════════════════════════════════════════════════════════════
"""
Line 1 of file:
    import sys, time as _time; sys.path.insert(0, "/base/dir")

Then add imports:
    from monitoring.metrics import (
        task_timer, record_task_error,
        report_timer, record_report, record_stores_processed,
        record_db_duration, record_db_error, record_db_retry,
    )

Wrap __main__:
    if __name__ == "__main__":
        store_names = get_unique_stores()
        success_count = 0
        failed_count  = 0

        with task_timer("monthly_reports"):
            for store in store_names:
                try:
                    with report_timer(store, "monthly"):
                        generate_store_report(store)
                    record_report(store, "monthly", True)
                    success_count += 1
                    time.sleep(1)
                except Exception as e:
                    record_report(store, "monthly", False)
                    record_task_error("monthly_reports", e)
                    failed_count += 1
                    print(f"❌ Error for {store}: {e}")

        # OUTSIDE task_timer block:
        record_stores_processed("monthly", success_count, failed_count)
"""


# ══════════════════════════════════════════════════════════════════════════════
# 9. monthly_mail.py   (DAG task: monthly_mail)
# ══════════════════════════════════════════════════════════════════════════════
"""
Line 1 of file:
    import sys; sys.path.insert(0, "/base/dir")

Then add imports:
    from monitoring.metrics import task_timer, mail_timer, record_mail_sent, record_task_error

Wrap __main__:
    if __name__ == "__main__":
        with task_timer("monthly_mail"):
            with mail_timer("monthly"):
                main()

Per-recipient send loop:
    try:
        send_email(...)
        record_mail_sent("monthly", True)
    except Exception as e:
        record_mail_sent("monthly", False)
"""


# ══════════════════════════════════════════════════════════════════════════════
# 10. wa_stock_alert.py   (DAG task: wa_stock_alert)
# ══════════════════════════════════════════════════════════════════════════════
"""
Line 1 of file:
    import sys; sys.path.insert(0, "/base/dir")

Then add imports:
    from monitoring.metrics import task_timer, record_wa_sent, record_wa_error, record_task_error

In send_whatsapp():
    if r.status_code == 200:
        record_wa_sent("stock_alert", True)
        return True
    else:
        record_wa_sent("stock_alert", False)
        record_wa_error("stock_alert")
        return False

Wrap __main__:
    if __name__ == "__main__":
        with task_timer("wa_stock_alert"):
            main()
"""


# ══════════════════════════════════════════════════════════════════════════════
# 11. wa_sender.py   (DAG task: currently commented out)
# ══════════════════════════════════════════════════════════════════════════════
"""
Line 1 of file:
    import sys; sys.path.insert(0, "/base/dir")

Then add imports:
    from monitoring.metrics import task_timer, record_wa_sent, record_wa_error, record_task_error

Per-send attempt:
    try:
        send_message(...)
        record_wa_sent("daily_report", True)
    except Exception as e:
        record_wa_sent("daily_report", False)
        record_wa_error("daily_report")

Wrap __main__:
    if __name__ == "__main__":
        with task_timer("wa_sender"):
            main()
"""


# ══════════════════════════════════════════════════════════════════════════════
# 12. llm_recommender.py   (library — no DAG task of its own)
# ══════════════════════════════════════════════════════════════════════════════
"""
Line 1 of file:
    import sys, time as _time; sys.path.insert(0, "/base/dir")

Then add imports:
    from monitoring.metrics import record_llm_call

NO task_timer here — this is a library called from weekly_azure_llm.py and
monthly_azure_llm.py which already set the script context. Adding task_timer
here would overwrite _current_script mid-run and push to the wrong slot.

In _get_recommendation():
    def _get_recommendation(prompt: str) -> tuple:
        for attempt in range(2):
            try:
                t0 = _time.time()
                text = _call_groq(prompt)
                if text:
                    record_llm_call("groq", True, _time.time() - t0)
                    time.sleep(2)
                    return text, False
            except Exception as e:
                err = str(e).lower()
                if "rate_limit" in err or "429" in err or "quota" in err:
                    record_llm_call("groq", False, 0, rate_limited=True)
                else:
                    record_llm_call("groq", False, 0)
                break

        try:
            t0 = _time.time()
            text = _call_ollama(prompt)
            if text:
                record_llm_call("ollama", True, _time.time() - t0, is_fallback=True)
                return text, True
        except Exception as e:
            record_llm_call("ollama", False, 0, is_fallback=True)
"""