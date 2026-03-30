"""
monitoring/INTEGRATION.py  (v2 — corrected to match DAG)
─────────────────────────────────────────────────────────────────────────────
Exact code to add to each script. Only additions — nothing existing changes.

DAG task_id → script → task_timer label
─────────────────────────────────────────────────────────────────────────────
  etl_pip              etl/core_pipeline.py       task_timer("etl_pip")
  product_update       etl/product_update.py      task_timer("product_update")
  run_analysis         analysis.py *              task_timer("run_analysis")
  rtv_report           rtv_report.py              task_timer("rtv_report")
  stock                stock.py                   task_timer("stock")
  weekly_reports       weekly_azure_llm.py        task_timer("weekly_reports")
  weekly_mail          mail.py                    task_timer("weekly_mail")
  monthly_reports      monthly_azure_llm.py       task_timer("monthly_reports")
  monthly_mail         monthly_mail.py            task_timer("monthly_mail")
  wa_stock_alert       wa_stock_alert.py          task_timer("wa_stock_alert")
  wa_sender            wa_sender.py               task_timer("wa_sender")

  * run_analysis DAG task runs run_analysis.sh which calls analysis.py.
    Add task_timer("run_analysis") inside analysis.py __main__.

  report_cache / report_cache_monthly — pure cache scripts, no metrics needed.
  llm_recommender.py — library, no own task_timer (inherits context from caller).
─────────────────────────────────────────────────────────────────────────────
"""

# ══════════════════════════════════════════════════════════════════════════════
# 1. etl/core_pipeline.py   (DAG task: etl_pip)
# ══════════════════════════════════════════════════════════════════════════════
"""
Add at top:
    from monitoring.metrics import task_timer, record_task_error, record_etl_rows, record_etl_api_error

Wrap __main__:
    if __name__ == "__main__":
        with task_timer("etl_pip"):
            main()

Inside main(), after each DB insert/load:
    record_etl_rows("billing_data", len(df))

On API call failures:
    record_etl_api_error("/api/endpoint-name")
"""


# ══════════════════════════════════════════════════════════════════════════════
# 2. etl/product_update.py   (DAG task: product_update)
# ══════════════════════════════════════════════════════════════════════════════
"""
Add at top:
    from monitoring.metrics import task_timer, record_task_error, record_etl_rows, record_etl_api_error

Wrap __main__:
    if __name__ == "__main__":
        with task_timer("product_update"):
            main()

Inside main(), after each DB insert/load:
    record_etl_rows("product_data", len(df))

On API call failures:
    record_etl_api_error("/api/endpoint-name")
"""


# ══════════════════════════════════════════════════════════════════════════════
# 3. analysis.py   (DAG task: run_analysis — called via run_analysis.sh)
# ══════════════════════════════════════════════════════════════════════════════
"""
Add at top:
    import time as _time
    from monitoring.metrics import task_timer, record_task_error, record_db_duration, record_db_error, record_db_retry

Wrap __main__:
    if __name__ == "__main__":
        with task_timer("run_analysis"):
            main()

Wrap safe_read_sql (or equivalent DB helper) to add timing:
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
Add at top:
    from monitoring.metrics import task_timer, record_rtv, record_task_error

After saving each store's RTV CSV, load it and record:
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
Add at top:
    from monitoring.metrics import task_timer, record_stock_fetch, record_task_error

In fetch_stock_report(), replace the status print with:
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
Add at top:
    import time as _time
    from monitoring.metrics import (
        task_timer, record_task_error,
        report_timer, record_report, record_stores_processed,
        record_db_duration, record_db_error, record_db_retry,
        record_stock_counts,
    )

Wrap safe_read_sql the same way as analysis.py above, using script="weekly_reports".

Wrap __main__ store loop:
    if __name__ == "__main__":
        store_names = get_unique_stores()
        success_count = 0
        failed_count  = 0

        with task_timer("weekly_reports"):
            for store in store_names:
                try:
                    with report_timer(store, "weekly"):       # ← ADD
                        generate_store_report(store)
                    record_report(store, "weekly", True)      # ← ADD
                    success_count += 1
                    time.sleep(1)
                except Exception as e:
                    record_report(store, "weekly", False)     # ← ADD
                    record_task_error("weekly_reports", e)    # ← ADD
                    failed_count += 1
                    print(f"❌ Error for {store}: {e}")

        # IMPORTANT: call OUTSIDE task_timer block
        record_stores_processed("weekly", success_count, failed_count)  # ← ADD

Inside generate_store_report(), after load_stock_lookups():
    stock_df = _load_stock_csv_raw(store_name)
    if not stock_df.empty:
        neg = int((stock_df["quantity"] < 0).sum())
        low = int(((stock_df["quantity"] > 0) & (stock_df["quantity"] <= LOW_STOCK_THRESHOLD)).sum())
        record_stock_counts(store_name, neg, low)              # ← ADD
"""


# ══════════════════════════════════════════════════════════════════════════════
# 7. mail.py   (DAG task: weekly_mail)
# ══════════════════════════════════════════════════════════════════════════════
"""
Add at top:
    from monitoring.metrics import task_timer, mail_timer, record_mail_sent, record_task_error

Wrap __main__:
    if __name__ == "__main__":
        with task_timer("weekly_mail"):
            with mail_timer("weekly"):
                main()

Inside the per-store/per-recipient send loop:
    try:
        send_email(...)
        record_mail_sent("weekly", True)   # ← ADD
    except Exception as e:
        record_mail_sent("weekly", False)  # ← ADD
"""


# ══════════════════════════════════════════════════════════════════════════════
# 8. monthly_azure_llm.py   (DAG task: monthly_reports)
# ══════════════════════════════════════════════════════════════════════════════
"""
Identical pattern to weekly_azure_llm.py — use report_type="monthly"
and script="monthly_reports":

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

        # IMPORTANT: call OUTSIDE task_timer block
        record_stores_processed("monthly", success_count, failed_count)
"""


# ══════════════════════════════════════════════════════════════════════════════
# 9. monthly_mail.py   (DAG task: monthly_mail)
# ══════════════════════════════════════════════════════════════════════════════
"""
Add at top:
    from monitoring.metrics import task_timer, mail_timer, record_mail_sent, record_task_error

Wrap __main__:
    if __name__ == "__main__":
        with task_timer("monthly_mail"):
            with mail_timer("monthly"):
                main()

Inside the per-store/per-recipient send loop:
    try:
        send_email(...)
        record_mail_sent("monthly", True)   # ← ADD
    except Exception as e:
        record_mail_sent("monthly", False)  # ← ADD
"""


# ══════════════════════════════════════════════════════════════════════════════
# 10. wa_stock_alert.py   (DAG task: wa_stock_alert — active)
# ══════════════════════════════════════════════════════════════════════════════
"""
Add at top:
    from monitoring.metrics import task_timer, record_wa_sent, record_wa_error, record_task_error

In send_whatsapp(), replace the return lines:
    if r.status_code == 200:
        record_wa_sent("stock_alert", True)    # ← ADD
        return True
    else:
        record_wa_sent("stock_alert", False)   # ← ADD
        record_wa_error("stock_alert")         # ← ADD
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
Add at top:
    from monitoring.metrics import task_timer, record_wa_sent, record_wa_error, record_task_error

Around each send attempt:
    try:
        send_message(...)
        record_wa_sent("daily_report", True)   # ← ADD
    except Exception as e:
        record_wa_sent("daily_report", False)  # ← ADD
        record_wa_error("daily_report")        # ← ADD

Wrap __main__:
    if __name__ == "__main__":
        with task_timer("wa_sender"):
            main()
"""


# ══════════════════════════════════════════════════════════════════════════════
# 12. llm_recommender.py   (library — no DAG task of its own)
# ══════════════════════════════════════════════════════════════════════════════
"""
Add at top:
    import time as _time
    from monitoring.metrics import record_llm_call

NO task_timer here — this is a library called from within weekly_azure_llm.py
and monthly_azure_llm.py, which already set the script context. Adding another
task_timer would overwrite _current_script and push metrics to the wrong slot.

In _get_recommendation(), wrap each LLM call with timing:

    def _get_recommendation(prompt: str) -> tuple:
        for attempt in range(2):
            try:
                t0 = _time.time()
                text = _call_groq(prompt)
                if text:
                    record_llm_call("groq", True, _time.time() - t0)   # ← ADD
                    time.sleep(2)
                    return text, False
            except Exception as e:
                err = str(e).lower()
                if "rate_limit" in err or "429" in err or "quota" in err:
                    record_llm_call("groq", False, 0, rate_limited=True)  # ← ADD
                    ...
                else:
                    record_llm_call("groq", False, 0)                     # ← ADD
                    ...
                break

        try:
            t0 = _time.time()
            text = _call_ollama(prompt)
            if text:
                record_llm_call("ollama", True, _time.time() - t0,
                                is_fallback=True)                         # ← ADD
                return text, True
        except Exception as e:
            record_llm_call("ollama", False, 0, is_fallback=True)         # ← ADD
            ...
"""