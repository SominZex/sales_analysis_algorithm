"""
monitoring/INTEGRATION.py
─────────────────────────────────────────────────────────────────────────────
Exact code to add to each script. Only additions — nothing existing changes.
─────────────────────────────────────────────────────────────────────────────
"""

# ══════════════════════════════════════════════════════════════════════════════
# 1. etl_pip.py  and  product_update.py
# ══════════════════════════════════════════════════════════════════════════════
"""
Add at top:
    from monitoring.metrics import task_timer, record_task_error, record_etl_rows, record_etl_api_error

Wrap __main__:
    if __name__ == "__main__":
        with task_timer("etl"):         # or "product_update"
            main()

Inside main(), after each DB insert/load, add:
    record_etl_rows("billing_data", len(df))

On API call failures, add:
    record_etl_api_error("/api/endpoint-name")
"""


# ══════════════════════════════════════════════════════════════════════════════
# 2. analysis.py  (daily_analysis task)
# ══════════════════════════════════════════════════════════════════════════════
"""
Add at top:
    import time as _time
    from monitoring.metrics import task_timer, record_task_error, record_db_duration, record_db_error, record_db_retry

Wrap __main__:
    if __name__ == "__main__":
        with task_timer("analysis"):
            main()

Wrap safe_read_sql (or equivalent) to add timing:
    def safe_read_sql(query, params=None, retries=5, delay=3):
        label = str(query)[:50].replace("\\n", " ").strip()
        for attempt in range(retries):
            try:
                t0 = _time.time()
                with engine.connect() as conn:
                    result = pd.read_sql(query, conn, params=params)
                record_db_duration("analysis", label, _time.time() - t0)  # ← ADD
                return result
            except OperationalError as e:
                record_db_error("analysis", label)                         # ← ADD
                if attempt > 0:
                    record_db_retry("analysis")                            # ← ADD
                ... (rest unchanged)
"""


# ══════════════════════════════════════════════════════════════════════════════
# 3. weekly_llm.py  (weekly_reports task)
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

Wrap safe_read_sql — same as analysis.py above, use script="weekly".

Wrap generate_store_report call in __main__:
    if __name__ == "__main__":
        store_names = get_unique_stores()
        success_count = 0
        failed_count  = 0

        with task_timer("weekly"):
            for store in store_names:
                try:
                    with report_timer(store, "weekly"):       # ← ADD
                        generate_store_report(store)
                    record_report(store, "weekly", True)      # ← ADD
                    success_count += 1
                    time.sleep(1)
                except Exception as e:
                    record_report(store, "weekly", False)     # ← ADD
                    record_task_error("weekly", e)            # ← ADD
                    failed_count += 1
                    print(f"❌ Error for {store}: {e}")

        record_stores_processed("weekly", success_count, failed_count)  # ← ADD

Inside generate_store_report(), after load_stock_lookups():
    stock_df = _load_stock_csv_raw(store_name)  # load raw df once
    if not stock_df.empty:
        neg = int((stock_df["quantity"] < 0).sum())
        low = int(((stock_df["quantity"] > 0) & (stock_df["quantity"] <= LOW_STOCK_THRESHOLD)).sum())
        record_stock_counts(store_name, neg, low)              # ← ADD
"""


# ══════════════════════════════════════════════════════════════════════════════
# 4. monthly_llm.py  (monthly_reports task)
# ══════════════════════════════════════════════════════════════════════════════
"""
Identical to weekly_llm.py above — use report_type="monthly" and script="monthly".

    with task_timer("monthly"):
        for store in store_names:
            try:
                with report_timer(store, "monthly"):
                    generate_store_report(store)
                record_report(store, "monthly", True)
                ...
            except Exception as e:
                record_report(store, "monthly", False)
                record_task_error("monthly", e)

    record_stores_processed("monthly", success_count, failed_count)
"""


# ══════════════════════════════════════════════════════════════════════════════
# 5. mail.py  (weekly_mail task)
# ══════════════════════════════════════════════════════════════════════════════
"""
Add at top:
    from monitoring.metrics import task_timer, mail_timer, record_mail_sent, record_task_error

Wrap __main__:
    if __name__ == "__main__":
        with task_timer("mail"):
            with mail_timer("weekly"):
                main()

Inside the per-store/per-recipient send loop, after each send attempt:
    try:
        send_email(...)
        record_mail_sent("weekly", True)   # ← ADD
    except Exception as e:
        record_mail_sent("weekly", False)  # ← ADD
"""


# ══════════════════════════════════════════════════════════════════════════════
# 6. monthly_mail.py  (monthly_mail task)
# ══════════════════════════════════════════════════════════════════════════════
"""
Same as mail.py — use report_type="monthly".

    with task_timer("monthly_mail"):
        with mail_timer("monthly"):
            main()

    record_mail_sent("monthly", True/False)
"""


# ══════════════════════════════════════════════════════════════════════════════
# 7. stock.py
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
# 8. rtv_report.py
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
        with task_timer("rtv"):
            main()
"""


# ══════════════════════════════════════════════════════════════════════════════
# 9. wa_sender.py  (daily WhatsApp — currently commented out in DAG)
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
# 10. wa_stock_alert.py
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
# 11. llm_recommender.py  (_get_recommendation function)
# ══════════════════════════════════════════════════════════════════════════════
"""
Add at top:
    import time as _time
    from monitoring.metrics import record_llm_call

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
                    record_llm_call("groq", False, 0,
                                    rate_limited=True)                  # ← ADD
                    ...
                else:
                    record_llm_call("groq", False, 0)                   # ← ADD
                    ...
                break

        try:
            t0 = _time.time()
            text = _call_ollama(prompt)
            if text:
                record_llm_call("ollama", True, _time.time() - t0,
                                is_fallback=True)                       # ← ADD
                return text, True
        except Exception as e:
            record_llm_call("ollama", False, 0, is_fallback=True)       # ← ADD
            ...
"""