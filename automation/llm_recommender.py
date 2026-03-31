import sys; sys.path.insert(0, "/base/dir")
import os
import json
import time
import requests
import pandas as pd
import numpy as np
from groq import Groq
from dotenv import load_dotenv
from sqlalchemy.engine import Engine

load_dotenv()

GROQ_MODEL   = os.getenv("GROQ_MODEL",   "openai/gpt-oss-120b")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:3b")
OLLAMA_HOST  = os.getenv("OLLAMA_HOST",  "http://localhost:11434")

_client = None

def _get_client() -> Groq:
    global _client
    if _client is None:
        api_key = os.getenv("GROQ_API_KEY")
        if not api_key:
            raise RuntimeError("GROQ_API_KEY not set.")
        _client = Groq(api_key=api_key)
    return _client


#wrapper
_UNAVAILABLE = """
<div style="background:#f0f7ff;border-left:5px solid #0078d7;border-radius:6px;
            padding:14px 18px;margin:10px 0 24px 0;font-family:'Segoe UI',sans-serif;">
  <div style="font-size:15px;font-weight:bold;color:#0078d7;margin-bottom:8px;">
    New Shop AI Recommendation \u2014 Unavailable
  </div>
  <div style="font-size:14px;color:#888;">
    Could not generate recommendation. Check GROQ_API_KEY in .env and verify the GROQ_MODEL is correct.
  </div>
</div>
"""

_SECTION_COLORS = ["#0078d7", "#e67e22", "#27ae60", "#8e44ad", "#c0392b"]

_SECTION_ICONS = {
    "health":    "&#127973;",
    "revenue":   "&#128200;",
    "peak":      "&#128293;",
    "inventory": "&#128230;",
    "margin":    "&#128185;",
}

def _section_icon(title):
    t = title.lower()
    for key, icon in _SECTION_ICONS.items():
        if key in t:
            return icon
    return "&#128204;"


def _md_inline(text):
    import re
    text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)
    text = re.sub(r"\*(.+?)\*",     r"<em>\1</em>",         text)
    return text


def _parse_sections(text):
    import re
    text = re.sub(r"\*\*(\d+\.\s)", r"\1", text)
    text = re.sub(r"(\d+\.\s[^\n]*)\*\*", r"\1", text)
    section_re = re.compile(r"^(\d+)\.\s+(.+)$", re.MULTILINE)
    matches = list(section_re.finditer(text))
    if not matches:
        return [("Recommendation", [text.strip()])]
    sections = []
    for i, m in enumerate(matches):
        title = m.group(2).strip().rstrip(":")
        start = m.end()
        end   = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body  = text[start:end].strip()
        bullets = []
        for line in re.split(r"\n", body):
            line = line.strip().lstrip("-\u2022\u2013").strip()
            if line:
                bullets.append(line)
        sections.append((title, bullets))
    return sections



# llm call

def _call_groq(prompt: str) -> str:
    client = _get_client()
    chat_completion = client.chat.completions.create(
        messages=[
            {
                "role": "system",
                "content": "You are a retail sales analyst. Write concise bullet point recommendations for a store manager based on the data provided.",
            },
            {"role": "user", "content": prompt},
        ],
        model=GROQ_MODEL,
        temperature=0.2,
        max_tokens=5000,
    )
    return chat_completion.choices[0].message.content.strip()


def _call_ollama(prompt: str) -> str:
    system = "You are a retail sales analyst. Write concise bullet point recommendations for a store manager based on the data provided."
    full_prompt = f"[INST] <<SYS>>\n{system}\n<</SYS>>\n\n{prompt} [/INST]"
    resp = requests.post(
        f"{OLLAMA_HOST}/api/generate",
        json={"model": OLLAMA_MODEL, "prompt": full_prompt, "stream": False,
              "options": {"temperature": 0.2, "num_predict": 500}},
        timeout=300,
    )
    resp.raise_for_status()
    return resp.json().get("response", "").strip()


def _get_recommendation(prompt: str) -> tuple:
    for attempt in range(2):
        try:
            text = _call_groq(prompt)
            if text:
                print(f"Groq ({GROQ_MODEL}) response received.")
                time.sleep(2)
                return text, False
        except Exception as e:
            err = str(e).lower()
            if "rate_limit" in err or "429" in err or "quota" in err:
                if attempt == 0:
                    print("Groq rate limit — waiting 30s then retrying...")
                    time.sleep(30)
                    continue
                else:
                    print(f"Groq rate limit persists after retry — skipping recommendation.")
            else:
                print(f"Groq failed: {str(e)[:80]} — skipping recommendation.")
            break
    return "", False


def _wrap_html(text: str, used_fallback: bool = False) -> str:
    if not text:
        return _UNAVAILABLE

    sections = _parse_sections(text)
    cards_html = []

    for idx, (title, bullets) in enumerate(sections):
        color = _SECTION_COLORS[idx % len(_SECTION_COLORS)]
        icon  = _section_icon(title)

        bullet_rows = ""
        for b in bullets:
            bullet_rows += (
                f'<div style="display:flex;gap:8px;margin:5px 0;align-items:flex-start;">'
                f'<span style="color:{color};margin-top:2px;flex-shrink:0;">&#9656;</span>'
                f'<span style="color:#333;font-size:13.5px;line-height:1.6;">{_md_inline(b)}</span>'
                f'</div>'
            )

        cards_html.append(
            f'<div style="background:#fff;border-top:3px solid {color};border-radius:8px;'
            f'padding:12px 16px;margin:0 0 12px 0;box-shadow:0 1px 4px rgba(0,0,0,0.07);">'
            f'<div style="font-size:13px;font-weight:700;color:{color};'
            f'text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px;">'
            f'{icon}&nbsp;&nbsp;{title}</div>'
            f'{bullet_rows}'
            f'</div>'
        )

    return (
        '<div style="background:#f4f7fb;border-radius:10px;padding:16px 18px;'
        'margin:10px 0 24px 0;font-family:\'Segoe UI\',sans-serif;">'
        '<div style="font-size:14px;font-weight:700;color:#0078d7;'
        'margin-bottom:12px;letter-spacing:0.3px;">&#129302;&nbsp; New Shop intelligence insights</div>'
        + "".join(cards_html)
        + '</div>'
    )

# ── WEEKLY SNAPSHOT

def save_weekly_snapshot(
    store_name: str,
    week_start,
    brand_df: pd.DataFrame,
    category_df: pd.DataFrame,
    product_df: pd.DataFrame,
    engine: Engine,
) -> None:
    try:
        from sqlalchemy import text

        def _upsert(df, table, name_col, db_name_col):
            if df.empty:
                return
            rows = []
            for _, row in df.iterrows():
                rows.append({
                    "storename":       store_name,
                    db_name_col:       str(row.get(name_col, "")),
                    "week_start":      str(week_start),
                    "total_sales":     float(row.get("total_sales", 0) or 0),
                    "quantity_sold":   int(row.get("quantity_sold", 0) or 0),
                    "profit_margin":   float(str(row.get("profit_margin", 0)).replace("%", "") or 0),
                    "contrib_percent": float(str(row.get("contrib_percent", 0)).replace("%", "") or 0),
                })
            upsert_sql = text(f"""
                INSERT INTO {table}
                    (storename, {db_name_col}, week_start, total_sales, quantity_sold, profit_margin, contrib_percent)
                VALUES
                    (:storename, :{db_name_col}, :week_start, :total_sales, :quantity_sold, :profit_margin, :contrib_percent)
                ON CONFLICT (storename, {db_name_col}, week_start)
                DO UPDATE SET
                    total_sales     = EXCLUDED.total_sales,
                    quantity_sold   = EXCLUDED.quantity_sold,
                    profit_margin   = EXCLUDED.profit_margin,
                    contrib_percent = EXCLUDED.contrib_percent,
                    created_at      = NOW();
            """)
            with engine.begin() as conn:
                conn.execute(upsert_sql, rows)

        _upsert(brand_df,    "weekly_brand_snapshot",    "brandName",    "brandname")
        _upsert(category_df, "weekly_category_snapshot", "categoryName", "categoryname")
        _upsert(product_df,  "weekly_product_snapshot",  "productname",  "productname")
        print(f"Snapshot saved for {store_name} (week: {week_start})")

    except Exception as e:
        print(f"Snapshot save failed (non-critical): {e}")


# monthly snapshot

def save_monthly_snapshot(
    store_name: str,
    month_start,
    brand_df: pd.DataFrame,
    category_df: pd.DataFrame,
    product_df: pd.DataFrame,
    engine: Engine,
) -> None:
    try:
        from sqlalchemy import text

        def _upsert(df, table, name_col, db_name_col):
            if df.empty:
                return
            rows = []
            for _, row in df.iterrows():
                rows.append({
                    "storename":       store_name,
                    db_name_col:       str(row.get(name_col, "")),
                    "month_start":     str(month_start),
                    "total_sales":     float(row.get("total_sales", 0) or 0),
                    "quantity_sold":   int(row.get("quantity_sold", 0) or 0),
                    "profit_margin":   float(str(row.get("profit_margin", row.get("PROFIT_MARGIN", 0))).replace("%", "") or 0),
                    "contrib_percent": float(str(row.get("contrib_percent", 0)).replace("%", "") or 0),
                })
            upsert_sql = text(f"""
                INSERT INTO {table}
                    (storename, {db_name_col}, month_start, total_sales, quantity_sold, profit_margin, contrib_percent)
                VALUES
                    (:storename, :{db_name_col}, :month_start, :total_sales, :quantity_sold, :profit_margin, :contrib_percent)
                ON CONFLICT (storename, {db_name_col}, month_start)
                DO UPDATE SET
                    total_sales     = EXCLUDED.total_sales,
                    quantity_sold   = EXCLUDED.quantity_sold,
                    profit_margin   = EXCLUDED.profit_margin,
                    contrib_percent = EXCLUDED.contrib_percent,
                    created_at      = NOW();
            """)
            with engine.begin() as conn:
                conn.execute(upsert_sql, rows)

        _upsert(brand_df,    "monthly_brand_snapshot",    "brandName",    "brandname")
        _upsert(category_df, "monthly_category_snapshot", "categoryName", "categoryname")
        _upsert(product_df,  "monthly_product_snapshot",  "productname",  "productname")
        print(f"Monthly snapshot saved for {store_name} (month: {month_start})")

    except Exception as e:
        print(f"Monthly snapshot save failed (non-critical): {e}")


# weekly trend

def _fetch_wow_trends(
    store_name: str,
    week_start,
    name_col: str,
    table: str,
    db_name_col: str,
    engine: Engine,
) -> dict:
    try:
        from sqlalchemy import text
        prev_week = str(pd.Timestamp(week_start) - pd.Timedelta(weeks=1))[:10]
        query = text(f"""
            SELECT {db_name_col} AS name, total_sales, quantity_sold, profit_margin
            FROM {table}
            WHERE storename = :store AND week_start = :prev_week
        """)
        with engine.connect() as conn:
            rows = conn.execute(query, {"store": store_name, "prev_week": prev_week}).fetchall()
        if not rows:
            return {}
        return {r[0]: {"prev_sales": float(r[1] or 0), "prev_qty": int(r[2] or 0), "prev_margin": float(r[3] or 0)} for r in rows}
    except Exception as e:
        print(f"WoW fetch failed (non-critical): {e}")
        return {}


# monthly trend

def _fetch_mom_trends(
    store_name: str,
    month_start,
    table: str,
    db_name_col: str,
    engine: Engine,
) -> dict:
    try:
        from sqlalchemy import text
        prev_month = str(pd.Timestamp(month_start) - pd.DateOffset(months=1))[:10]
        query = text(f"""
            SELECT {db_name_col} AS name, total_sales, quantity_sold, profit_margin
            FROM {table}
            WHERE storename = :store AND month_start = :prev_month
        """)
        with engine.connect() as conn:
            rows = conn.execute(query, {"store": store_name, "prev_month": prev_month}).fetchall()
        if not rows:
            return {}
        return {r[0]: {"prev_sales": float(r[1] or 0), "prev_qty": int(r[2] or 0), "prev_margin": float(r[3] or 0)} for r in rows}
    except Exception as e:
        print(f"MoM fetch failed (non-critical): {e}")
        return {}


def _compute_trend(current_val: float, prev_val: float):
    if prev_val == 0:
        return None
    pct = ((current_val - prev_val) / prev_val) * 100
    return f"{'+' if pct >= 0 else ''}{pct:.1f}%"

def _enrich_with_trends(records: list, trend_data: dict, name_key: str) -> list:
    """
    Attaches sales_change, qty_change, margin_shift to each record dict
    using prior-period data. Fields are omitted if no trend data exists.
    Safe — returns original records unchanged if trend_data is empty.
    """
    if not trend_data:
        return records
    enriched = []
    for rec in records:
        rec = dict(rec)
        name = rec.get(name_key, "")
        if name in trend_data:
            prev = trend_data[name]
            sc = _compute_trend(rec.get("_sales", 0), prev["prev_sales"])
            qc = _compute_trend(rec.get("_qty",   0), prev["prev_qty"])
            curr_m = rec.get("_margin", None)
            ms = round(curr_m - prev["prev_margin"], 1) if curr_m is not None else None
            if sc:  rec["sales_change"] = sc
            if qc:  rec["qty_change"]   = qc
            if ms is not None:
                rec["margin_shift"] = f"{'+' if ms >= 0 else ''}{ms}pp"
        enriched.append(rec)
    return enriched


def _compute_predictions(df_rows: list, trend_data: dict, name_key: str) -> dict:
    """
    Lightweight velocity-based predictions using 2-period data.
    Returns:
      - stockout_risk:   items with high sales + accelerating qty demand
      - margin_erosion:  items whose margin is declining period-over-period
      - rising_stars:    items outside top revenue but with strong sales growth
    Only populated when trend_data exists — empty dicts otherwise.
    """
    if not trend_data:
        return {"stockout_risk": [], "margin_erosion": [], "rising_stars": []}

    stockout_risk  = []
    margin_erosion = []
    rising_stars   = []

    # Rank current items by sales to identify top vs non-top
    sorted_by_sales = sorted(df_rows, key=lambda r: r.get("_sales", 0), reverse=True)
    top_names = {r[name_key] for r in sorted_by_sales[:5]}

    for rec in df_rows:
        name = rec.get(name_key, "")
        if name not in trend_data:
            continue
        prev = trend_data[name]
        curr_sales  = rec.get("_sales",  0)
        curr_qty    = rec.get("_qty",    0)
        curr_margin = rec.get("_margin", None)
        prev_sales  = prev["prev_sales"]
        prev_qty    = prev["prev_qty"]
        prev_margin = prev["prev_margin"]

        sales_pct = ((curr_sales - prev_sales) / prev_sales * 100) if prev_sales > 0 else None
        qty_pct   = ((curr_qty   - prev_qty)   / prev_qty   * 100) if prev_qty   > 0 else None

        # Stockout risk: top-revenue item with qty growing >20% — demand accelerating
        if name in top_names and qty_pct is not None and qty_pct > 20:
            stockout_risk.append({
                "name":          name,
                "curr_qty":      int(curr_qty),
                "qty_change":    f"+{qty_pct:.1f}%",
                "curr_sales":    round(curr_sales, 2),
                "warning":       "demand accelerating — check stock level and reorder before next period"
            })

        # Margin erosion: margin dropping >3pp two periods in a row
        if curr_margin is not None and prev_margin > 0:
            margin_shift = round(curr_margin - prev_margin, 1)
            if margin_shift < -3:
                margin_erosion.append({
                    "name":         name,
                    "curr_margin":  round(curr_margin, 2),
                    "margin_shift": f"{margin_shift}pp",
                    "warning":      "margin eroding — supplier cost likely increased, renegotiate immediately"
                })

        # Rising stars: NOT in top 5 revenue but sales growing >25%
        if name not in top_names and sales_pct is not None and sales_pct > 25:
            rising_stars.append({
                "name":         name,
                "sales_change": f"+{sales_pct:.1f}%",
                "curr_sales":   round(curr_sales, 2),
                "curr_margin":  round(curr_margin, 2) if curr_margin is not None else None,
                "signal":       "gaining momentum — increase shelf space and ensure stock availability"
            })

    # Sort by severity
    stockout_risk.sort(key=lambda x: float(x["qty_change"].replace("+", "").replace("%", "")), reverse=True)
    margin_erosion.sort(key=lambda x: float(x["margin_shift"].replace("pp", "").split()[0]))
    rising_stars.sort(key=lambda x: float(x["sales_change"].replace("+", "").replace("%", "")), reverse=True)

    return {
        "stockout_risk":  stockout_risk[:3],
        "margin_erosion": margin_erosion[:3],
        "rising_stars":   rising_stars[:3],
    }

#intelligence

def _clean_numeric(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.replace("%", "", regex=False)
        .apply(lambda x: float(x) if x.replace(".", "").replace("-", "").isdigit() else np.nan)
    )


def _get_margin_col(df: pd.DataFrame):
    for c in ["profit_margin", "PROFIT_MARGIN"]:
        if c in df.columns:
            return c
    return None


def _compute_intelligence(df: pd.DataFrame, name_col: str, trend_data: dict) -> dict:
    if df.empty:
        return {}

    margin_col = _get_margin_col(df)
    df = df.copy()
    df["_sales"]   = _clean_numeric(df["total_sales"])
    df["_qty"]     = pd.to_numeric(df.get("quantity_sold", pd.Series(dtype=float)), errors="coerce").fillna(0)
    df["_margin"]  = _clean_numeric(df[margin_col]) if margin_col else pd.Series(np.nan, index=df.index)
    df["_contrib"] = _clean_numeric(df["contrib_percent"]) if "contrib_percent" in df.columns else pd.Series(np.nan, index=df.index)

    total_sales = df["_sales"].sum()
    avg_margin  = df["_margin"].mean()

    top10      = df.nlargest(10, "_sales")[[name_col, "_sales", "_qty", "_margin", "_contrib"]].round(2)
    # Bottom 5 by quantity (worst first) — exclude zero-qty items already caught by anomalies
    bot5_qty   = df[df["_qty"] > 0].nsmallest(5, "_qty")[[name_col, "_qty", "_sales", "_margin"]].round(2)
    low_margin = df[df["_margin"] < min(avg_margin, 15)].nsmallest(5, "_margin")[[name_col, "_margin", "_sales"]].round(2)
    avg_qty    = round(df[df["_qty"] > 0]["_qty"].mean(), 1)

    top10_names = set(top10[name_col].tolist())
    hidden_gems = df[~df[name_col].isin(top10_names)].nlargest(3, "_margin")[[name_col, "_margin", "_sales", "_qty"]].round(2)

    df["_rev_weight"]  = df["_sales"] / total_sales if total_sales > 0 else 0
    max_margin         = df["_margin"].max() if df["_margin"].notna().any() else 1
    df["_margin_risk"] = 1 - (df["_margin"].fillna(0) / max_margin if max_margin > 0 else 0)
    df["_risk_score"]  = (df["_rev_weight"] * df["_margin_risk"] * 100).round(2)
    high_risk          = df.nlargest(3, "_risk_score")[[name_col, "_risk_score", "_sales", "_margin"]].round(2)

    top1_share         = round((top10.iloc[0]["_sales"] / total_sales * 100), 1) if total_sales > 0 else 0
    top3_share         = round((top10.head(3)["_sales"].sum() / total_sales * 100), 1) if total_sales > 0 else 0
    concentration_flag = top3_share > 50

    mix_risk_items = pd.DataFrame()
    if not df["_margin"].isna().all() and not df["_contrib"].isna().all():
        mix_risk_items = df[
            (df["_contrib"] > df["_contrib"].median()) & (df["_margin"] < avg_margin)
        ][[name_col, "_contrib", "_margin"]].head(3).round(2)

    anomalies = []
    for _, row in df[df["_margin"] < 0][[name_col, "_margin", "_sales"]].iterrows():
        anomalies.append(f"{row[name_col]}: NEGATIVE margin {row['_margin']}% on Rs.{row['_sales']:,.0f} sales")
    for _, row in df[df["_qty"] == 1][[name_col, "_sales"]].iterrows():
        anomalies.append(f"{row[name_col]}: only 1 unit sold — dead stock risk")

    # Trend signals
    declining     = []
    margin_shifts = []
    all_rows = df.to_dict("records")

    if trend_data:
        for _, row in df.iterrows():
            name = row[name_col]
            if name not in trend_data:
                continue
            prev         = trend_data[name]
            sales_change = _compute_trend(row["_sales"], prev["prev_sales"])
            qty_change   = _compute_trend(row["_qty"],   prev["prev_qty"])
            margin_shift = round(row["_margin"] - prev["prev_margin"], 2) if not np.isnan(row["_margin"]) else None

            if sales_change and float(sales_change.replace("+", "").replace("%", "")) < -10:
                declining.append({
                    "name":         name,
                    "sales_change": sales_change,
                    "qty_change":   qty_change,
                    "curr_sales":   round(row["_sales"], 2),
                    "prev_sales":   prev["prev_sales"],
                })
            if margin_shift is not None and abs(margin_shift) >= 3:
                margin_shifts.append({
                    "name":         name,
                    "margin_shift": f"{'+' if margin_shift > 0 else ''}{margin_shift}pp {'↑ improved' if margin_shift > 0 else '↓ deteriorated'}",
                    "prev_margin":  prev["prev_margin"],
                    "curr_margin":  round(row["_margin"], 2),
                })

        declining.sort(key=lambda x: float(x["sales_change"].replace("+", "").replace("%", "")))
        declining     = declining[:3]
        margin_shifts = margin_shifts[:3]

    # ── Enrich key sections with trend deltas
    top10_enriched      = _enrich_with_trends(top10.to_dict("records"),      trend_data, name_col)
    bot5_enriched       = _enrich_with_trends(bot5_qty.to_dict("records"),   trend_data, name_col)
    low_margin_enriched = _enrich_with_trends(low_margin.to_dict("records"), trend_data, name_col)
    gems_enriched       = _enrich_with_trends(hidden_gems.to_dict("records"),trend_data, name_col)

    # Predictive signals
    predictions = _compute_predictions(all_rows, trend_data, name_col)

    return {
        "top_10_by_revenue":      top10_enriched,
        "bottom_5_by_quantity":   bot5_enriched,
        "avg_qty_sold":           avg_qty,
        "low_margin_items":       low_margin_enriched,
        "hidden_margin_gems":     gems_enriched,
        "high_risk_items":        high_risk.to_dict("records"),
        "mix_risk_items":         mix_risk_items.to_dict("records") if not mix_risk_items.empty else [],
        "concentration_flag":     concentration_flag,
        "top1_revenue_share_pct": top1_share,
        "top3_revenue_share_pct": top3_share,
        "avg_margin_pct":         round(avg_margin, 2),
        "anomalies":              anomalies,
        "trend_declining":        declining,
        "trend_margin_shifts":    margin_shifts,
        "has_trend_data":         bool(trend_data),
        "predictions":            predictions,
    }


def brand_recommendation(
    store_name: str,
    brand_df: pd.DataFrame,
    total_sales: float,
    week_start=None,
    month_start=None,
    engine: Engine = None,
    report_type: str = "weekly",
) -> str:
    trend_data = {}
    if engine is not None:
        if report_type == "monthly" and month_start is not None:
            trend_data = _fetch_mom_trends(store_name, month_start, "monthly_brand_snapshot", "brandname", engine)
        elif report_type == "weekly" and week_start is not None:
            trend_data = _fetch_wow_trends(store_name, week_start, "brandName", "weekly_brand_snapshot", "brandname", engine)

    intel = _compute_intelligence(brand_df, "brandName", trend_data)
    if not intel:
        return _UNAVAILABLE

    trend_label  = "MoM" if report_type == "monthly" else "WoW"
    period_label = "month" if report_type == "monthly" else "week"
    conc = (f"Top 3 brands = {intel['top3_revenue_share_pct']}% of revenue — over-concentration"
            if intel['concentration_flag'] else
            f"Healthy spread — top 3 brands = {intel['top3_revenue_share_pct']}% of revenue")
    trend_section = (
        f"\n{trend_label} DECLINING BRANDS (revenue dropped >10% vs last {period_label}):\n"
        f"{json.dumps(intel['trend_declining'], indent=2) if intel['trend_declining'] else 'None — all brands stable or growing'}\n\n"
        f"{trend_label} MARGIN SHIFTS (margin changed >3pp vs last {period_label}):\n"
        f"{json.dumps(intel['trend_margin_shifts'], indent=2) if intel['trend_margin_shifts'] else 'None — margins stable'}\n"
        if intel["has_trend_data"] else
        f"\n{trend_label} TREND: No prior {period_label} snapshot yet — trend data will appear from next run.\n"
    )

    # Pre-compute deduplication at Python level — LLM cannot repeat these names
    top2_names = [r["brandName"] for r in intel["top_10_by_revenue"][:2]] if intel["top_10_by_revenue"] else []
    margin_risk_candidates = [r for r in (intel["low_margin_items"] + intel["high_risk_items"]) if r.get("brandName") not in top2_names]
    gem_candidates = [r for r in intel["hidden_margin_gems"] if r.get("brandName") not in top2_names and (not margin_risk_candidates or r.get("brandName") != (margin_risk_candidates[0].get("brandName") if margin_risk_candidates else ""))]

    pred = intel["predictions"]
    pred_section = (
        f"\nPREDICTIVE SIGNALS (based on {trend_label} velocity):\n"
        f"STOCKOUT RISK (demand accelerating — reorder urgently):\n{json.dumps(pred['stockout_risk'], indent=2) if pred['stockout_risk'] else 'None detected'}\n"
        f"MARGIN EROSION (margin declining vs last {period_label}):\n{json.dumps(pred['margin_erosion'], indent=2) if pred['margin_erosion'] else 'None detected'}\n"
        f"RISING STARS ({trend_label} growth >25%, not yet top revenue):\n{json.dumps(pred['rising_stars'], indent=2) if pred['rising_stars'] else 'None detected'}\n"
        if intel["has_trend_data"] else
        f"\nPREDICTIVE SIGNALS: No prior {period_label} snapshot yet — predictions will appear from next run.\n"
    )

    prompt = f"""
        You are a senior retail strategy analyst evaluating BRAND performance across the store.

        Focus: supplier performance, revenue concentration, and margin quality.

        Store: "{store_name}" | {report_type}
        Total Sales: Rs.{total_sales:,.2f}
        Avg Margin: {intel['avg_margin_pct']}%

        ================ DATA =================

        TOP BRANDS:
        {json.dumps(intel['top_10_by_revenue'][:5], indent=2)}

        LOW MOVEMENT BRANDS:
        {json.dumps(intel['bottom_5_by_quantity'], indent=2)}

        MARGIN RISK BRANDS:
        {json.dumps(margin_risk_candidates[:5], indent=2)}

        HIGH MARGIN BRANDS (UNDERUTILIZED):
        {json.dumps(gem_candidates[:3], indent=2)}

        CONCENTRATION:
        Top 3 brands contribute {intel['top3_revenue_share_pct']}%

        ANOMALIES:
        {json.dumps(intel['anomalies']) if intel['anomalies'] else "None"}

        TRENDS:
        {json.dumps(intel['trend_declining'], indent=2) if intel['trend_declining'] else "Stable"}

        MARGIN SHIFTS:
        {json.dumps(intel['trend_margin_shifts'], indent=2) if intel['trend_margin_shifts'] else "Stable"}

        PREDICTIONS:
        {json.dumps(pred, indent=2)}

        ================ OUTPUT =================

        Return EXACTLY these 5 sections, each with 3 tight bullet points:

        1. STORE HEALTH SCORE
        - Score (0–100) with label (Healthy / Moderate / At Risk)
        - Key drivers of the score: concentration %, avg margin %, number of risk brands
        - Single biggest threat to store health right now and what to do about it

        2. REVENUE FORECAST
        - Direction (Growth / Stable / Decline) with estimated % and projected Rs impact on next week
        - Top 2–3 brands driving that direction with their individual sales change %
        - Biggest risk to the forecast — which brand could derail it and why

        3. PEAK SALES DRIVER
        - Top brand by revenue: name, Rs sales, contribution %, WoW change
        - Second and third strongest brands and their momentum (growing / stable / declining)
        - Action: how to capitalise — increase shelf space, push promotions, or protect against decline

        4. INVENTORY EFFICIENCY
        - Stockout risk brands (demand accelerating): name, qty growth %, recommended reorder urgency
        - Slow-moving / dead brands: name, units sold, Rs tied up — flag for markdown or return
        - One systemic action: e.g. reorder cycle adjustment, vendor escalation, or dead stock clearance plan

        5. MARGIN INTELLIGENCE
        - Worst margin brand: name, margin %, Rs sales, financial impact of the loss
        - Margin erosion alerts: brands where margin dropped >3pp — root cause (supplier cost / pricing gap)
        - Immediate action per brand: delist / renegotiate / reprice — with specific target margin to aim for

        ================ RULES =================

        - Exactly 3 bullet points per section — no more, no less
        - Every bullet must contain at least one specific number (Rs / % / units / pp)
        - No generic advice — every line must be actionable for this store's data
        - No repetition across sections — each section covers distinct ground
        - Write as a trusted advisor, not a report generator
        """
    text, fallback = _get_recommendation(prompt)
    return _wrap_html(text, fallback)


def category_recommendation(
    store_name: str,
    category_df: pd.DataFrame,
    total_sales: float,
    week_start=None,
    month_start=None,
    engine: Engine = None,
    report_type: str = "weekly",
) -> str:
    trend_data = {}
    if engine is not None:
        if report_type == "monthly" and month_start is not None:
            trend_data = _fetch_mom_trends(store_name, month_start, "monthly_category_snapshot", "categoryname", engine)
        elif report_type == "weekly" and week_start is not None:
            trend_data = _fetch_wow_trends(store_name, week_start, "categoryName", "weekly_category_snapshot", "categoryname", engine)

    intel = _compute_intelligence(category_df, "categoryName", trend_data)
    if not intel:
        return _UNAVAILABLE

    trend_label  = "MoM" if report_type == "monthly" else "WoW"
    period_label = "month" if report_type == "monthly" else "week"
    conc = (f"Top 3 categories = {intel['top3_revenue_share_pct']}% of revenue — over-concentration"
            if intel['concentration_flag'] else
            f"Healthy spread — top 3 categories = {intel['top3_revenue_share_pct']}% of revenue")
    trend_section = (
        f"\n{trend_label} DECLINING CATEGORIES (revenue dropped >10% vs last {period_label}):\n"
        f"{json.dumps(intel['trend_declining'], indent=2) if intel['trend_declining'] else 'None — all categories stable or growing'}\n\n"
        f"{trend_label} MARGIN SHIFTS (margin changed >3pp vs last {period_label}):\n"
        f"{json.dumps(intel['trend_margin_shifts'], indent=2) if intel['trend_margin_shifts'] else 'None — margins stable'}\n"
        if intel["has_trend_data"] else
        f"\n{trend_label} TREND: No prior {period_label} snapshot yet — trend data will appear from next run.\n"
    )

    # Pre-compute deduplication at Python level
    top2_cat_names = [r["categoryName"] for r in intel["top_10_by_revenue"][:2]] if intel["top_10_by_revenue"] else []
    cat_margin_candidates = [r for r in (intel["low_margin_items"] + intel["high_risk_items"]) if r.get("categoryName") not in top2_cat_names]
    cat_gem_candidates = [r for r in intel["hidden_margin_gems"] if r.get("categoryName") not in top2_cat_names and (not cat_margin_candidates or r.get("categoryName") != (cat_margin_candidates[0].get("categoryName") if cat_margin_candidates else ""))]

    pred = intel["predictions"]
    pred_section = (
        f"\nPREDICTIVE SIGNALS (based on {trend_label} velocity):\n"
        f"STOCKOUT RISK:\n{json.dumps(pred['stockout_risk'], indent=2) if pred['stockout_risk'] else 'None detected'}\n"
        f"MARGIN EROSION:\n{json.dumps(pred['margin_erosion'], indent=2) if pred['margin_erosion'] else 'None detected'}\n"
        f"RISING STARS:\n{json.dumps(pred['rising_stars'], indent=2) if pred['rising_stars'] else 'None detected'}\n"
        if intel["has_trend_data"] else
        f"\nPREDICTIVE SIGNALS: No prior {period_label} snapshot yet — predictions will appear from next run.\n"
    )

    prompt = f"""
        You are a retail merchandising analyst evaluating CATEGORY performance.

        Focus: category mix, demand distribution, and shelf optimization.

        Store: "{store_name}" | {report_type}
        Total Sales: Rs.{total_sales:,.2f}
        Avg Margin: {intel['avg_margin_pct']}%

        ================ DATA =================

        TOP CATEGORIES:
        {json.dumps(intel['top_10_by_revenue'][:5], indent=2)}

        LOW PERFORMANCE CATEGORIES:
        {json.dumps(intel['bottom_5_by_quantity'], indent=2)}

        LOW MARGIN CATEGORIES:
        {json.dumps(cat_margin_candidates[:5], indent=2)}

        HIGH MARGIN OPPORTUNITIES:
        {json.dumps(cat_gem_candidates[:3], indent=2)}

        MIX RISK:
        {json.dumps(intel['mix_risk_items'], indent=2)}

        CONCENTRATION:
        Top 3 categories contribute {intel['top3_revenue_share_pct']}%

        ANOMALIES:
        {json.dumps(intel['anomalies']) if intel['anomalies'] else "None"}

        TRENDS:
        {json.dumps(intel['trend_declining'], indent=2) if intel['trend_declining'] else "Stable"}

        PREDICTIONS:
        {json.dumps(pred, indent=2)}

        ================ OUTPUT =================

        Return EXACTLY these 5 sections, each with 3 tight bullet points:

        1. STORE HEALTH SCORE
        - Score (0–100) with label (Healthy / Moderate / At Risk)
        - Key drivers: category concentration %, avg margin %, number of low-margin or mix-risk categories
        - Single biggest structural weakness in the category mix and recommended fix

        2. REVENUE FORECAST
        - Direction (Growth / Stable / Decline) with estimated % and projected Rs impact on next week
        - Top 2–3 categories driving that direction with their individual sales change %
        - Biggest category risk — which one could pull revenue down and why

        3. PEAK SALES DRIVER
        - Top category by revenue: name, Rs sales, contribution %, WoW change
        - Second and third strongest categories and their momentum trend
        - Action: how to capitalise — increase shelf allocation, bundle with rising star, or defend against decline

        4. INVENTORY EFFICIENCY
        - Categories with stockout or surging demand: name, qty growth %, reorder urgency
        - Overstocked or slow categories: name, units sold, Rs of idle inventory — flag for markdown or reallocation
        - One structural action: shelf space rebalancing, vendor lead time fix, or reorder frequency change

        5. MARGIN INTELLIGENCE
        - Worst margin category: name, margin %, Rs sales, and the profit being lost
        - Mix risk: high-revenue but low-margin categories dragging overall store margin down
        - Immediate action: which category to reduce shelf space for, which to promote, and what margin target to set

        ================ RULES =================

        - Exactly 3 bullet points per section — no more, no less
        - Every bullet must contain at least one specific number (Rs / % / units / pp)
        - No generic advice — every line must be actionable for this store's category data
        - No repetition across sections — each covers distinct ground
        - Focus on category mix decisions a store manager can act on today
        """
    text, fallback = _get_recommendation(prompt)
    return _wrap_html(text, fallback)


def product_recommendation(
    store_name: str,
    product_df: pd.DataFrame,
    total_sales: float,
    week_start=None,
    month_start=None,
    engine: Engine = None,
    report_type: str = "weekly",
) -> str:
    trend_data = {}
    if engine is not None:
        if report_type == "monthly" and month_start is not None:
            trend_data = _fetch_mom_trends(store_name, month_start, "monthly_product_snapshot", "productname", engine)
        elif report_type == "weekly" and week_start is not None:
            trend_data = _fetch_wow_trends(store_name, week_start, "productName", "weekly_product_snapshot", "productname", engine)

    intel = _compute_intelligence(product_df, "productName", trend_data)
    if not intel:
        return _UNAVAILABLE

    trend_label  = "MoM" if report_type == "monthly" else "WoW"
    period_label = "month" if report_type == "monthly" else "week"
    conc = (f"Top 3 products = {intel['top3_revenue_share_pct']}% of revenue — over-concentration"
            if intel['concentration_flag'] else
            f"Healthy spread — top 3 products = {intel['top3_revenue_share_pct']}% of revenue")
    trend_section = (
        f"\n{trend_label} DECLINING PRODUCTS (revenue dropped >10% vs last {period_label}):\n"
        f"{json.dumps(intel['trend_declining'], indent=2) if intel['trend_declining'] else 'None — all products stable or growing'}\n\n"
        f"{trend_label} MARGIN SHIFTS (margin changed >3pp vs last {period_label}):\n"
        f"{json.dumps(intel['trend_margin_shifts'], indent=2) if intel['trend_margin_shifts'] else 'None — margins stable'}\n"
        if intel["has_trend_data"] else
        f"\n{trend_label} TREND: No prior {period_label} snapshot yet — trend data will appear from next run.\n"
    )

    # Pre-compute deduplication at Python level
    top2_prod_names = [r["productName"] for r in intel["top_10_by_revenue"][:2]] if intel["top_10_by_revenue"] else []
    prod_margin_candidates = [r for r in (intel["low_margin_items"] + intel["high_risk_items"]) if r.get("productName") not in top2_prod_names]
    prod_gem_candidates = [r for r in intel["hidden_margin_gems"] if r.get("productName") not in top2_prod_names and (not prod_margin_candidates or r.get("productName") != (prod_margin_candidates[0].get("productName") if prod_margin_candidates else ""))]

    pred = intel["predictions"]
    pred_section = (
        f"\nPREDICTIVE SIGNALS (based on {trend_label} velocity):\n"
        f"STOCKOUT RISK:\n{json.dumps(pred['stockout_risk'], indent=2) if pred['stockout_risk'] else 'None detected'}\n"
        f"MARGIN EROSION:\n{json.dumps(pred['margin_erosion'], indent=2) if pred['margin_erosion'] else 'None detected'}\n"
        f"RISING STARS:\n{json.dumps(pred['rising_stars'], indent=2) if pred['rising_stars'] else 'None detected'}\n"
        if intel["has_trend_data"] else
        f"\nPREDICTIVE SIGNALS: No prior {period_label} snapshot yet — predictions will appear from next run.\n"
    )

    prompt = f"""
        You are a store operations analyst evaluating PRODUCT (SKU-level) performance.

        Focus: fast movers, dead stock, and execution decisions.

        Store: "{store_name}" | {report_type}
        Total Sales: Rs.{total_sales:,.2f}
        Avg Margin: {intel['avg_margin_pct']}%

        ================ DATA =================

        TOP PRODUCTS:
        {json.dumps(intel['top_10_by_revenue'][:5], indent=2)}

        LOW MOVEMENT PRODUCTS:
        {json.dumps(intel['bottom_5_by_quantity'], indent=2)}

        LOW MARGIN PRODUCTS:
        {json.dumps(intel['low_margin_items'][:5], indent=2)}

        HIGH MARGIN PRODUCTS:
        {json.dumps(intel['hidden_margin_gems'][:3], indent=2)}

        ANOMALIES:
        {json.dumps(intel['anomalies']) if intel['anomalies'] else "None"}

        TRENDS:
        {json.dumps(intel['trend_declining'], indent=2) if intel['trend_declining'] else "Stable"}

        PREDICTIONS:
        {json.dumps(pred, indent=2)}

        ================ OUTPUT =================

        Return EXACTLY these 5 sections, each with 3 tight bullet points:

        1. STORE HEALTH SCORE
        - Score (0–100) with label (Healthy / Moderate / At Risk)
        - Key drivers: SKU velocity spread, dead stock count, negative/low margin SKU count
        - Single most urgent SKU-level problem and the fix needed today

        2. REVENUE FORECAST
        - Direction (Growth / Stable / Decline) with estimated % and projected Rs impact on next week
        - Top 2–3 SKUs driving that direction: name, Rs sales, WoW change %
        - Biggest SKU risk — which product is declining fastest and what happens if unchecked

        3. PEAK SALES DRIVER
        - Top SKU by revenue: name, Rs sales, contribution %, WoW change, units sold
        - Second and third strongest SKUs and whether their momentum is building or fading
        - Action: restock priority, promotional push, or shelf positioning change to protect the top drivers

        4. INVENTORY EFFICIENCY
        - Stockout risk SKUs (demand accelerating): name, qty growth %, reorder urgency + vendor to contact
        - Dead stock / 1-unit-sold SKUs: name, Rs tied up — recommend markdown %, return, or discontinue
        - One systemic action: e.g. fast-mover reorder cycle, dead stock clearance timeline, or GRN fix

        5. MARGIN INTELLIGENCE
        - Worst margin SKU: name, margin %, Rs sales, actual Rs profit being lost per week
        - SKUs with margin erosion >3pp: name, shift in pp — likely cause (supplier cost / underpricing)
        - Immediate action per SKU: delist / reprice / renegotiate — with specific margin target to recover

        ================ RULES =================

        - Exactly 3 bullet points per section — no more, no less
        - Every bullet must contain at least one specific number (Rs / % / units / pp)
        - No generic advice — every line must reference specific SKUs from the data
        - No repetition across sections — each covers distinct ground
        - Write as a store operations advisor giving execution-ready instructions
        """
    text, fallback = _get_recommendation(prompt)
    return _wrap_html(text, fallback)


# ══════════════════════════════════════════════════════════════════════════════
# ── STOCK INSIGHT FUNCTIONS ───────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
#
# These three functions read the per-store stock CSV (downloaded by stock.py),
# compute low/out-of-stock intelligence for the requested dimension
# (brand / category / product), and return a styled HTML block that is
# appended directly to the existing AI Recommendation box in the weekly
# (or monthly) PDF report — rendered right after the sales insight bullets.
#
# Usage in weekly_llm.py  (add after the existing brand_rec / category_rec /
# product_rec calls):
#
#   from llm_recommender import (
#       brand_recommendation, category_recommendation, product_recommendation,
#       brand_stock_insight, category_stock_insight, product_stock_insight,
#   )
#
#   STOCK_DIR = "store_stocks"          # directory where stock.py saves CSVs
#   LOW_STOCK_THRESHOLD = 5             # units; tweak as needed
#
#   brand_stock    = brand_stock_insight(store_name, STOCK_DIR, LOW_STOCK_THRESHOLD)
#   category_stock = category_stock_insight(store_name, STOCK_DIR, LOW_STOCK_THRESHOLD)
#   product_stock  = product_stock_insight(store_name, STOCK_DIR, LOW_STOCK_THRESHOLD)
#
# Then in the HTML template, append each *_stock variable right after its
# corresponding *_rec variable:
#
#   {brand_rec}
#   {brand_stock}
#   ...
#   {category_rec}
#   {category_stock}
#   ...
#   {product_rec}
#   {product_stock}


_LOW_STOCK_STYLE = """
<div style="
    background: #fff8e1;
    border-left: 5px solid #f9a825;
    border-radius: 6px;
    padding: 14px 18px;
    margin: 10px 0 24px 0;
    font-family: 'Segoe UI', sans-serif;
">
    <div style="font-size:15px; font-weight:bold; color:#e65100; margin-bottom:8px;">
        {header}
    </div>
    <div style="font-size:14px; line-height:1.9;">
{body}
    </div>
</div>
"""

_LOW_STOCK_UNAVAILABLE = _LOW_STOCK_STYLE.format(
    header="New Shop Stock Alert — Unavailable",
    body="Could not generate stock insight. Check that the store stock CSV exists in the stock directory."
)

_STOCK_BULLET_RULES = (
    "One sentence per bullet. Name + number + one action only. "
    "No generic phrases. No repeated names. Use only numbers from the data."
)

#Stock bullet colour constants
_C_NEG     = "#c62828"
_C_OOS     = "#e65100"
_C_LOW     = "#f57c00"
_C_GAP     = "#1565c0"
_C_PATTERN = "#6a1b9a"

def _sb(color: str, text: str) -> str:
    """Wrap one stock alert bullet as a coloured HTML div."""
    return f'<div style="color:{color}; margin:4px 0;">{text}</div>'


def _load_stock_csv(store_name: str, stock_dir: str) -> pd.DataFrame:
    """
    Load the stock CSV for a store from stock_dir.
    Tries <store_name>.csv first (exact match), then with spaces replaced by
    underscores to match how stock.py saves files.
    Returns an empty DataFrame on any failure.
    """
    safe_name = store_name.replace("/", "_")
    candidates = [
        os.path.join(stock_dir, f"{safe_name}.csv"),
        os.path.join(stock_dir, f"{store_name}.csv"),
    ]
    for path in candidates:
        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
                for col in ["quantity", "sellingPrice", "costPrice", "printedMrp", "totalAmount"]:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
                return df
            except Exception as e:
                print(f"Stock CSV read error for {store_name}: {e}")
                return pd.DataFrame()
    print(f"No stock CSV found for {store_name} in {stock_dir} — stock insight skipped.")
    return pd.DataFrame()


def _compute_stock_intelligence(
    stock_df: pd.DataFrame,
    group_col: str,
    threshold: float,
) -> dict:
    """
    Aggregate stock data by group_col and return intelligence dict.

    Returns dict with:
      - oos_items      : list of dicts — completely out-of-stock groups
      - low_items      : list of dicts — groups with 1–threshold units
      - high_value_oos : top out-of-stock groups by sellingPrice * quantity_lost
      - summary        : store-level counts
    """
    if stock_df.empty or group_col not in stock_df.columns:
        return {}

    df = stock_df.copy()
    neg_df = df[df["quantity"] < 0]
    oos_df = df[df["quantity"] == 0]
    low_df = df[(df["quantity"] > 0) & (df["quantity"] <= threshold)]

    def _agg(subset, label):
        if subset.empty:
            return []
        agg = (
            subset.groupby(group_col)
            .agg(
                sku_count=("productName", "count"),
                min_qty=("quantity", "min"),
                avg_selling_price=("sellingPrice", "mean"),
                total_value_at_risk=("sellingPrice", "sum"),
            )
            .sort_values("sku_count", ascending=False)
            .reset_index()
            .head(10)
        )
        records = agg.to_dict("records")
        for r in records:
            r["status"] = label
            r["avg_selling_price"] = round(r["avg_selling_price"], 2)
            r["total_value_at_risk"] = round(r["total_value_at_risk"], 2)
        return records

    oos_items = _agg(oos_df, "OUT_OF_STOCK")
    low_items = _agg(low_df, f"LOW_STOCK (≤{threshold:.0f} units)")

    # Negative stock: items sold without GRN grouped by group_col
    neg_items = []
    if not neg_df.empty:
        agg_neg = (
            neg_df.groupby(group_col)
            .agg(
                sku_count=("productName", "count"),
                min_qty=("quantity", "min"),
                total_qty=("quantity", "sum"),
                avg_selling_price=("sellingPrice", "mean"),
            )
            .sort_values("sku_count", ascending=False)
            .reset_index()
            .head(10)
        )
        neg_items = agg_neg.to_dict("records")
        for r in neg_items:
            r["avg_selling_price"] = round(r["avg_selling_price"], 2)
            r["total_qty"]         = round(r["total_qty"], 2)
            r["min_qty"]           = round(r["min_qty"], 2)

    # Negative stock: individual products
    neg_products = []
    if not neg_df.empty and "productName" in neg_df.columns:
        neg_products = (
            neg_df[["productName", group_col, "quantity", "sellingPrice", "vendorName"]]
            .sort_values("quantity")
            .head(10)
            .round(2)
            .to_dict("records")
        )

    # High-value
    high_value_oos = []
    if not oos_df.empty:
        hv = (
            oos_df[["productName", group_col, "sellingPrice"]]
            .sort_values("sellingPrice", ascending=False)
            .head(5)
            .to_dict("records")
        )
        high_value_oos = [
            {group_col: r[group_col], "productName": r["productName"],
             "sellingPrice": round(r["sellingPrice"], 2)}
            for r in hv
        ]

    return {
        "oos_items":       oos_items,
        "low_items":       low_items,
        "neg_items":       neg_items,
        "neg_products":    neg_products,
        "high_value_oos":  high_value_oos,
        "total_skus":      len(df),
        "oos_count":       len(oos_df),
        "low_count":       len(low_df),
        "neg_count":       len(neg_df),
        "threshold":       threshold,
    }


def _wrap_stock_html(bullets: list) -> str:
    if not bullets:
        return _LOW_STOCK_UNAVAILABLE
    return _LOW_STOCK_STYLE.format(
        header="Stock Alerts",
        body="".join(bullets),
    )


def brand_stock_insight(
    store_name: str,
    stock_dir: str,
    low_stock_threshold: float = 5,
) -> str:
    """
    Generate a stock-alert insight block for brands.
    Returned HTML is appended after brand_recommendation() in the weekly report.
    """
    stock_df = _load_stock_csv(store_name, stock_dir)
    if stock_df.empty:
        return ""

    intel = _compute_stock_intelligence(stock_df, "brand", low_stock_threshold)
    if not intel or (intel["oos_count"] == 0 and intel["low_count"] == 0 and intel["neg_count"] == 0):
        return ""

    bullets = []

    # Negative stock — deep red
    for r in intel["neg_items"][:3]:
        bullets.append(_sb(_C_NEG,
            f"{r['brand']}: {r['sku_count']} SKU(s) with negative stock "
            f"(worst: {r['min_qty']:.0f} units) — sold without GRN, post pending GRNs immediately."
        ))

    # OOS brands — dark orange
    for r in intel["oos_items"][:2]:
        bullets.append(_sb(_C_OOS,
            f"{r['brand']}: {r['sku_count']} SKU(s) completely out of stock "
            f"(₹{r['total_value_at_risk']:.0f} at risk) — place reorder today."
        ))

    # Low-stock brands — orange
    for r in intel["low_items"][:2]:
        bullets.append(_sb(_C_LOW,
            f"{r['brand']}: {r['sku_count']} SKU(s) with ≤{intel['threshold']:.0f} units "
            f"(min {r['min_qty']:.0f} units) — request top-up this week."
        ))

    # Highest-value OOS product — blue
    if intel["high_value_oos"]:
        hv = intel["high_value_oos"][0]
        bullets.append(_sb(_C_GAP,
            f"{hv['productName']} ({hv['brand']}, ₹{hv['sellingPrice']:.0f}) "
            f"— out of stock, fast-track reorder to avoid lost revenue."
        ))

    return _wrap_stock_html(bullets)


def category_stock_insight(
    store_name: str,
    stock_dir: str,
    low_stock_threshold: float = 5,
) -> str:
    """
    Generate a stock-alert insight block for categories.
    Returned HTML is appended after category_recommendation() in the weekly report.
    """
    stock_df = _load_stock_csv(store_name, stock_dir)
    if stock_df.empty:
        return ""

    intel = _compute_stock_intelligence(stock_df, "categoryName", low_stock_threshold)
    if not intel or (intel["oos_count"] == 0 and intel["low_count"] == 0 and intel["neg_count"] == 0):
        return ""

    bullets = []

    # Negative stock — deep red
    for r in intel["neg_items"][:3]:
        bullets.append(_sb(_C_NEG,
            f"{r['categoryName']}: {r['sku_count']} SKU(s) with negative stock "
            f"(worst: {r['min_qty']:.0f} units) — sold without GRN, post pending GRNs immediately."
        ))

    # OOS categories — dark orange
    for r in intel["oos_items"][:2]:
        bullets.append(_sb(_C_OOS,
            f"{r['categoryName']}: {r['sku_count']} SKU(s) completely out of stock "
            f"(₹{r['total_value_at_risk']:.0f} at risk) — contact supplier today."
        ))

    # Low-stock categories — orange
    for r in intel["low_items"][:2]:
        bullets.append(_sb(_C_LOW,
            f"{r['categoryName']}: {r['sku_count']} SKU(s) with ≤{intel['threshold']:.0f} units "
            f"(min {r['min_qty']:.0f} units) — prioritise restock before weekend."
        ))

    # Highest-value OOS product — blue
    if intel["high_value_oos"]:
        hv = intel["high_value_oos"][0]
        bullets.append(_sb(_C_GAP,
            f"{hv['productName']} ({hv['categoryName']}, ₹{hv['sellingPrice']:.0f}) "
            f"— out of stock, fast-track reorder to protect category revenue."
        ))

    return _wrap_stock_html(bullets)


def product_stock_insight(
    store_name: str,
    stock_dir: str,
    low_stock_threshold: float = 5,
) -> str:
    """
    Generate a stock-alert insight block for individual products.
    Returned HTML is appended after product_recommendation() in the weekly report.
    Focuses on the most critical individual SKUs — highest selling price OOS first.
    """
    stock_df = _load_stock_csv(store_name, stock_dir)
    if stock_df.empty:
        return ""

    if "productName" not in stock_df.columns:
        return ""

    oos_df = stock_df[stock_df["quantity"] == 0].copy()
    low_df = stock_df[
        (stock_df["quantity"] > 0) & (stock_df["quantity"] <= low_stock_threshold)
    ].copy()
    neg_df = stock_df[stock_df["quantity"] < 0].copy()

    if oos_df.empty and low_df.empty and neg_df.empty:
        return ""

    # Top OOS by selling price — highest revenue risk
    top_oos = (
        oos_df[["productName", "brand", "categoryName", "sellingPrice", "vendorName"]]
        .sort_values("sellingPrice", ascending=False)
        .head(10)
        .to_dict("records")
    ) if not oos_df.empty else []

    # Top low-stock sorted by sellingPrice descending
    top_low = (
        low_df[["productName", "brand", "categoryName", "quantity", "sellingPrice", "vendorName"]]
        .sort_values("sellingPrice", ascending=False)
        .head(8)
        .to_dict("records")
    ) if not low_df.empty else []

    # Negative stock — most negative quantity first
    top_neg = (
        neg_df[["productName", "brand", "categoryName", "quantity", "sellingPrice", "vendorName"]]
        .sort_values("quantity")
        .head(10)
        .round(2)
        .to_dict("records")
    ) if not neg_df.empty else []

    import json as _json

    bullets = []

    # Negative stock — deep red
    for r in top_neg[:3]:
        bullets.append(_sb(_C_NEG,
            f"{r['productName']} (qty {r['quantity']:.0f}, vendor: {r.get('vendorName', 'unknown')}) "
            f"— negative stock, sold without GRN. Post GRN immediately."
        ))
    if len(top_neg) > 3:
        bullets.append(_sb(_C_NEG,
            f"{len(neg_df)} SKUs total have negative stock — run full GRN reconciliation today."
        ))

    # OOS products — dark orange
    for r in top_oos[:2]:
        bullets.append(_sb(_C_OOS,
            f"{r['productName']} (₹{r['sellingPrice']:.0f}, vendor: {r.get('vendorName', 'unknown')}) "
            f"— out of stock, reorder immediately."
        ))

    # Low-stock products — orange
    for r in top_low[:2]:
        bullets.append(_sb(_C_LOW,
            f"{r['productName']} (qty {r['quantity']:.0f}, ₹{r['sellingPrice']:.0f}, "
            f"vendor: {r.get('vendorName', 'unknown')}) — critically low, add to next reorder."
        ))

    # Pattern alert — purple
    from collections import Counter
    def _dominant(items, key):
        c = Counter(r.get(key, "") for r in items if r.get(key))
        top = c.most_common(1)
        return (top[0][0], top[0][1]) if top and top[0][1] >= 3 else None

    neg_pattern = _dominant(top_neg, "brand") or _dominant(top_neg, "vendorName")
    oos_pattern = _dominant(top_oos, "brand") or _dominant(top_oos, "vendorName")
    if neg_pattern:
        bullets.append(_sb(_C_PATTERN,
            f"{neg_pattern[0]}: {neg_pattern[1]} SKUs with negative stock "
            f"— systemic GRN failure, escalate to account manager."
        ))
    elif oos_pattern:
        bullets.append(_sb(_C_PATTERN,
            f"{oos_pattern[0]}: {oos_pattern[1]} SKUs out of stock "
            f"— systemic supply issue, escalate to account manager."
        ))

    return _wrap_stock_html(bullets)


_RTV_STYLE = """
<div style="
    background: #fce4ec;
    border-left: 5px solid #c2185b;
    border-radius: 6px;
    padding: 14px 18px;
    margin: 10px 0 24px 0;
    font-family: 'Segoe UI', sans-serif;
">
    <div style="font-size:15px; font-weight:bold; color:#880e4f; margin-bottom:8px;">
        {header}
    </div>
    <div style="font-size:14px; line-height:1.9;">
{body}
    </div>
</div>
"""

_C_RTV_HIGH   = "#b71c1c"
_C_RTV_VENDOR = "#880e4f"
_C_RTV_PROD   = "#ad1457"
_C_RTV_REASON = "#6a1b9a"

def _rtv_bullet(color: str, text: str) -> str:
    return f'<div style="color:{color}; margin:4px 0;">{text}</div>'


def rtv_insight(store_name: str, rtv_dir: str) -> str:
    """
    Generate a Return-to-Vendor alert block for a store.
    Returns "" silently if no RTV CSV exists for this store.

    Columns expected: rtvId, Date, time, vendorName, productId,
                      productName, barcode, quantity, price,
                      totalAmount, description, storeName, doneBy
    """
    safe_name  = store_name.replace("/", "_")
    candidates = [
        os.path.join(rtv_dir, f"{safe_name}.csv"),
        os.path.join(rtv_dir, f"{store_name}.csv"),
    ]

    df = pd.DataFrame()
    for path in candidates:
        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
            except Exception as e:
                print(f"RTV CSV read error for {store_name}: {e}")
                return ""
            break

    if df.empty:
        return ""

    for col in ["quantity", "price", "totalAmount"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    total_returns = len(df)
    total_qty     = int(df["quantity"].sum())
    total_value   = df["totalAmount"].sum()

    bullets = []

    # Summary
    bullets.append(_rtv_bullet(_C_RTV_HIGH,
        f"{total_returns} return line(s) today — {total_qty} units worth "
        f"₹{total_value:,.0f} sent back to vendors."
    ))

    # Top vendors by return value
    if "vendorName" in df.columns:
        vendor_summary = (
            df.groupby("vendorName")
            .agg(lines=("productName", "count"),
                 qty=("quantity", "sum"),
                 value=("totalAmount", "sum"))
            .sort_values("value", ascending=False)
            .reset_index()
        )
        for _, r in vendor_summary.head(2).iterrows():
            bullets.append(_rtv_bullet(_C_RTV_VENDOR,
                f"{r['vendorName']}: {int(r['lines'])} SKU(s), "
                f"{int(r['qty'])} units, ₹{r['value']:,.0f} returned today."
            ))

    # Highest value individual products
    if "productName" in df.columns and "totalAmount" in df.columns:
        top_products = (
            df[["productName", "quantity", "price", "totalAmount", "vendorName"]]
            .sort_values("totalAmount", ascending=False)
            .head(3)
        )
        for _, r in top_products.iterrows():
            vendor = r.get("vendorName", "unknown") if pd.notna(r.get("vendorName")) else "unknown"
            bullets.append(_rtv_bullet(_C_RTV_PROD,
                f"{r['productName']} — {int(r['quantity'])} units @ "
                f"₹{r['price']:,.0f}, total ₹{r['totalAmount']:,.0f} "
                f"(vendor: {vendor})."
            ))

    # Most common return reason
    if "description" in df.columns:
        reasons = df["description"].dropna().str.strip()
        reasons = reasons[reasons != ""]
        if not reasons.empty:
            from collections import Counter
            top_reason, count = Counter(reasons).most_common(1)[0]
            bullets.append(_rtv_bullet(_C_RTV_REASON,
                f"Most common return reason: \"{top_reason}\" "
                f"({count} occurrence(s)) — investigate with vendor to prevent recurrence."
            ))

    if not bullets:
        return ""

    return _RTV_STYLE.format(
        header="Return to Vendor (RTV) — Today's Summary",
        body="".join(bullets),
    )