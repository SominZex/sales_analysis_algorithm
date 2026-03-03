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

# ── Config ─────────────────────────────────────────────────────────────────────
GROQ_MODEL   = os.getenv("GROQ_MODEL",   "llama-3.1-8b-instant")
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


# ── HTML wrapper ───────────────────────────────────────────────────────────────
_REC_STYLE = """
<div style="
    background: #f0f7ff;
    border-left: 5px solid #0078d7;
    border-radius: 6px;
    padding: 14px 18px;
    margin: 10px 0 24px 0;
    font-family: 'Segoe UI', sans-serif;
">
    <div style="font-size:15px; font-weight:bold; color:#0078d7; margin-bottom:8px;">
        {header}
    </div>
    <div style="font-size:14px; color:#333; line-height:1.7; white-space:pre-line;">
{body}
    </div>
</div>
"""

_UNAVAILABLE = _REC_STYLE.format(
    header="New Shop AI Recommendation — Unavailable",
    body="Could not generate recommendation. "
         "Check GROQ_API_KEY in .env and that Ollama is running (ollama serve)."
)

_BULLET_RULES = (
    "LENGTH RULE: Each bullet must be exactly ONE sentence. "
    "Maximum 2 items per bullet — pick the most important only. Do not chain multiple items. "
    "If a section list is empty, draw an insight from another section instead of writing 'none detected'."
)


# ── LLM calls ──────────────────────────────────────────────────────────────────

def _call_groq(prompt: str) -> str:
    client = _get_client()
    chat_completion = client.chat.completions.create(
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a retail sales analyst writing bullet points for a store manager. "
                    "You will receive pre-computed intelligence with exact names and numbers. "
                    "STRICT RULES: "
                    "1. NEVER write a name not present verbatim in the data. If unsure, skip it. "
                    "2. NEVER invent or calculate any number — copy numbers directly from the data only. "
                    "3. Each bullet is ONE sentence maximum. Do not chain multiple items in one bullet. "
                    "4. No preamble, no closing remarks, bullet points only."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        model=GROQ_MODEL,
        temperature=0.2,
        max_tokens=500,
    )
    return chat_completion.choices[0].message.content.strip()


def _call_ollama(prompt: str) -> str:
    system = (
        "You are a retail sales analyst. Convert pre-computed facts into bullet points. "
        "One sentence per bullet. Only use names and numbers from the data. No preamble or closing remarks."
    )
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
                print(f"      ✅ Groq ({GROQ_MODEL}) response received.")
                time.sleep(2)
                return text, False
        except Exception as e:
            err = str(e).lower()
            if "rate_limit" in err or "429" in err or "quota" in err:
                if attempt == 0:
                    print("      ⚠️  Groq rate limit — waiting 30s then retrying...")
                    time.sleep(30)
                    continue
                else:
                    print("      ⚠️  Groq rate limit persists — switching to Ollama.")
            else:
                print(f"      ⚠️  Groq failed: {str(e)[:80]} — switching to Ollama.")
            break
    try:
        print(f"      🔄 Calling Ollama ({OLLAMA_MODEL})...")
        text = _call_ollama(prompt)
        if text:
            print(f"      ✅ Ollama ({OLLAMA_MODEL}) response received.")
            return text, True
    except requests.exceptions.ConnectionError:
        print(f"      ❌ Ollama not reachable. Run: ollama serve")
    except Exception as e:
        print(f"      ❌ Ollama failed: {str(e)[:80]}")
    return "", False


def _wrap_html(text: str, used_fallback: bool = False) -> str:
    if not text:
        return _UNAVAILABLE
    return _REC_STYLE.format(header="New Shop AI Recommendation", body=text)


# ══════════════════════════════════════════════════════════════════════════════
# ── WEEKLY SNAPSHOT ───────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

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
        print(f"      💾 Snapshot saved for {store_name} (week: {week_start})")

    except Exception as e:
        print(f"      ⚠️  Snapshot save failed (non-critical): {e}")


# ══════════════════════════════════════════════════════════════════════════════
# ── MONTHLY SNAPSHOT ──────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

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
        print(f"      💾 Monthly snapshot saved for {store_name} (month: {month_start})")

    except Exception as e:
        print(f"      ⚠️  Monthly snapshot save failed (non-critical): {e}")


# ══════════════════════════════════════════════════════════════════════════════
# ── WoW TREND ─────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

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
        print(f"      ⚠️  WoW fetch failed (non-critical): {e}")
        return {}


# ══════════════════════════════════════════════════════════════════════════════
# ── MoM TREND ─────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

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
        print(f"      ⚠️  MoM fetch failed (non-critical): {e}")
        return {}


def _compute_trend(current_val: float, prev_val: float):
    if prev_val == 0:
        return None
    pct = ((current_val - prev_val) / prev_val) * 100
    return f"{'+' if pct >= 0 else ''}{pct:.1f}%"


# ══════════════════════════════════════════════════════════════════════════════
# ── INTELLIGENCE ENGINE ────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

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
    bot10_qty  = df[df["_qty"] > 0].nsmallest(10, "_qty")[[name_col, "_qty", "_sales", "_margin"]].round(2)
    low_margin = df[df["_margin"] < min(avg_margin, 15)].nsmallest(5, "_margin")[[name_col, "_margin", "_sales"]].round(2)

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

    # ── Trend signals (works for both WoW and MoM) ────────────────────────────
    declining     = []
    margin_shifts = []
    if trend_data:
        for _, row in df.iterrows():
            name = row[name_col]
            if name not in trend_data:
                continue
            prev         = trend_data[name]
            sales_change = _compute_trend(row["_sales"], prev["prev_sales"])
            qty_change   = _compute_trend(row["_qty"],   prev["prev_qty"])
            margin_shift = round(row["_margin"] - prev["prev_margin"], 2) if not np.isnan(row["_margin"]) else None

            if sales_change and float(sales_change.replace("+", "")) < -10:
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

        declining.sort(key=lambda x: float(x["sales_change"].replace("+", "")))
        declining     = declining[:3]
        margin_shifts = margin_shifts[:3]

    return {
        "top_10_by_revenue":      top10.to_dict("records"),
        "bottom_10_by_quantity":  bot10_qty.to_dict("records"),
        "low_margin_items":       low_margin.to_dict("records"),
        "hidden_margin_gems":     hidden_gems.to_dict("records"),
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
    }


# ══════════════════════════════════════════════════════════════════════════════
# ── Public API ─────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

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
    conc = (f"⚠️ Top 3 brands = {intel['top3_revenue_share_pct']}% of revenue — over-concentration"
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

    prompt = f"""Store: "{store_name}" | {report_type} | Total Sales: Rs.{total_sales:,.2f} | Avg margin: {intel['avg_margin_pct']}%

USE ONLY BRAND NAMES LISTED BELOW. Do not add any other brand names.

TOP 10 BRANDS by revenue:
{json.dumps(intel['top_10_by_revenue'], indent=2)}

BOTTOM 10 BRANDS by quantity sold:
{json.dumps(intel['bottom_10_by_quantity'], indent=2)}

LOW MARGIN BRANDS:
{json.dumps(intel['low_margin_items'], indent=2)}

HIGH MARGIN UNDERUTILISED (not in top 10 revenue — push these):
{json.dumps(intel['hidden_margin_gems'], indent=2)}

HIGH RISK BRANDS (high revenue share + low margin):
{json.dumps(intel['high_risk_items'], indent=2)}

MIX SHIFT RISK:
{json.dumps(intel['mix_risk_items'], indent=2)}

CONCENTRATION: {conc}
ANOMALIES: {json.dumps(intel['anomalies']) if intel['anomalies'] else "None"}
{trend_section}

{_BULLET_RULES}
Write exactly 5 bullets:

1. [STOCK PRIORITY] Top 2 brands by revenue only — name + Rs. revenue + margin % + one action:
   margin >30% → "reorder immediately and increase shelf space"
   margin 15-30% → "maintain stock and run a {period_label}ly combo offer"
   margin <15% → "keep stocked but negotiate cost before next order"

2. [MARGIN RISK] Single most urgent brand from LOW MARGIN or HIGH RISK — name + margin % + one action:
   margin <5% → "raise price by Rs.3-5 or discontinue if volume is low"
   margin 5-10% → "raise price by Rs.2-3 or renegotiate supplier cost this {period_label}"
   margin 10-15% → "small cost reduction here will significantly lift profit"

3. [HIDDEN OPPORTUNITY] Single best brand from HIGH MARGIN UNDERUTILISED — name + margin % + one action:
   "Place at counter or bundle with [#1 revenue brand] this {report_type}"

4. [DEAD STOCK] Top 2 worst from BOTTOM QUANTITY — name + quantity + one action:
   qty <=10 → "do not reorder — shelf space has better use"
   qty 11-30 → "run a 2-for-1 offer to clear this {report_type}"
   qty 31-60 → "move to counter for impulse purchase visibility"

5. [TREND / RISK] Use {trend_label} data if available, otherwise use MIX SHIFT or CONCENTRATION:
   If {trend_label} declining brands exist → name the worst + sales_change % + "investigate cause and consider reducing reorder quantity"
   If {trend_label} margin shifts exist → name the brand + margin shift direction + "review supplier cost or pricing immediately"
   If no {trend_label} data → use concentration or mix shift — cite exact figures and one action"""

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
    conc = (f"⚠️ Top 3 categories = {intel['top3_revenue_share_pct']}% of revenue — over-concentration"
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

    prompt = f"""Store: "{store_name}" | {report_type} | Total Sales: Rs.{total_sales:,.2f} | Avg margin: {intel['avg_margin_pct']}%

USE ONLY CATEGORY NAMES LISTED BELOW. Do not add any other category names.

TOP 10 CATEGORIES by revenue:
{json.dumps(intel['top_10_by_revenue'], indent=2)}

BOTTOM 10 CATEGORIES by quantity sold:
{json.dumps(intel['bottom_10_by_quantity'], indent=2)}

LOW MARGIN CATEGORIES:
{json.dumps(intel['low_margin_items'], indent=2)}

HIGH MARGIN UNDERUTILISED:
{json.dumps(intel['hidden_margin_gems'], indent=2)}

HIGH RISK CATEGORIES:
{json.dumps(intel['high_risk_items'], indent=2)}

MIX SHIFT RISK:
{json.dumps(intel['mix_risk_items'], indent=2)}

CONCENTRATION: {conc}
ANOMALIES: {json.dumps(intel['anomalies']) if intel['anomalies'] else "None"}
{trend_section}

{_BULLET_RULES}
Write exactly 5 bullets:

1. [REVENUE DRIVER] Top 2 categories by revenue — name + Rs. revenue + contrib% + one action:
   contrib >20% → "protect this category — ensure full range is stocked"
   contrib 10-20% → "grow this category — add SKU variety or a promotional bundle"
   contrib <10% → "review range — reduce low-margin SKUs in this category"

2. [MARGIN RISK] Single most urgent from LOW MARGIN or HIGH RISK — name + margin % + one action:
   margin <5% → "audit all SKUs — remove loss-makers immediately"
   margin 5-10% → "raise prices by Rs.2-5 or negotiate supplier terms this {period_label}"
   margin 10-15% → "review top 3 SKUs for cost reduction opportunity"

3. [HIDDEN OPPORTUNITY] Single best from HIGH MARGIN UNDERUTILISED — name + margin % + one action:
   "Increase shelf space or bundle with top revenue category to drive volume this {report_type}"

4. [LOW DEMAND] Top 2 from BOTTOM QUANTITY — name + quantity + one action:
   qty <=10 → "review whether this category earns its shelf space"
   qty 11-50 → "run a category promotion or move to higher-traffic position"
   qty 51-100 → "add a combo deal with a high-revenue category to lift volume"

5. [TREND / RISK] Use {trend_label} data if available, otherwise use MIX SHIFT or CONCENTRATION:
   If {trend_label} declining categories exist → name the worst + sales_change % + "investigate cause and consider reducing SKU range"
   If {trend_label} margin shifts exist → name the category + shift direction + "review pricing or supplier terms immediately"
   If no {trend_label} data → use concentration or mix shift — cite exact figures and one action"""

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
    conc = (f"⚠️ Top 3 products = {intel['top3_revenue_share_pct']}% of revenue — over-concentration"
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

    prompt = f"""Store: "{store_name}" | {report_type} | Total Sales: Rs.{total_sales:,.2f} | Avg margin: {intel['avg_margin_pct']}%

USE ONLY PRODUCT NAMES LISTED BELOW. Do not add any other product names.

TOP 10 PRODUCTS by revenue:
{json.dumps(intel['top_10_by_revenue'], indent=2)}

BOTTOM 10 PRODUCTS by quantity sold:
{json.dumps(intel['bottom_10_by_quantity'], indent=2)}

LOW MARGIN PRODUCTS:
{json.dumps(intel['low_margin_items'], indent=2)}

HIGH MARGIN UNDERUTILISED:
{json.dumps(intel['hidden_margin_gems'], indent=2)}

HIGH RISK PRODUCTS:
{json.dumps(intel['high_risk_items'], indent=2)}

MIX SHIFT RISK:
{json.dumps(intel['mix_risk_items'], indent=2)}

CONCENTRATION: {conc}
ANOMALIES: {json.dumps(intel['anomalies']) if intel['anomalies'] else "None"}
{trend_section}

{_BULLET_RULES}
Write exactly 5 bullets:

1. [STOCK PRIORITY] Top 2 products by revenue — name + Rs. revenue + margin % + one action:
   margin >30% → "reorder immediately and increase shelf space"
   margin 15-30% → "maintain stock and consider a {period_label}ly combo offer"
   margin <15% → "keep stocked but raise price by Rs.2-3 before next reorder"

2. [MARGIN RISK] Single most urgent from LOW MARGIN or HIGH RISK — name + margin % + one action:
   margin <0% → "STOP selling at current price — raise price or remove from shelf immediately"
   margin 0-5% → "raise price by Rs.3-5 or remove if supplier cost cannot be reduced"
   margin 5-10% → "negotiate supplier cost — 2% reduction improves margin meaningfully"
   margin 10-15% → "small price adjustment will significantly lift profit"

3. [HIDDEN OPPORTUNITY] Single best from HIGH MARGIN UNDERUTILISED — name + margin % + one action:
   "Move to eye level or counter, or bundle with [#1 revenue product] for an upsell offer this {report_type}"

4. [DEAD STOCK] Top 2 from BOTTOM QUANTITY — name + quantity + one action:
   qty ==1 → "single unit sold — do not reorder"
   qty <=5 → "consider removing from range — shelf space has better use"
   qty 6-20 → "run a 2-for-1 or discount offer this {report_type}"
   qty 21-50 → "move to checkout counter for impulse purchase visibility"

5. [TREND / RISK] Use {trend_label} data if available, otherwise use MIX SHIFT or CONCENTRATION:
   If {trend_label} declining products exist → name the worst + sales_change % + "investigate cause and consider reducing reorder quantity"
   If {trend_label} margin shifts exist → name the product + shift direction + "review supplier cost or pricing immediately"
   If no {trend_label} data → use concentration or mix shift — cite exact figures and one action"""

    text, fallback = _get_recommendation(prompt)
    return _wrap_html(text, fallback)