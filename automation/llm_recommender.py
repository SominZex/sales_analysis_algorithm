import os
import json
import time
import requests
import pandas as pd
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

# ── Config ─────────────────────────────────────────────────────────────────────
GROQ_MODEL   = os.getenv("GROQ_MODEL",   "llama-3.1-8b-instant")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:3b")
OLLAMA_HOST  = os.getenv("OLLAMA_HOST",  "http://localhost:11434")

# ── Groq client (lazy init) ────────────────────────────────────────────────────
_client = None

def _get_client() -> Groq:
    """Lazy-init Groq client so import doesn't fail if key is missing at module load."""
    global _client
    if _client is None:
        api_key = os.getenv("GROQ_API_KEY")
        if not api_key:
            raise RuntimeError(
                "GROQ_API_KEY not set. Get a free key at https://console.groq.com "
                "and add it to your .env file."
            )
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


# ── PRIMARY: Groq ──────────────────────────────────────────────────────────────

def _call_groq(prompt: str) -> str:
    """Call Groq API. Raises exception on rate-limit or any failure."""
    client = _get_client()
    chat_completion = client.chat.completions.create(
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a retail sales analyst. Provide concise, specific, "
                    "actionable recommendations based on the sales data provided. "
                    "Respond only with bullet points. No preamble or closing remarks. "
                    "Give only 4 lines of response, prioritising the most important insights and actions."
                ),
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
        model=GROQ_MODEL,
        temperature=0.3,
        max_tokens=500,
    )
    return chat_completion.choices[0].message.content.strip()


# ── FALLBACK: Ollama local ─────────────────────────────────────────────────────

def _call_ollama(prompt: str) -> str:
    """Call local Ollama (llama3.2:3b). Raises exception if Ollama is not running."""
    system = (
        "You are a retail sales analyst. Provide concise, specific, "
        "actionable recommendations based on the sales data provided. "
        "Respond only with bullet points. No preamble or closing remarks. "
        "Give only 4 lines of response, prioritising the most important insights and actions."
    )
    full_prompt = f"[INST] <<SYS>>\n{system}\n<</SYS>>\n\n{prompt} [/INST]"

    resp = requests.post(
        f"{OLLAMA_HOST}/api/generate",
        json={
            "model": OLLAMA_MODEL,
            "prompt": full_prompt,
            "stream": False,
            "options": {
                "temperature": 0.3,
                "num_predict": 500,
            },
        },
        timeout=300,  # 5 min — CPU inference is slow
    )
    resp.raise_for_status()
    return resp.json().get("response", "").strip()


# ── Dispatcher: try Groq → retry once → fallback to Ollama ────────────────────

def _get_recommendation(prompt: str) -> tuple:
    """
    Returns (recommendation_text, used_fallback).
    Tries Groq first (with 1 retry on rate limit).
    Falls back to Ollama only if Groq fails twice.
    """
    # ── Try Groq (up to 2 attempts) ───────────────────────────────────────────
    for attempt in range(2):
        try:
            text = _call_groq(prompt)
            if text:
                print(f"      ✅ Groq ({GROQ_MODEL}) response received.")
                time.sleep(2)  # 2s pace — stays within 30 RPM limit
                return text, False
        except Exception as e:
            err = str(e).lower()
            if "rate_limit" in err or "429" in err or "quota" in err:
                if attempt == 0:
                    print("      ⚠️  Groq rate limit hit — waiting 30s then retrying...")
                    time.sleep(30)
                    continue
                else:
                    print("      ⚠️  Groq rate limit persists — switching to Ollama fallback.")
            else:
                print(f"      ⚠️  Groq failed: {str(e)[:80]} — switching to Ollama fallback.")
            break

    # ── Fallback: Ollama ───────────────────────────────────────────────────────
    try:
        print(f"      🔄 Calling Ollama ({OLLAMA_MODEL}) at {OLLAMA_HOST} ...")
        text = _call_ollama(prompt)
        if text:
            print(f"      ✅ Ollama ({OLLAMA_MODEL}) response received.")
            return text, True
    except requests.exceptions.ConnectionError:
        print(f"      ❌ Ollama not reachable at {OLLAMA_HOST}. Run: ollama serve")
    except Exception as e:
        print(f"      ❌ Ollama also failed: {str(e)[:80]}")

    return "", False  # both failed — report still generates with unavailable block


def _wrap_html(text: str, used_fallback: bool = False) -> str:
    """Wrap recommendation in styled HTML. Header shows which engine was used."""
    if not text:
        return _UNAVAILABLE
    header = (
        f"New Shop AI Recommendation"
        if used_fallback else
        f"New Shop AI Recommendation"
    )
    return _REC_STYLE.format(header=header, body=text)


# ── Data helpers ───────────────────────────────────────────────────────────────

def _top_n_as_list(df: pd.DataFrame, name_col: str, value_col: str, n: int = 5) -> list:
    """Return top-N rows as a list of dicts with clean numeric values."""
    if df.empty or name_col not in df.columns or value_col not in df.columns:
        return []
    sub = df[[name_col, value_col]].head(n).copy()
    sub[value_col] = (
        sub[value_col]
        .astype(str)
        .str.replace("%", "", regex=False)
        .apply(lambda x: float(x) if x.replace(".", "").replace("-", "").isdigit() else 0)
    )
    return sub.to_dict("records")


def _low_margin_list(df: pd.DataFrame, name_col: str, margin_col: str, n: int = 3) -> list:
    """Return the N rows with the lowest profit margin."""
    if df.empty or margin_col not in df.columns:
        return []
    tmp = df[[name_col, margin_col]].copy()
    tmp["_margin"] = (
        tmp[margin_col].astype(str).str.replace("%", "", regex=False)
        .apply(lambda x: float(x) if x.replace(".", "").replace("-", "").isdigit() else 0)
    )
    return (
        tmp.nsmallest(n, "_margin")[[name_col, "_margin"]]
        .rename(columns={"_margin": "profit_margin_%"})
        .to_dict("records")
    )


# ── Public API ─────────────────────────────────────────────────────────────────

def brand_recommendation(
    store_name: str,
    brand_df: pd.DataFrame,
    total_sales: float,
    report_type: str = "weekly",
) -> str:
    """Generate HTML recommendation block for the Brands section.
    Pass the DataFrame BEFORE the '%' suffix is added to columns."""
    top_brands = _top_n_as_list(brand_df, "brandName", "total_sales", 5)
    low_margin = (
        _low_margin_list(brand_df, "brandName", "profit_margin", 3) or
        _low_margin_list(brand_df, "brandName", "PROFIT_MARGIN", 3)
    )

    prompt = f"""Analyse the {report_type} brand performance for store "{store_name}".

Total {report_type} sales: Rs.{total_sales:,.2f}
Top 5 brands by revenue: {json.dumps(top_brands, indent=2)}
Brands with lowest profit margin: {json.dumps(low_margin, indent=2)}

Write 4 bullet points covering:
- Which brands to prioritise and why
- Brands with low profit margin needing pricing or cost review
- Any brand with high quantity but weak revenue (possible under-pricing)
- Specific actions the store manager should take this {report_type}"""

    text, fallback = _get_recommendation(prompt)
    return _wrap_html(text, fallback)


def category_recommendation(
    store_name: str,
    category_df: pd.DataFrame,
    total_sales: float,
    report_type: str = "weekly",
) -> str:
    """Generate HTML recommendation block for the Categories section."""
    top_cats   = _top_n_as_list(category_df, "categoryName", "total_sales", 5)
    low_margin = (
        _low_margin_list(category_df, "categoryName", "profit_margin", 3) or
        _low_margin_list(category_df, "categoryName", "PROFIT_MARGIN", 3)
    )
    top_share = round((top_cats[0]["total_sales"] / total_sales * 100), 1) if top_cats and total_sales > 0 else 0

    prompt = f"""Analyse the {report_type} category performance for store "{store_name}".

Total {report_type} sales: Rs.{total_sales:,.2f}
Top category contributes {top_share}% of total sales.
Top 5 categories by revenue: {json.dumps(top_cats, indent=2)}
Categories with lowest profit margin: {json.dumps(low_margin, indent=2)}

Write 4 bullet points covering:
- Dominant categories and how to grow them further
- Under-performing or low-margin categories needing attention
- Whether the store is over-reliant on one category (concentration risk)
- Specific actions the store manager should take this {report_type}"""

    text, fallback = _get_recommendation(prompt)
    return _wrap_html(text, fallback)


def product_recommendation(
    store_name: str,
    product_df: pd.DataFrame,
    total_sales: float,
    report_type: str = "weekly",
) -> str:
    """Generate HTML recommendation block for the Products section."""
    top_products    = _top_n_as_list(product_df, "productName", "total_sales", 5)
    low_margin      = (
        _low_margin_list(product_df, "productName", "profit_margin", 3) or
        _low_margin_list(product_df, "productName", "PROFIT_MARGIN", 3)
    )
    bottom_products = _top_n_as_list(
        product_df.tail(5).reset_index(drop=True), "productName", "total_sales", 5
    )

    prompt = f"""Analyse the {report_type} product performance for store "{store_name}".

Total {report_type} sales: Rs.{total_sales:,.2f}
Top 5 products by revenue: {json.dumps(top_products, indent=2)}
Products with lowest profit margin: {json.dumps(low_margin, indent=2)}
Slowest-selling products: {json.dumps(bottom_products, indent=2)}

Write 4 bullet points covering:
- Star products to ensure are always in stock
- Products with very low or negative margin (reprice or discontinue?)
- Slow movers that need promotions or clearance action
- Specific actions the store manager should take this {report_type}"""

    text, fallback = _get_recommendation(prompt)
    return _wrap_html(text, fallback)