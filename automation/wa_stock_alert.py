"""
wa_stock_alert.py
─────────────────────────────────────────────────────────────────────────────
Reads per-store stock CSVs (from store_stocks/), computes the same low-stock
and negative-stock intelligence used by the weekly PDF report, and sends a
plain-text WhatsApp alert to each store's business partner via the Twilio
WhatsApp API.

Prerequisites
─────────────
pip install twilio pandas python-dotenv

.env variables required
───────────────────────
TWILIO_ACCOUNT_SID  = ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
TWILIO_AUTH_TOKEN   = your_auth_token
TWILIO_WA_FROM      = whatsapp:+14155238886   (Twilio sandbox or approved number)

Optional .env overrides
───────────────────────
STOCK_DIR           = store_stocks       (default)
PARTNER_FILE        = partner.csv        (default)
LOW_STOCK_THRESHOLD = 5                  (default)

Run
───
python wa_stock_alert.py
"""

import os
import re
import time
import pandas as pd
from collections import Counter
from dotenv import load_dotenv
from twilio.rest import Client

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
TWILIO_ACCOUNT_SID  = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN   = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_WA_FROM      = os.getenv("TWILIO_WA_FROM", "whatsapp:+14155238886")  # sandbox default

STOCK_DIR           = os.getenv("STOCK_DIR", "store_stocks")
PARTNER_FILE        = os.getenv("PARTNER_FILE", "partner.csv")
LOW_STOCK_THRESHOLD = float(os.getenv("LOW_STOCK_THRESHOLD", "5"))

# Seconds to wait between messages to respect API rate limits
SEND_DELAY = 2


# ── Helpers ───────────────────────────────────────────────────────────────────

def _normalise_wa_number(raw: str) -> str:
    """
    Normalise to Twilio WhatsApp format: 'whatsapp:+919876543210'
    Accepts numbers with or without country code, spaces, dashes, parentheses.
    """
    digits = re.sub(r"[\s\-\(\)]", "", str(raw))
    # Ensure it starts with + for E.164 format
    if not digits.startswith("+"):
        digits = "+" + digits
    return f"whatsapp:{digits}"


def _load_stock_csv(store_name: str) -> pd.DataFrame:
    safe_name  = store_name.replace("/", "_")
    candidates = [
        os.path.join(STOCK_DIR, f"{safe_name}.csv"),
        os.path.join(STOCK_DIR, f"{store_name}.csv"),
    ]
    for path in candidates:
        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
                for col in ["quantity", "sellingPrice", "costPrice"]:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
                return df
            except Exception as e:
                print(f"  ⚠️  CSV read error for {store_name}: {e}")
                return pd.DataFrame()
    return pd.DataFrame()


def _dominant(items: list, key: str):
    """Return (name, count) if any value appears 3+ times, else None."""
    c = Counter(r.get(key, "") for r in items if r.get(key))
    top = c.most_common(1)
    return (top[0][0], top[0][1]) if top and top[0][1] >= 3 else None


# ── Message builder ───────────────────────────────────────────────────────────

def build_message(store_name: str, df: pd.DataFrame, threshold: float) -> str:
    """
    Build a plain-text WhatsApp message with negative-stock and low-stock
    alerts, mirroring the PDF report logic exactly.
    Returns "" if there is nothing to report.
    """
    neg_df = df[df["quantity"] < 0].copy()
    low_df = df[(df["quantity"] > 0) & (df["quantity"] <= threshold)].copy()

    if neg_df.empty and low_df.empty:
        return ""

    lines = []
    lines.append(f"⚠️ *Stock Alert — {store_name}*")
    lines.append(f"_{pd.Timestamp.today().strftime('%d %b %Y')}_")
    lines.append("")

    # ── NEGATIVE STOCK (GRN anomaly) ─────────────────────────────────────────
    if not neg_df.empty:
        lines.append("*🔴 NEGATIVE STOCK (Sold Without GRN)*")

        # Brand-level summary
        brand_neg = (
            neg_df.groupby("brand")
            .agg(sku_count=("productName", "count"), min_qty=("quantity", "min"))
            .sort_values("sku_count", ascending=False)
            .reset_index()
            .head(3)
        ) if "brand" in neg_df.columns else pd.DataFrame()

        for _, r in brand_neg.iterrows():
            lines.append(
                f"  • {r['brand']}: {int(r['sku_count'])} SKU(s), "
                f"worst qty {int(r['min_qty'])} — post GRN immediately."
            )

        # Individual products — most negative first
        top_neg = (
            neg_df[["productName", "quantity", "vendorName"]]
            .sort_values("quantity")
            .head(5)
        ) if "productName" in neg_df.columns else pd.DataFrame()

        if not top_neg.empty:
            lines.append("")
            lines.append("  _Top affected products:_")
            for _, r in top_neg.iterrows():
                vendor = r.get("vendorName", "unknown")
                vendor = vendor if pd.notna(vendor) else "unknown"
                lines.append(
                    f"    - {r['productName']} "
                    f"(qty {int(r['quantity'])}, vendor: {vendor})"
                )

        # Systemic pattern
        top_neg_list = neg_df.to_dict("records")
        pattern = (
            _dominant(top_neg_list, "brand") or
            _dominant(top_neg_list, "vendorName")
        )
        if pattern:
            lines.append(
                f"\n  🚨 *{pattern[0]}* has {pattern[1]} negative-stock SKUs "
                f"— systemic GRN failure, escalate to account manager."
            )

        lines.append(
            f"\n  Total negative-stock SKUs: *{len(neg_df)}* — "
            f"run full GRN reconciliation today."
        )
        lines.append("")

    # ── LOW STOCK ─────────────────────────────────────────────────────────────
    if not low_df.empty:
        lines.append(f"*🟡 LOW STOCK (≤{threshold:.0f} units)*")

        # Brand-level summary
        brand_low = (
            low_df.groupby("brand")
            .agg(sku_count=("productName", "count"), min_qty=("quantity", "min"))
            .sort_values("sku_count", ascending=False)
            .reset_index()
            .head(3)
        ) if "brand" in low_df.columns else pd.DataFrame()

        for _, r in brand_low.iterrows():
            lines.append(
                f"  • {r['brand']}: {int(r['sku_count'])} SKU(s), "
                f"min {int(r['min_qty'])} unit(s) — request top-up this week."
            )

        # Individual products — highest selling price first (revenue risk)
        top_low = (
            low_df[["productName", "quantity", "sellingPrice", "vendorName"]]
            .sort_values("sellingPrice", ascending=False)
            .head(5)
        ) if "productName" in low_df.columns else pd.DataFrame()

        if not top_low.empty:
            lines.append("")
            lines.append("  _Highest-value low-stock products:_")
            for _, r in top_low.iterrows():
                vendor = r.get("vendorName", "unknown")
                vendor = vendor if pd.notna(vendor) else "unknown"
                lines.append(
                    f"    - {r['productName']} "
                    f"(qty {int(r['quantity'])}, ₹{r['sellingPrice']:.0f}, "
                    f"vendor: {vendor})"
                )

        lines.append(
            f"\n  Total low-stock SKUs: *{len(low_df)}* — add to next reorder."
        )

    return "\n".join(lines)


# ── WhatsApp sender ───────────────────────────────────────────────────────────

def send_whatsapp(wa_number: str, message: str) -> bool:
    """Send a plain-text WhatsApp message via Twilio. Returns True on success."""
    if not TWILIO_ACCOUNT_SID or not TWILIO_AUTH_TOKEN:
        raise RuntimeError(
            "TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN must be set in .env before sending."
        )
    try:
        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        msg = client.messages.create(
            from_=TWILIO_WA_FROM,
            to=wa_number,
            body=message,
        )
        # Twilio returns a status — 'queued' or 'sent' are both success
        if msg.sid:
            return True
        return False
    except Exception as e:
        print(f"    ❌ Twilio error: {e}")
        return False


# ── Weekly report link sender ─────────────────────────────────────────────────

def send_report_links():
    """
    Reads the pdf_link column from partner.csv and sends each store's
    PDF report link to its business partner via WhatsApp.
    Skips rows where pdf_link is empty or missing.
    """
    if not os.path.exists(PARTNER_FILE):
        print(f"⚠️  {PARTNER_FILE} not found — skipping report link delivery.")
        return

    partners = pd.read_csv(PARTNER_FILE)

    if "pdf_link" not in partners.columns:
        print("⚠️  No 'pdf_link' column found in partner.csv — skipping report link delivery.")
        return

    # Only rows that have both a wa_number and a pdf_link
    sendable = partners.dropna(subset=["wa_number", "pdf_link"])
    sendable = sendable[sendable["pdf_link"].str.strip() != ""]

    if sendable.empty:
        print("ℹ️  No PDF links found in partner.csv — skipping report link delivery.")
        return

    print(f"\n📎 Sending weekly report links to {len(sendable)} partner(s)...\n")

    sent = skipped = failed = 0

    for _, row in sendable.iterrows():
        store_name = str(row["storeName"])
        wa_number  = _normalise_wa_number(row["wa_number"])
        pdf_link   = str(row["pdf_link"]).strip()

        message = (
            f"📊 *Weekly Store Report — {store_name}*\n"
            f"_{pd.Timestamp.today().strftime('%d %b %Y')}_\n\n"
            f"Your weekly performance report is ready.\n"
            f"Tap the link below to view or download it:\n\n"
            f"{pdf_link}\n\n"
            f"_This link is valid for 7 days._"
        )

        print(f"▶ {store_name} → {wa_number}")
        success = send_whatsapp(wa_number, message)
        if success:
            print(f"  ✅ Report link sent.\n")
            sent += 1
        else:
            print(f"  ❌ Failed to send report link.\n")
            failed += 1

        time.sleep(SEND_DELAY)

    print(f"📊 Report links — {sent} sent, {failed} failed, {skipped} skipped.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    # Load partner mapping
    if not os.path.exists(PARTNER_FILE):
        raise FileNotFoundError(f"Partner file not found: {PARTNER_FILE}")

    partners = pd.read_csv(PARTNER_FILE)
    required = {"storeName", "wa_number"}
    if not required.issubset(partners.columns):
        raise ValueError(f"partner.csv must have columns: {required}")

    # Drop rows with no WA number
    partners = partners.dropna(subset=["wa_number"])
    partners["wa_number"] = partners["wa_number"].apply(_normalise_wa_number)

    print(f"📋 Loaded {len(partners)} partner(s) from {PARTNER_FILE}\n")

    sent = 0
    skipped = 0
    no_alert = 0

    for _, row in partners.iterrows():
        store_name = str(row["storeName"])
        wa_number  = row["wa_number"]

        print(f"▶ {store_name} → {wa_number}")

        # Load stock CSV
        df = _load_stock_csv(store_name)
        if df.empty:
            print(f"  ℹ️  No stock CSV found — skipping.\n")
            skipped += 1
            continue

        # Build message
        message = build_message(store_name, df, LOW_STOCK_THRESHOLD)
        if not message:
            print(f"  ✅ No low/negative stock — nothing to send.\n")
            no_alert += 1
            continue

        # Send
        print(f"  📤 Sending alert...")
        success = send_whatsapp(wa_number, message)
        if success:
            print(f"  ✅ Sent.\n")
            sent += 1
        else:
            print(f"  ❌ Failed to send.\n")

        time.sleep(SEND_DELAY)

    print(
        f"\n📊 Done — {sent} sent, {no_alert} no alert needed, "
        f"{skipped} skipped (no CSV)."
    )

    # ── Send weekly PDF report links ──────────────────────────────────────────
    print("\n" + "─" * 60)
    send_report_links()


if __name__ == "__main__":
    main()