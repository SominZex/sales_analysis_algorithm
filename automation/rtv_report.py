import requests
import base64
import pandas as pd
import os
from datetime import datetime, timedelta
import shutil
import argparse

LOGIN_URL = "https://api.example.in/login"
RTV_URL   = "https://api.example.in/rtvstore-stocks/RTVreport"

USERNAME = "pwd/user"
PASSWORD = "pwd/user"

STORE_FILE = "/base/url/partner.csv"
OUTPUT_DIR = "/base/url/store_rtv"   # safer disk


# ───────────────────────── LOGIN ─────────────────────────

def login():
    r = requests.post(LOGIN_URL, json={
        "username": USERNAME,
        "password": PASSWORD
    })

    # Try parsing JSON safely
    try:
        data = r.json()
    except Exception:
        raise Exception(f"Invalid login response: {r.text}")

    token = data.get("token")

    # ✅ Only check token existence (NOT status code)
    if not token:
        raise Exception(f"Login failed: {data}")

    return token

# ───────────────────────── DATE RANGE (WEEKLY) ─────────────────────────

def get_weekly_date_range(execution_date=None):
    """
    Returns a CLOSED 7-day window:
    T-7 → T-1
    """

    if execution_date:
        base_date = datetime.strptime(execution_date, "%Y-%m-%d").date()
    else:
        base_date = datetime.utcnow().date()

    end   = base_date - timedelta(days=1)   # yesterday
    start = end - timedelta(days=6)         # 7 days total

    return start.isoformat(), end.isoformat()


# ───────────────────────── FETCH RTV ─────────────────────────

def fetch_rtv_report(token, store_id, store_name, fromDate, toDate):
    headers = {
        "Authorization": token,
        "accept": "*/*"
    }

    params = {
        "storeId": str(store_id),
        "fromDate": fromDate,
        "toDate": toDate,
    }

    r = requests.get(RTV_URL, headers=headers, params=params)

    print(f"{store_name} -> Status {r.status_code}")

    if r.status_code != 200:
        raise Exception(f"API failed for {store_name}: {r.text}")

    if not r.text:
        raise Exception(f"Empty response for {store_name}")

    # Decode safely
    try:
        decoded = base64.b64decode(r.text)

        if len(decoded) < 100:
            raise Exception("Decoded file too small (invalid data)")

    except Exception as e:
        raise Exception(f"Decoding failed for {store_name}: {e}")

    # Atomic write
    filename = os.path.join(OUTPUT_DIR, f"{store_name}.csv")
    temp_file = filename + ".tmp"

    with open(temp_file, "wb") as f:
        f.write(decoded)

    os.rename(temp_file, filename)

    print(f"Saved {filename}")


# ───────────────────────── MAIN ─────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--execution_date",
        type=str,
        required=False,
        help="Airflow execution date (YYYY-MM-DD)"
    )
    args = parser.parse_args()

    # Clean previous run (idempotent)
    shutil.rmtree(OUTPUT_DIR, ignore_errors=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    stores = pd.read_csv(STORE_FILE)

    token = login()

    fromDate, toDate = get_weekly_date_range(args.execution_date)

    print(f"Fetching WEEKLY RTV from {fromDate} → {toDate}")

    failed = []

    for _, row in stores.iterrows():
        store_id = row["store"]
        store_name = str(row["storeName"]).replace("/", "_")

        try:
            fetch_rtv_report(token, store_id, store_name, fromDate, toDate)
        except Exception as e:
            print(f"❌ {store_name} failed: {e}")
            failed.append(store_name)

    if failed:
        raise Exception(f"RTV FAILED for stores: {failed}")

    print("✅ WEEKLY RTV COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    main()