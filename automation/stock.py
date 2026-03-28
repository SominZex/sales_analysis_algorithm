import requests
import base64
import pandas as pd
import os
import shutil
import argparse
from datetime import datetime

LOGIN_URL = "https://api.example.in/login"
STOCK_URL = "https://api.example.in/store-stocks/report/storeStocksCSV"

USERNAME = "user/pwd"
PASSWORD = "user/pwd"

STORE_FILE = "/base/dir/partner.csv"
OUTPUT_DIR = "/base/dir/store_stocks"


# ───────────────────────── LOGIN ─────────────────────────

def login():
    r = requests.post(LOGIN_URL, json={
        "username": USERNAME,
        "password": PASSWORD
    })

    try:
        data = r.json()
    except Exception:
        raise Exception(f"Invalid login response: {r.text}")

    # ✅ Check token presence instead of status code
    token = data.get("token")

    if not token:
        raise Exception(f"Login failed: {data}")

    return token
# ───────────────────────── FETCH STOCK ─────────────────────────

def fetch_stock_report(token, store_id, store_name, run_date):
    headers = {
        "Authorization": token,
        "accept": "*/*"
    }

    params = {
        "store": int(store_id)
    }

    r = requests.get(STOCK_URL, headers=headers, params=params)

    print(f"{store_name} -> Status {r.status_code}")

    if r.status_code != 200:
        raise Exception(f"API failed for {store_name}: {r.text}")

    if not r.text:
        raise Exception(f"Empty response for {store_name}")

    try:
        decoded = base64.b64decode(r.text)

        if len(decoded) < 100:
            raise Exception("Decoded file too small (invalid data)")

    except Exception as e:
        raise Exception(f"Decoding failed for {store_name}: {e}")

    # Convert to DataFrame to tag snapshot date (CRITICAL)
    try:
        from io import StringIO
        df = pd.read_csv(StringIO(decoded.decode("utf-8")))

        # Add snapshot date column
        df["snapshot_date"] = run_date

        encoded = df.to_csv(index=False).encode("utf-8")

    except Exception as e:
        raise Exception(f"CSV parsing failed for {store_name}: {e}")

    # Atomic write
    filename = os.path.join(OUTPUT_DIR, f"{store_name}.csv")
    temp_file = filename + ".tmp"

    with open(temp_file, "wb") as f:
        f.write(encoded)

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

    # Use Airflow date if available
    if args.execution_date:
        run_date = args.execution_date
    else:
        run_date = datetime.utcnow().date().isoformat()

    print(f"Stock snapshot date: {run_date}")

    # Clean previous run (idempotent)
    shutil.rmtree(OUTPUT_DIR, ignore_errors=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    stores = pd.read_csv(STORE_FILE)

    token = login()

    failed = []

    for _, row in stores.iterrows():
        store_id = row["store"]
        store_name = str(row["storeName"]).replace("/", "_")

        try:
            fetch_stock_report(token, store_id, store_name, run_date)
        except Exception as e:
            print(f"❌ {store_name} failed: {e}")
            failed.append(store_name)

    if failed:
        raise Exception(f"STOCK FAILED for stores: {failed}")

    print("✅ STOCK SNAPSHOT COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    main()