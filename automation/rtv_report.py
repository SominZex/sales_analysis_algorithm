import requests
import base64
import pandas as pd
import os
from datetime import date

LOGIN_URL = "https://api.example.in/login"
RTV_URL   = "https://api.example.in/rtvstore-stocks/RTV"

USERNAME = "username"
PASSWORD = "pwd"

STORE_FILE = "stores.csv"
OUTPUT_DIR = "store_rtv"


def login():
    r = requests.post(LOGIN_URL, json={
        "username": USERNAME,
        "password": PASSWORD
    })
    return r.json()["token"]


def fetch_rtv_report(token, store_id, store_name):
    today = date.today().isoformat()   # e.g. "2025-03-18"

    headers = {
        "Authorization": token,
        "accept": "*/*"
    }

    params = {
        "storeId":  str(store_id),
        "fromDate": today,
        "toDate":   today,
    }

    r = requests.get(RTV_URL, headers=headers, params=params)

    print(f"{store_name} -> Status {r.status_code}")

    if r.status_code == 200:
        try:
            decoded = base64.b64decode(r.text)
        except Exception:
            decoded = r.content

        filename = os.path.join(OUTPUT_DIR, f"{store_name}.csv")
        with open(filename, "wb") as f:
            f.write(decoded)

        print(f"Saved {filename}")

    else:
        print(f"Failed for {store_name}: {r.text}")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    stores = pd.read_csv(STORE_FILE)
    token  = login()

    for _, row in stores.iterrows():
        store_id   = row["store"]
        store_name = str(row["storeName"]).replace("/", "_")
        fetch_rtv_report(token, store_id, store_name)

if __name__ == "__main__":
    main()