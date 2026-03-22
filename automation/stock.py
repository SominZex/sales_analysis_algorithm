import requests
import base64
import pandas as pd
import os

LOGIN_URL = "https://api.example.in/login"
STOCK_URL = "https://api.example.in/store-stocks/report/storeStocksCSV"

USERNAME = "example"
PASSWORD = "example"

STORE_FILE = "stores.csv"
OUTPUT_DIR = "store_stocks"


def login():
    r = requests.post(LOGIN_URL, json={
        "username": USERNAME,
        "password": PASSWORD
    })
    return r.json()["token"]


def fetch_stock_report(token, store_id, store_name):

    headers = {
        "Authorization": token,
        "accept": "*/*"
    }

    params = {
        "store": int(store_id)
    }

    r = requests.get(STOCK_URL, headers=headers, params=params)

    print(f"{store_name} -> Status {r.status_code}")

    if r.status_code == 200:

        decoded = base64.b64decode(r.text)

        filename = os.path.join(OUTPUT_DIR, f"{store_name}.csv")

        with open(filename, "wb") as f:
            f.write(decoded)

        print(f"Saved {filename}")

    else:
        print(f"Failed for {store_name}: {r.text}")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    stores = pd.read_csv(STORE_FILE)
    token = login()

    for _, row in stores.iterrows():
        store_id = row["store"]
        store_name = str(row["storeName"]).replace("/", "_")
        fetch_stock_report(token, store_id, store_name)

if __name__ == "__main__":
    main()