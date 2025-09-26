import requests
import os
import base64
import binascii
import re
from datetime import datetime, timedelta


class CSVDownloader:
    def __init__(self, base_url="https://api.thenewshop.in", username="user_name", password="pw"):
        self.base_url = base_url
        self.username = username
        self.password = password
        self.token = None
        self.session = requests.Session()

    def authenticate(self):
        """Get authentication token from login API"""
        print("Authenticating...")

        login_url = f"{self.base_url}/login"
        payload = {"username": self.username, "password": self.password}

        response = self.session.post(login_url, json=payload)

        if response.status_code in [200, 201]:
            data = response.json()
            self.token = data.get("token")
            if self.token:
                print("Authentication successful!")
                return True
            else:
                print("Error: No token received")
                return False
        else:
            print(f"Authentication failed: {response.status_code}")
            print(f"Response: {response.text}")
            return False

    def download_yesterday_csv(self, order_type="online", output_dir="./downloads"):
        """Download CSV report for yesterday's data only"""
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"Downloading yesterday's data: {yesterday}")
        return self.download_csv(order_type, yesterday, yesterday, output_dir)

    def download_csv(self, order_type="online", from_date=None, to_date=None, output_dir="./downloads"):
        """Download CSV report"""
        if not self.token:
            if not self.authenticate():
                return False

        os.makedirs(output_dir, exist_ok=True)

        csv_url = f"{self.base_url}/orders/orderReportCSV"
        params = {"orderType": order_type, "fromDate": from_date, "toDate": to_date}
        headers = {"accept": "*/*", "Authorization": self.token}

        print(f"Downloading CSV for {order_type} orders from {from_date} to {to_date}...")

        response = self.session.get(csv_url, params=params, headers=headers)

        if response.status_code != 200:
            print(f"Download failed: {response.status_code}")
            print(f"Response: {response.text}")
            return None

        # Check if response contains JSON error
        content_type = response.headers.get("content-type", "").lower()
        if "application/json" in content_type:
            try:
                error_data = response.json()
                print(f"API returned error: {error_data}")
                return None
            except Exception:
                pass

        # Save file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"orders_{order_type}_{from_date}.csv"
        filepath = os.path.join(output_dir, filename)

        content = response.content
        try:
            text_content = content.decode("utf-8")
        except UnicodeDecodeError:
            # Binary content
            with open(filepath, "wb") as f:
                f.write(content)
            print(f"Saved binary CSV: {filepath}")
            return filepath

        # Check if Base64
        clean_content = re.sub(r"\s", "", text_content)
        try:
            decoded_bytes = base64.b64decode(clean_content, validate=True)
            try:
                decoded_text = decoded_bytes.decode("utf-8")
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(decoded_text)
                print("Base64 decoded and saved as text.")
            except UnicodeDecodeError:
                with open(filepath, "wb") as f:
                    f.write(decoded_bytes)
                print("Base64 decoded and saved as binary.")
        except (binascii.Error, ValueError):
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(text_content)
            print("Saved as plain text CSV.")

        print(f"CSV saved: {filepath}")
        return filepath


def main():
    downloader = CSVDownloader(username="nssomin", password="nssomin")
    result = downloader.download_yesterday_csv(order_type="online", output_dir="./downloads")

    if result:
        print("Yesterday's report downloaded successfully!")
    else:
        print("Download failed!")


if __name__ == "__main__":
    main()
