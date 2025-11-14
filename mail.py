import os
import smtplib
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from datetime import datetime

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SENDER_EMAIL = "satpal@newshop.in"
SENDER_PASSWORD = "outd pxir nvgc mwrp"
CC_EMAILS = ["kamfranchise@newshop.in", "data@newshop.in"]
BCC_EMAILS = ["mani@newshop.in"]

REPORTS_DIR = "/home/azureuser/azure_analysis_algorithm/store_reports"
PARTNER_FILE = "/home/azureuser/azure_analysis_algorithm/partner.csv"

def create_email_body(store_name):
    """Return the HTML email body for a specific store."""
    today = datetime.now().strftime("%d %B %Y")
    return f"""
    <html>
    <body style="font-family: 'Segoe UI', Arial, sans-serif; color: #333; background-color: #f9f9f9; padding: 20px;">
        <div style="max-width: 600px; background: #ffffff; padding: 25px; border-radius: 10px; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">
            <h2 style="color: #0078D7; text-align: center;">Weekly Performance Report</h2>
            <hr style="border: 1px solid #0078D7; width: 80%; margin: 15px auto;">

            <p>Dear Business Partner,</p>

            <p>We're pleased to share the <strong>weekly performance report</strong> for your store:</p>

            <h3 style="text-align: center; color: #0078D7;">{store_name}</h3>

            <p>The report covers the store's sales performance, top-performing categories, brands, and products for the week ending <strong>{today}</strong>.</p>

            <p>Please review the attached PDF for detailed insights, including visual trends and performance summaries.</p>

            <p>If you have any questions or would like to discuss the results further, feel free to reach out to our analytics team.</p>

            <br>
            <p>Warm regards,</p>
            <p><strong>Analytics & Insights Team</strong><br>
            <em>New Shop.</em><br>
            📧 data@newshop.in</p>

            <hr style="margin-top: 25px;">
            <p style="font-size: 12px; color: #777; text-align: center;">
                This is an automated email. Please do not reply directly to this message.
            </p>
        </div>
    </body>
    </html>
    """

def send_email_with_attachment(to_email, subject, body, attachment_path):
    """Send an email with a PDF attachment"""
    try:
        msg = MIMEMultipart()
        msg["From"] = SENDER_EMAIL
        msg["To"] = to_email
        msg["Cc"] = ", ".join(CC_EMAILS)
        msg["Bcc"] = ", ".join(BCC_EMAILS)
        msg["Subject"] = subject

        msg.attach(MIMEText(body, "html"))

        with open(attachment_path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(attachment_path)}")
            msg.attach(part)

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            # Send to both TO and CC recipients
            server.send_message(msg)

        print(f"Email sent successfully to {to_email} (CC: {', '.join(CC_EMAILS)})")
    except Exception as e:
        print(f"Failed to send email to {to_email}: {e}")

def send_all_reports():
    partners_df = pd.read_csv(PARTNER_FILE)

    for _, row in partners_df.iterrows():
        store_name = row["storeName"]
        email = row["email"]

        pdf_name = f"{store_name.replace(' ', '_')}_weekly_report.pdf"
        pdf_path = os.path.join(REPORTS_DIR, pdf_name)

        if os.path.exists(pdf_path):
            subject = f"Weekly Store Report - {store_name}"

            body = create_email_body(store_name)
            send_email_with_attachment(email, subject, body, pdf_path)
        else:
            print(f"Report not found for store: {store_name}")


if __name__ == "__main__":
    send_all_reports()