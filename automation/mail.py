import os
import smtplib
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
import sys
sys.path.insert(0, "/base/dir")
from monitoring.metrics import task_timer, mail_timer, record_mail_sent

SMTP_SERVER = "smtp.zoho.in"
SMTP_PORT = 465
SENDER_EMAIL = "sender@mail.com"
SENDER_PASSWORD = "app_pwd"
CC_EMAILS = ['firstmail', 'secondmail']
BCC_EMAILS = ["bccmail"]

PARTNER_FILE = "/base/dir/partner.csv"

def create_email_body(store_name, pdf_link):
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

            <p>Please click the button below to view your report:</p>

            <div style="text-align: center; margin: 25px 0;">
                <a href="{pdf_link}" target="_blank"
                   style="background-color: #0078D7; color: #ffffff; padding: 12px 28px;
                          text-decoration: none; border-radius: 6px; font-size: 15px;
                          font-weight: bold; display: inline-block;">
                    View Weekly Report
                </a>
            </div>

            <p style="font-size: 13px; color: #777; text-align: center;">
                Link expires in 3 days. If the button doesn't work, copy and paste this URL into your browser:<br>
                <a href="{pdf_link}" style="color: #0078D7; word-break: break-all;">{pdf_link}</a>
            </p>

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

def send_email(to_email, subject, body):
    """Send an HTML email with no attachment."""
    try:
        msg = MIMEMultipart()
        msg["From"] = SENDER_EMAIL
        msg["To"] = to_email
        msg["Cc"] = ", ".join(CC_EMAILS)
        msg["Bcc"] = ", ".join(BCC_EMAILS)
        msg["Subject"] = subject

        msg.attach(MIMEText(body, "html"))

        all_recipients = [to_email] + CC_EMAILS + BCC_EMAILS

        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.sendmail(SENDER_EMAIL, all_recipients, msg.as_string())

        print(f"Email sent successfully to {to_email} (CC: {', '.join(CC_EMAILS)}, BCC: {', '.join(BCC_EMAILS)})")

    except Exception as e:
        print(f"Failed to send email to {to_email}: {e}")

def send_all_reports():
    partners_df = pd.read_csv(PARTNER_FILE)

    for _, row in partners_df.iterrows():
        store_name = row["storeName"]
        email = row["email"]
        pdf_link = row.get("pdf_link", "")

        if not pdf_link or pd.isna(pdf_link):
            print(f"No PDF link found for store: {store_name}")
            continue

        subject = f"Weekly Store Report - {store_name}"
        body = create_email_body(store_name, pdf_link)
        try:
            send_email(email, subject, body)
            record_mail_sent("weekly", True)                  
        except Exception as e:
            record_mail_sent("weekly", False)         
            print(f"❌ Failed to record mail for {store_name}: {e}")

if __name__ == "__main__":
    with task_timer("weekly_mail"):
        with mail_timer("weekly"):
            send_all_reports()