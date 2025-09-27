import asyncio
import os
import smtplib
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from playwright.async_api import async_playwright
import psycopg2
import uuid

# Email configuration
EMAIL_CONFIG = {
    'smtp_server': 'smtp.gmail.com',
    'smtp_port': 587,
    'sender_email': 'mail',
    'sender_password': 'app_pw',
    'to': 'mail_mail',
    'cc_recipients': ['list', 'of', 'mail'],
    'tracking_host': 'http://<ip_server>:<port>',
    'summary_recipient': 'email'
}

# PostgreSQL config
PG_CONFIG = {
    'dbname': 'db_name',
    'user': 'user_name',
    'password': 'pw',
    'host': 'server_ip',
    'port': port
}

def log_event(recipient, report_date, event):
    """Insert tracking event into PostgreSQL table"""
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO tracking (recipient, report_date, event) VALUES (%s, %s, %s)",
        (recipient, report_date, event)
    )
    conn.commit()
    cur.close()
    conn.close()

async def save_pdf():
    os.makedirs("reports", exist_ok=True)
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    file_path = os.path.join("reports", f"sales_report_{yesterday}.pdf")

    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        await page.goto("http://127.0.0.1:8050", wait_until="networkidle")
        await page.pdf(
            path=file_path,
            format="A3",
            landscape=True,
            margin={"top": "0mm", "bottom": "0mm", "left": "0mm", "right": "0mm"},
            scale=1.0,
            print_background=True
        )
        await browser.close()
        print(f"PDF saved as {file_path}")
    return file_path, yesterday

def send_email_with_attachment():
    """Send email with PDF attachment and tracking pixel/link"""
    try:
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        file_path = os.path.join("reports", f"sales_report_{yesterday}.pdf")
        
        if not os.path.exists(file_path):
            print(f"PDF file not found: {file_path}")
            return False

        # Create single email with TO and CC
        msg = MIMEMultipart('alternative')
        msg['From'] = EMAIL_CONFIG['sender_email']
        msg['To'] = EMAIL_CONFIG['to']  # data@newshop.in
        msg['Cc'] = ', '.join(EMAIL_CONFIG['cc_recipients'])  # All other recipients in CC
        msg['Subject'] = f"Daily Sales Report - {yesterday}"

        # Unique ID for this email for robust tracking
        unique_id = str(uuid.uuid4())

        # Tracking pixel URL for email opens
        tracking_pixel = f"{EMAIL_CONFIG['tracking_host']}/track_open/{unique_id}?recipient={EMAIL_CONFIG['to']}&report={yesterday}"

        # Optional PDF download link for click tracking
        download_link = f"{EMAIL_CONFIG['tracking_host']}/download/{unique_id}?recipient={EMAIL_CONFIG['to']}&report={yesterday}"

        # Email Body
        html_body = f"""
        <p>Dear Team,</p>
        <p>Please find attached the daily sales report for {yesterday}.</p>
        <img src="{tracking_pixel}" width="1" height="1" style="display:none;">
        <p>Best regards,<br>Automated Reporting System</p>
        """
        msg.attach(MIMEText(html_body, 'html'))

        # Attach PDF
        with open(file_path, "rb") as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename={os.path.basename(file_path)}')
        msg.attach(part)

        # Send to all recipients (TO + CC)
        all_recipients = [EMAIL_CONFIG['to']] + EMAIL_CONFIG['cc_recipients']
        
        # Server Config
        server = smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port'])
        server.starttls()
        server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
        server.sendmail(EMAIL_CONFIG['sender_email'], all_recipients, msg.as_string())
        server.quit()

        print(f"Email sent to {EMAIL_CONFIG['to']} with CC to {len(EMAIL_CONFIG['cc_recipients'])} recipients")

        # Log events for tracking
        log_event(EMAIL_CONFIG['to'], yesterday, "sent")
        for cc_recipient in EMAIL_CONFIG['cc_recipients']:
            log_event(cc_recipient, yesterday, "sent_cc")

        return True

    except Exception as e:
        print(f"Error sending email: {e}")
        return False

async def generate_and_send_report():
    await save_pdf()
    send_email_with_attachment()

if __name__ == "__main__":
    asyncio.run(generate_and_send_report())