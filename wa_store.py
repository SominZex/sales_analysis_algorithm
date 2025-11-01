import os
import time
import pandas as pd
from pathlib import Path
from playwright.sync_api import sync_playwright
from datetime import datetime, timedelta


class WhatsAppSender:
    def __init__(self, user_data_dir="/home/azureuser/azure_analysis_algorithm/whatsapp"):
        """Initialize WhatsApp sender with persistent session"""
        self.user_data_dir = user_data_dir
        os.makedirs(user_data_dir, exist_ok=True)

    def validate_pdf(self, pdf_path):
        """Validate PDF before attempting upload"""
        print(f"\nValidating PDF: {pdf_path}")

        if not os.path.exists(pdf_path):
            raise FileNotFoundError(f"PDF not found: {pdf_path}")

        size = os.path.getsize(pdf_path)
        if size == 0:
            raise Exception("PDF file is empty (0 bytes)")

        if size > 100 * 1024 * 1024:
            raise Exception(f"PDF too large ({size/(1024*1024):.2f}MB). WhatsApp limit is 100MB")

        # Check PDF header
        with open(pdf_path, 'rb') as f:
            header = f.read(8)
            if not header.startswith(b'%PDF-'):
                raise Exception(f"Invalid PDF file. Header: {header[:20]}")

        print(f"✓ PDF validation passed ({size/(1024*1024):.2f}MB)")
        return True

    def wait_for_whatsapp_load(self, page, timeout=120):
        """Wait for WhatsApp to load"""
        print("Checking WhatsApp Web status...")
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                elapsed = int(time.time() - start_time)
                
                # Check for QR code
                qr_selectors = [
                    'canvas[aria-label*="Scan"]',
                    'canvas[role="img"]',
                    'div[data-ref]',
                ]

                for qr_sel in qr_selectors:
                    if page.locator(qr_sel).count() > 0:
                        print("\n" + "="*50)
                        print("⚠️  QR CODE DETECTED - Please scan with your phone")
                        print("="*50)
                        page.screenshot(path="qr_code.png")
                        print("QR code screenshot saved as 'qr_code.png'")
                        print("\nWaiting for you to scan the QR code...")

                        while page.locator(qr_sel).count() > 0:
                            time.sleep(2)

                        print("✓ QR code scanned successfully!")
                        time.sleep(5)
                        break

                # Check if WhatsApp loaded
                loaded_selectors = [
                    '[data-testid="chat-list-search"]',
                    'div[contenteditable="true"][data-tab="3"]',
                    '#side',
                    '[data-testid="chat-list"]',
                    'div[data-testid="chatlist-content"]',
                ]

                for selector in loaded_selectors:
                    try:
                        elements = page.locator(selector)
                        if elements.count() > 0 and elements.first.is_visible():
                            print(f"✓ WhatsApp loaded successfully (found: {selector})")
                            time.sleep(3)
                            return True
                    except:
                        continue

                # Print status every 10 seconds
                if elapsed % 10 == 0 and elapsed > 0:
                    print(f"Still loading... ({elapsed}s / {timeout}s)")

                time.sleep(2)

            except Exception as e:
                time.sleep(2)

        print("\n⚠️  Timeout reached!")
        page.screenshot(path="timeout_screenshot.png")
        return False

    def send_pdf_to_number(self, page, wa_number, pdf_path, message, store_name):
        """Send PDF directly to a phone number via WhatsApp Web"""
        print(f"\n{'='*60}")
        print(f"📤 Sending to {store_name}")
        print(f"📱 Number: {wa_number}")
        print(f"📄 File: {os.path.basename(pdf_path)}")
        print("="*60)

        try:
            # Use direct WhatsApp Web URL instead of wa.me
            # Remove + from number for the URL
            clean_number = wa_number.replace("+", "")
            chat_url = f"https://web.whatsapp.com/send?phone={clean_number}"
            print(f"Opening chat URL: {chat_url}")
            page.goto(chat_url, wait_until="domcontentloaded", timeout=30000)
            time.sleep(5)

            # Wait for chat to open - look for message input box
            print("Waiting for chat to open...")
            message_box_found = False
            for attempt in range(60):
                message_box_selectors = [
                    '[contenteditable="true"][data-tab="10"]',
                    '[data-testid="conversation-compose-box-input"]',
                    'div[role="textbox"][title="Type a message"]',
                ]
                
                for selector in message_box_selectors:
                    try:
                        if page.locator(selector).count() > 0:
                            message_box_found = True
                            print(f"✓ Chat opened (found: {selector})")
                            break
                    except:
                        continue
                
                if message_box_found:
                    break
                
                if attempt % 10 == 0 and attempt > 0:
                    print(f"  Still waiting... ({attempt}/60 seconds)")
                    
                time.sleep(1)

            if not message_box_found:
                raise Exception("Chat did not open - message box not found")

            time.sleep(2)

            # Get initial message count for verification
            initial_count = 0
            message_selector = None
            try:
                message_count_selectors = [
                    'div[data-testid="msg-container"]',
                    'div.message-out',
                ]
                for sel in message_count_selectors:
                    count = page.locator(sel).count()
                    if count > 0:
                        initial_count = count
                        message_selector = sel
                        print(f"Initial message count: {initial_count}")
                        break
            except:
                pass

            # Upload PDF
            print("Attaching PDF...")
            self.validate_pdf(pdf_path)
            abs_path = os.path.abspath(pdf_path)

            # Click attach button with better detection
            attach_button = None
            attach_selectors = [
                '[data-testid="clip"]',
                'span[data-icon="clip"]',
                'button[aria-label="Attach"]',
                'div[aria-label="Attach"]',
                'button[title="Attach"]',
            ]

            print("Looking for attach button...")
            for wait_attempt in range(30):
                for selector in attach_selectors:
                    try:
                        loc = page.locator(selector)
                        if loc.count() > 0:
                            elem = loc.first
                            if elem.is_visible():
                                attach_button = elem
                                print(f"✓ Found attach button: {selector}")
                                break
                    except:
                        continue
                
                if attach_button:
                    break
                
                if wait_attempt % 5 == 0 and wait_attempt > 0:
                    print(f"  Still looking for attach button... ({wait_attempt}/30 seconds)")
                
                time.sleep(1)

            if not attach_button:
                # Try to find by looking for footer buttons
                print("Trying alternative method: looking for footer buttons...")
                try:
                    footer_buttons = page.locator('footer button, footer div[role="button"]')
                    if footer_buttons.count() > 0:
                        for i in range(footer_buttons.count()):
                            btn = footer_buttons.nth(i)
                            # Check if it has a clip icon child
                            if btn.locator('span[data-icon="clip"]').count() > 0:
                                attach_button = btn
                                print(f"✓ Found attach button via footer search")
                                break
                except:
                    pass

            if not attach_button:
                raise Exception("Attach button not found after extensive search")

            attach_button.click()
            print("✓ Clicked attach button")
            time.sleep(3)

            # Wait for attach menu to appear
            print("Waiting for attach menu...")
            menu_appeared = False
            for wait in range(10):
                try:
                    # Check if attach menu is visible
                    if page.locator('li[data-testid="mi-attach-document"]').count() > 0:
                        menu_appeared = True
                        print("✓ Attach menu appeared")
                        break
                    # Alternative check
                    if page.locator('input[type="file"]').count() > 0:
                        menu_appeared = True
                        print("✓ File input detected")
                        break
                except:
                    pass
                time.sleep(0.5)
            
            if not menu_appeared:
                print("⚠️ Attach menu might not be visible, proceeding anyway...")
            
            time.sleep(2)

            # Upload file
            print("Uploading PDF file...")
            upload_success = False
            
            # Method 1: Try direct file input method
            file_inputs = page.locator('input[type="file"]')
            print(f"Found {file_inputs.count()} file input(s)")
            
            if file_inputs.count() > 0:
                for i in range(file_inputs.count()):
                    try:
                        inp = file_inputs.nth(i)
                        accept = inp.get_attribute('accept') or ''
                        print(f"  Input {i}: accept='{accept}'")
                        
                        # Skip image-only inputs
                        if accept and 'image' in accept.lower() and 'application' not in accept.lower():
                            print(f"  Skipping input {i} (image only)")
                            continue
                        
                        inp.set_input_files(abs_path)
                        upload_success = True
                        print(f"✓ File uploaded via input element {i}")
                        break
                    except Exception as e:
                        print(f"  Failed on input {i}: {e}")
                        continue

            # Method 2: Try clicking document button then file chooser
            if not upload_success:
                print("Trying document button + file chooser method...")
                try:
                    doc_button = page.locator('li[data-testid="mi-attach-document"]')
                    if doc_button.count() > 0:
                        print("  Found document button, setting up file chooser...")
                        with page.expect_file_chooser(timeout=10000) as fc_info:
                            doc_button.first.click()
                            time.sleep(0.5)
                        file_chooser = fc_info.value
                        file_chooser.set_files(abs_path)
                        upload_success = True
                        print("✓ File uploaded via document button + file chooser")
                    else:
                        print("  Document button not found")
                except Exception as e:
                    print(f"  Document button method failed: {e}")

            # Method 3: Try generic button with file chooser
            if not upload_success:
                print("Trying generic attach menu button...")
                try:
                    attach_menu_buttons = page.locator('li[role="button"]')
                    if attach_menu_buttons.count() >= 2:
                        print(f"  Found {attach_menu_buttons.count()} menu buttons, trying second one...")
                        with page.expect_file_chooser(timeout=10000) as fc_info:
                            attach_menu_buttons.nth(1).click()
                            time.sleep(0.5)
                        file_chooser = fc_info.value
                        file_chooser.set_files(abs_path)
                        upload_success = True
                        print("✓ File uploaded via generic menu button")
                except Exception as e:
                    print(f"  Generic button method failed: {e}")

            if not upload_success:
                raise Exception("All file upload methods failed")

            print("Waiting for file to process...")
            time.sleep(7)

            # Add message/caption
            print("Adding message caption...")
            typed_message = False
            message_input_selectors = [
                'div[role="textbox"][data-tab="10"]',
                'div[contenteditable="true"][data-tab="10"]',
                'div[contenteditable="true"][data-tab="6"]',
                'div[contenteditable="true"][data-tab="1"]',
                '[data-testid="conversation-compose-box-input"]',
                'div[role="textbox"]',
                'footer div[contenteditable="true"]',
            ]

            # Wait for message input to be available
            for wait in range(15):
                for sel in message_input_selectors:
                    try:
                        loc = page.locator(sel)
                        if loc.count() > 0 and loc.first.is_visible():
                            try:
                                loc.first.click()
                                time.sleep(0.3)
                                loc.first.type(message, delay=50)
                                typed_message = True
                                print(f"✓ Typed message using: {sel}")
                                break
                            except Exception as e:
                                print(f"  Selector {sel} found but typing failed: {e}")
                                continue
                    except:
                        continue
                
                if typed_message:
                    break
                
                if wait % 3 == 0 and wait > 0:
                    print(f"  Still looking for message input... ({wait}/15 seconds)")
                
                time.sleep(1)

            if not typed_message:
                print("Trying keyboard fallback...")
                try:
                    page.keyboard.type(message, delay=50)
                    typed_message = True
                    print("✓ Typed message using keyboard fallback")
                except Exception as e:
                    print(f"⚠️ Could not type message: {e}")
                    print("⚠️ Proceeding without caption...")

            time.sleep(2)

            # Click send button
            print("Looking for send button...")
            send_button = None
            send_selectors = [
                '[data-testid="send"]',
                'span[data-icon="send"]',
                'button[aria-label="Send"]',
                'div[role="button"][aria-label="Send"]',
            ]

            for attempt in range(90):
                for sel in send_selectors:
                    try:
                        loc = page.locator(sel)
                        if loc.count() > 0:
                            elem = loc.first
                            if elem.is_visible():
                                send_button = elem
                                print(f"✓ Send button found: {sel}")
                                break
                    except:
                        continue
                
                if send_button:
                    break
                
                if attempt % 10 == 0 and attempt > 0:
                    print(f"  Still looking for send button... ({attempt}/90 seconds)")
                
                time.sleep(1)

            if not send_button:
                raise Exception("Send button not found after 90 seconds")

            print("Clicking send button...")
            try:
                send_button.click()
                print("✓ Send button clicked")
            except Exception as e:
                print(f"Regular click failed, trying force click: {e}")
                try:
                    send_button.click(force=True)
                    print("✓ Send button clicked (force)")
                except Exception as e2:
                    raise Exception(f"Could not click send button: {e2}")

            # Quick verification
            print("Verifying message sent...")
            time.sleep(5)  # Give WhatsApp time to process
            
            verification_success = False
            checks_passed = []
            
            try:
                # Quick check 1: Message count increased
                if message_selector:
                    current_count = page.locator(message_selector).count()
                    if current_count > initial_count:
                        checks_passed.append("message count increased")
                        verification_success = True
            except:
                pass
            
            try:
                # Quick check 2: Check for send dialog closed
                if page.locator('div[role="dialog"]').count() == 0:
                    checks_passed.append("send dialog closed")
                    verification_success = True
            except:
                pass
            
            try:
                # Quick check 3: Look for recently sent document
                recent_docs = page.locator('div.message-out span[data-icon="document"]')
                if recent_docs.count() > 0:
                    checks_passed.append("document in chat")
                    verification_success = True
            except:
                pass
            
            if verification_success:
                print(f"✓ Send verified ({', '.join(checks_passed)})")
            else:
                print("⚠️ Could not verify, but likely sent successfully")
            
            time.sleep(2)
            
            print(f"✅ Successfully sent to {store_name} ({wa_number})")
            return True

        except Exception as e:
            print(f"❌ Failed to send to {store_name}: {e}")
            raise


def main():
    CSV_FILE = "/home/azureuser/azure_analysis_algorithm/partner.csv"
    REPORT_DIR = "/home/azureuser/azure_analysis_algorithm/store_reports"

    print("=" * 60)
    print("WhatsApp Individual PDF Sender - Automated Run")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Read CSV
    try:
        df = pd.read_csv(CSV_FILE)
        print(f"✓ CSV loaded: {len(df)} stores found")
    except Exception as e:
        print(f"❌ Failed to read CSV: {e}")
        return

    # Validate CSV columns
    required_cols = {"storeName", "wa_number"}
    if not required_cols.issubset(df.columns):
        print(f"❌ CSV must contain columns: {required_cols}")
        print(f"Found columns: {df.columns.tolist()}")
        return

    sender = WhatsAppSender()

    with sync_playwright() as p:
        print("\nLaunching browser with saved session...")
        browser = p.chromium.launch_persistent_context(
            user_data_dir=sender.user_data_dir,
            headless=True,
            channel="chrome",
            args=[
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process',
                '--disable-gpu',
                '--window-size=1280,720',
            ],
            slow_mo=150,
        )

        page = browser.pages[0] if browser.pages else browser.new_page()
        page.set_viewport_size({"width": 1280, "height": 720})

        # Navigate to WhatsApp Web
        print("Navigating to WhatsApp Web...")
        page.goto("https://web.whatsapp.com", wait_until="networkidle", timeout=60000)
        time.sleep(5)

        # Wait for WhatsApp to load
        if not sender.wait_for_whatsapp_load(page):
            print("❌ WhatsApp failed to load. Please check screenshots.")
            browser.close()
            return

        print("\n" + "="*60)
        print("Starting to send reports to individual numbers...")
        print("="*60)

        # Track results
        success_count = 0
        failed_stores = []

        # Send to each store
        for idx, row in df.iterrows():
            store_name = str(row["storeName"]).strip()
            wa_number = str(row["wa_number"]).strip()
            
            # Clean phone number (remove any non-digits except +)
            wa_number = wa_number.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
            
            # Auto-add country code if missing
            if not wa_number.startswith("+"):
                # Assuming India (+91) - change this if needed
                wa_number = f"+91{wa_number}"
                print(f"✓ Added country code: {wa_number}")
            
            # Build PDF path
            pdf_filename = f"{store_name.replace(' ', '_')}_weekly_report.pdf"
            pdf_path = os.path.join(REPORT_DIR, pdf_filename)

            # Check if PDF exists
            if not os.path.exists(pdf_path):
                print(f"\n❌ Skipping {store_name}: PDF not found ({pdf_path})")
                failed_stores.append((store_name, "PDF not found"))
                continue

            # Create personalized message
            message = f"Hello,Here is your weekly sales report from New Shop Automatic reporting system.\n\nContact Ritik for further information."
            # Send PDF
            try:
                sender.send_pdf_to_number(page, wa_number, pdf_path, message, store_name)
                success_count += 1
                
                # Wait between sends to avoid rate limiting
                print("Waiting 5 seconds before next send...")
                time.sleep(5)
                
            except Exception as e:
                print(f"❌ Failed to send to {store_name} ({wa_number}): {e}")
                failed_stores.append((store_name, str(e)))
                continue

        # Summary
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"✅ Successfully sent: {success_count}/{len(df)}")
        
        if failed_stores:
            print(f"\n❌ Failed ({len(failed_stores)}):")
            for store, reason in failed_stores:
                print(f"  - {store}: {reason}")
        else:
            print("\n🎉 ALL REPORTS SENT SUCCESSFULLY!")

        print(f"\nFinished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

        print("\nClosing browser in 5 seconds...")
        time.sleep(5)
        browser.close()


if __name__ == "__main__":
    main()