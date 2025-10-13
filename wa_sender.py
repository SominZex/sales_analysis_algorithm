import os
import time
from pathlib import Path
from playwright.sync_api import sync_playwright
from datetime import datetime, timedelta


class WhatsAppSender:
    def __init__(self, user_data_dir="/home/azureuser/azure_analysis_algorithm/whatsapp"):
        """
        Initialize WhatsApp sender with persistent session

        Args:
            user_data_dir: Directory to store WhatsApp session data
        """
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

            # Check for encryption
            f.seek(0)
            content = f.read(8192)
            if b'/Encrypt' in content:
                raise Exception("PDF is encrypted/password-protected. WhatsApp won't accept it.")

        print(f"✓ PDF validation passed ({size/(1024*1024):.2f}MB)")
        return True

    def wait_for_whatsapp_load(self, page, timeout=120):
        """Wait for WhatsApp to load and handle QR code if needed"""
        print("Checking WhatsApp Web status...")
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                # Take a screenshot for debugging
                elapsed = int(time.time() - start_time)
                if elapsed % 10 == 0 and elapsed > 0:
                    page.screenshot(path=f"debug_loading_{elapsed}s.png")
                    print(f"Debug screenshot saved: debug_loading_{elapsed}s.png")

                # Check if QR code is present
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

                        # Wait for QR to disappear
                        while page.locator(qr_sel).count() > 0:
                            time.sleep(2)

                        print("✓ QR code scanned successfully!")
                        time.sleep(5)
                        break

                # EXPANDED selectors for loaded WhatsApp
                loaded_selectors = [
                    # Search box variations
                    '[data-testid="chat-list-search"]',
                    'div[contenteditable="true"][data-tab="3"]',
                    '[aria-label="Search input textbox"]',
                    'div[role="textbox"][title="Search input textbox"]',

                    # Side panel
                    '#side',
                    'div[data-testid="chatlist-content"]',

                    # Pane side
                    'div#pane-side',

                    # Chat list
                    '[data-testid="chat-list"]',
                    'div[aria-label="Chat list"]',

                    # Any chat element
                    '[data-testid="cell-frame-container"]',

                    # Main app wrapper
                    'div[data-testid="app-wrapper-main"]',

                    # Header with new chat button
                    'div[data-testid="chatlist-header"]',
                ]

                for selector in loaded_selectors:
                    try:
                        elements = page.locator(selector)
                        count = elements.count()
                        if count > 0:
                            # Verify it's actually visible
                            if elements.first.is_visible():
                                print(f"✓ WhatsApp loaded successfully (found: {selector})")
                                time.sleep(2)  # Extra stability wait
                                return True
                    except Exception as e:
                        continue

                # Alternative check: Look for specific text
                try:
                    if page.get_by_text("Chats").count() > 0 or \
                       page.get_by_text("Status").count() > 0 or \
                       page.get_by_text("Communities").count() > 0:
                        print("✓ WhatsApp loaded (found navigation text)")
                        time.sleep(2)
                        return True
                except:
                    pass

                # Print status every 5 seconds
                if elapsed % 5 == 0:
                    print(f"Still loading... ({elapsed}s / {timeout}s)")

                time.sleep(2)

            except Exception as e:
                elapsed = int(time.time() - start_time)
                print(f"Checking... ({elapsed}s) - Error: {str(e)[:50]}")
                time.sleep(2)

        # Timeout reached
        print("\n⚠️  Timeout reached!")
        page.screenshot(path="timeout_screenshot.png")

        # Save page HTML for debugging
        try:
            with open("timeout_page.html", "w", encoding="utf-8") as f:
                f.write(page.content())
            print("Page HTML saved to: timeout_page.html")
        except:
            pass

        return False

    def find_and_open_chat(self, page, group_name):
        """Find and open a chat/group"""
        print(f"\nSearching for: {group_name}")

        search_selectors = [
            '[data-testid="chat-list-search"]',
            'div[contenteditable="true"][data-tab="3"]',
            '[aria-label="Search input textbox"]',
            'div[role="textbox"][title="Search input textbox"]',
        ]

        # Wait for search box to be available with retry logic
        search_box = None
        max_attempts = 30  # 30 seconds total
        for attempt in range(max_attempts):
            for selector in search_selectors:
                try:
                    elements = page.locator(selector)
                    if elements.count() > 0:
                        elem = elements.first
                        # Check if it's actually visible and interactable
                        if elem.is_visible():
                            search_box = elem
                            print(f"✓ Found search box: {selector}")
                            break
                except Exception as e:
                    continue

            if search_box:
                break

            if attempt % 5 == 0 and attempt > 0:
                print(f"Still waiting for search box... ({attempt}/{max_attempts})")

            time.sleep(1)

        if not search_box:
            page.screenshot(path="no_search_box.png")
            raise Exception("Could not find search box. Screenshot saved as 'no_search_box.png'")

        # Try to click and ensure it's focused
        for click_attempt in range(3):
            try:
                search_box.click()
                time.sleep(0.5)
                break
            except Exception as e:
                if click_attempt == 2:
                    raise Exception(f"Could not click search box after 3 attempts: {e}")
                time.sleep(0.5)

        time.sleep(1)
        page.keyboard.type(group_name, delay=100)
        print(f"✓ Typed '{group_name}' in search")
        time.sleep(3)

        page.screenshot(path="search_results.png")
        print("Screenshot of search results saved as 'search_results.png'")

        chat_selectors = [
            f'span[title="{group_name}"]',
            '[data-testid="cell-frame-container"]',
            'div[role="listitem"]',
        ]

        clicked = False
        for selector in chat_selectors:
            try:
                elements = page.locator(selector)
                if elements.count() > 0:
                    elements.first.click()
                    print(f"✓ Clicked on chat using: {selector}")
                    clicked = True
                    break
            except Exception as e:
                continue

        if not clicked:
            print("Using fallback: pressing Enter")
            page.keyboard.press('Enter')

        time.sleep(3)

        message_box_selectors = [
            '[data-testid="conversation-compose-box-input"]',
            'div[contenteditable="true"][data-tab="10"]',
            '[aria-label="Type a message"]',
        ]

        for selector in message_box_selectors:
            if page.locator(selector).count() > 0:
                print("✓ Chat opened successfully")
                return True

        page.screenshot(path="chat_not_opened.png")
        raise Exception("Chat did not open. Screenshot saved as 'chat_not_opened.png'")

    def check_for_error_toast(self, page):
        """Check if WhatsApp shows an error message"""
        error_selectors = [
            'div[data-testid="toast-container"]',
            'div[role="alert"]',
            'span:has-text("Unsupported file")',
            'span:has-text("unsupported")',
            'span:has-text("failed")',
        ]

        for selector in error_selectors:
            if page.locator(selector).count() > 0:
                error_text = page.locator(selector).first.inner_text()
                return True, error_text

        return False, None

    def send_pdf_to_group(self, group_name, pdf_path, message="Sales report for today."):
        """
        Send PDF file to WhatsApp group with improved file handling and optional message/caption.

        Args:
            group_name (str): The group/chat name to send to.
            pdf_path (str): Absolute or relative path to the PDF file.
            message (str): Caption/message to send along with the PDF (default provided).
        """
        print("\n" + "="*60)
        print("Starting WhatsApp PDF Sender")
        print("="*60)

        # Validate PDF first
        self.validate_pdf(pdf_path)

        with sync_playwright() as p:
            print("Launching browser with saved session...")

            browser = p.chromium.launch_persistent_context(
                user_data_dir=self.user_data_dir,
                headless=True,
                channel="chrome",
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-web-security',
                    '--disable-features=IsolateOrigins,site-per-process',
                ],
                slow_mo=100,
            )

            page = browser.pages[0] if browser.pages else browser.new_page()
            page.set_viewport_size({"width": 1280, "height": 720})
            page.set_extra_http_headers({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            })

            try:
                print("Navigating to WhatsApp Web...")
                page.goto('https://web.whatsapp.com', wait_until="networkidle", timeout=60000)
                time.sleep(5)

                if not self.wait_for_whatsapp_load(page):
                    raise Exception("WhatsApp failed to load. Check debug screenshots and timeout_page.html")

                # Extra wait to ensure all UI elements are fully loaded and interactive
                print("Waiting for UI elements to stabilize...")
                time.sleep(5)

                self.find_and_open_chat(page, group_name)
                time.sleep(2)

                # Click attach button
                print("Opening attach menu...")
                page.screenshot(path="before_attach.png")

                attach_selectors = [
                    '[data-testid="clip"]',
                    'button[title="Attach"]',
                    'span[data-icon="clip"]',
                    'button[aria-label="Attach"]',
                ]

                attach_clicked = False
                for sel in attach_selectors:
                    try:
                        loc = page.locator(sel)
                        if loc.count() > 0:
                            loc.first.click()
                            attach_clicked = True
                            print(f"✓ Clicked attach: {sel}")
                            break
                    except Exception:
                        continue

                if not attach_clicked:
                    raise Exception("Could not open attach menu")

                time.sleep(2)
                page.screenshot(path="attach_menu_opened.png")

                # IMPROVED FILE UPLOAD LOGIC
                print("Starting file upload process...")
                abs_path = os.path.abspath(pdf_path)
                print(f"Will upload: {abs_path}")

                file_inputs = page.locator('input[type="file"]')
                print(f"Found {file_inputs.count()} file input(s)")
                upload_success = False

                if file_inputs.count() > 0:
                    for i in range(file_inputs.count()):
                        try:
                            inp = file_inputs.nth(i)
                            accept = inp.get_attribute('accept') or ''
                            print(f"Input {i}: accept='{accept}'")
                            if 'image' not in accept.lower():
                                inp.set_input_files(abs_path)
                                upload_success = True
                                print("✓ File uploaded via input element")
                                break
                        except Exception as e:
                            print(f"Failed on input {i}: {e}")
                            continue

                if not upload_success:
                    # Method 2: file chooser
                    print("Trying file chooser method...")
                    try:
                        with page.expect_file_chooser(timeout=10000) as fc_info:
                            # document button candidate
                            doc_button = page.locator('li[data-testid="mi-attach-document"]')
                            if doc_button.count() > 0:
                                doc_button.first.click()
                            else:
                                # fallback - often the second menu item is documents
                                page.locator('li[role="button"]').nth(1).click()
                        file_chooser = fc_info.value
                        file_chooser.set_files(abs_path)
                        upload_success = True
                        print("✓ File uploaded via file chooser")
                    except Exception as e:
                        print(f"File chooser method failed: {e}")

                if not upload_success:
                    raise Exception("All file upload methods failed")

                # Wait for WhatsApp to process the file and show preview dialog
                print("Waiting for WhatsApp to process the file (this may take a moment)...")
                time.sleep(5)
                page.screenshot(path="after_file_upload.png")

                # Wait longer for file preview dialog UI to appear
                print("Waiting for file preview dialog...")
                time.sleep(5)

                # Detect dialog open
                dialog_selectors = [
                    'div[role="dialog"]',
                    'div[data-testid="media-viewer"]',
                    'footer[data-testid="document-viewer-footer"]',
                    'div.document-viewer',
                ]
                dialog_open = False
                for sel in dialog_selectors:
                    try:
                        if page.locator(sel).count() > 0:
                            dialog_open = True
                            print(f"✓ Dialog confirmed open: {sel}")
                            break
                    except Exception:
                        continue

                if not dialog_open:
                    print("⚠️  WARNING: Dialog might not be open. Proceeding to try typing message anyway.")
                    page.screenshot(path="dialog_closed.png")

                # ------------------------------
                # Insert the message/caption here
                # ------------------------------
                typed_message = False
                message_selectors = [
                    # common caption / textbox selectors seen in the preview/dialog
                    'div[role="textbox"][data-tab="10"]',          # preview caption
                    'div[contenteditable="true"][data-tab="10"]',
                    'div[contenteditable="true"][data-tab="6"]',
                    'div[role="textbox"][title="Type a message"]',
                    '[data-testid="conversation-compose-box-input"]',
                    'div[contenteditable="true"][data-tab="1"]',
                    'div[role="textbox"]',
                ]

                for sel in message_selectors:
                    try:
                        loc = page.locator(sel)
                        if loc.count() > 0 and loc.first.is_visible():
                            try:
                                loc.first.click()
                                time.sleep(0.2)
                                loc.first.type(message)
                                typed_message = True
                                print(f"✓ Typed message using selector: {sel}")
                                break
                            except Exception as e:
                                print(f"Selector {sel} found but typing failed: {e}")
                                continue
                    except Exception:
                        continue

                if not typed_message:
                    # Fallback: try to focus the page and type via keyboard
                    try:
                        # attempt to focus the active element (dialog) then type
                        page.keyboard.type(message)
                        typed_message = True
                        print("✓ Typed message using keyboard fallback (no specific selector).")
                    except Exception as e:
                        print(f"⚠️ Failed to type message using fallback: {e}")

                if not typed_message:
                    print("⚠️ Could not attach message/caption to the PDF. Proceeding without message.")

                # ------------------------------
                # Continue with send-button detection & send
                # ------------------------------
                print("Looking for send button...")
                send_selectors = [
                    '[data-testid="send"]',
                    'span[data-icon="send"]',
                    'button[aria-label="Send"]',
                    'div[role="button"][aria-label="Send"]',
                ]

                send_button = None
                for _ in range(120):  # up to ~60s
                    for sel in send_selectors:
                        try:
                            loc = page.locator(sel)
                            if loc.count() > 0 and loc.first.is_visible():
                                send_button = loc.first
                                print(f"✓ Send button found: {sel}")
                                break
                        except Exception:
                            continue
                    if send_button:
                        break
                    time.sleep(0.5)

                if not send_button:
                    page.screenshot(path="no_send_button.png")
                    with open("no_send_page.html", "w", encoding="utf-8") as f:
                        f.write(page.content())
                    raise Exception("Send button not found. Check no_send_button.png and no_send_page.html")

                page.screenshot(path="ready_to_send.png")
                print("Clicking send button...")
                for attempt in range(3):
                    try:
                        send_button.click(timeout=5000)
                        print("✓ Send button clicked")
                        break
                    except Exception as e:
                        if attempt == 2:
                            raise
                        print(f"Click attempt {attempt+1} failed, retrying... ({e})")
                        time.sleep(1)

                # Wait for confirmation that message (PDF) appears in chat
                print("Waiting for message confirmation in chat...")
                sent_confirmed = False
                for i in range(60):
                    try:
                        if page.locator('span[data-icon="document"]').count() > 0:
                            sent_confirmed = True
                            print("✓ PDF message confirmed in chat (document icon found).")
                            break
                        if page.get_by_text(os.path.basename(pdf_path)).count() > 0:
                            sent_confirmed = True
                            print("✓ PDF filename found in chat messages.")
                            break
                    except Exception:
                        pass
                    time.sleep(1)

                if not sent_confirmed:
                    print("⚠️ Could not confirm PDF in chat after waiting. Check screenshots.")

                # Final checks for toasts/errors
                has_error, error_msg = self.check_for_error_toast(page)
                if has_error:
                    raise Exception(f"Send failed with error: {error_msg}")

                print("\n" + "="*60)
                print("✅ PDF SENT SUCCESSFULLY")
                print("="*60)

            except Exception as e:
                print(f"\n❌ ERROR: {e}")
                try:
                    page.screenshot(path="final_error.png")
                    print("Error screenshot: final_error.png")
                except:
                    pass
                raise

            finally:
                print("\nClosing browser in 5 seconds...")
                time.sleep(5)
                browser.close()



def get_yesterday_pdf(directory):
    """Get yesterday's PDF report"""
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    filename = f"sales_report_{yesterday}.pdf"
    pdf_path = os.path.join(directory, filename)

    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"PDF file not found: {pdf_path}")

    return pdf_path


def main():
    PDF_DIRECTORY = "/home/azureuser/azure_analysis_algorithm/reports"
    GROUP_NAME = "FOFO sales/ and query"
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    MESSAGE = f"Sales report of {yesterday}"

    try:
        yesterday_pdf = get_yesterday_pdf(PDF_DIRECTORY)
        print(f"✓ Yesterday's PDF found: {yesterday_pdf}")

        sender = WhatsAppSender()
        # pass message explicitly (optional)
        sender.send_pdf_to_group(GROUP_NAME, yesterday_pdf, message=MESSAGE)

    except Exception as e:
        print(f"\n❌ FAILED: {str(e)}")
        raise


if __name__ == "__main__":
    main()
