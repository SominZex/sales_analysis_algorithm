import os
import sys
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

    def dismiss_popups(self, page):
        """Dismiss any popups or notifications that might be blocking the UI"""
        print("Checking for popups/notifications...")
        
        popup_selectors = [
            'div[data-testid="popup-controls-ok"]',
            'button:has-text("OK")',
            'button:has-text("Not now")',
            'div[role="button"]:has-text("Dismiss")',
            '[data-animate-modal-popup="true"]',
            'span[data-icon="x"]',
            'span[data-icon="x-light"]',
            'div[aria-label="Close"]',
            'button[aria-label="Close"]',
        ]
        
        dismissed_any = False
        for selector in popup_selectors:
            try:
                elements = page.locator(selector)
                if elements.count() > 0:
                    for i in range(min(elements.count(), 3)):
                        try:
                            elem = elements.nth(i)
                            if elem.is_visible():
                                elem.click(timeout=2000)
                                print(f"✓ Dismissed popup: {selector}")
                                dismissed_any = True
                                time.sleep(1)
                                break
                        except:
                            continue
            except:
                continue
        
        if dismissed_any:
            time.sleep(2)
            print("✓ Popups dismissed, continuing...")
        else:
            print("No popups found")
        
        return dismissed_any

    def wait_for_whatsapp_load(self, page, timeout=120):
        """Improved WhatsApp loading detection"""
        print("Checking WhatsApp Web status...")
        start_time = time.time()
        
        page.screenshot(path="whatsapp_initial.png")
        print("Initial screenshot saved: whatsapp_initial.png")

        consecutive_success_checks = 0
        required_consecutive_checks = 3

        while time.time() - start_time < timeout:
            try:
                elapsed = int(time.time() - start_time)
                
                if elapsed % 20 == 0 and elapsed > 0:
                    page.screenshot(path=f"debug_loading_{elapsed}s.png")
                    print(f"Debug screenshot saved: debug_loading_{elapsed}s.png")

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

                # Check for success indicators
                success_indicators = [
                    '[data-testid="chat-list-search"]',
                    'div[contenteditable="true"][data-tab="3"]',
                    '#side',
                    'div#pane-side',
                    'div[data-testid="chatlist-content"]',
                    '[data-testid="cell-frame-container"]',
                ]

                found_success_indicator = False
                for selector in success_indicators:
                    try:
                        elements = page.locator(selector)
                        if elements.count() > 0:
                            elem = elements.first
                            try:
                                if elem.is_visible(timeout=1000):
                                    found_success_indicator = True
                                    print(f"✓ Success indicator found: {selector}")
                                    break
                            except:
                                continue
                    except:
                        continue

                if found_success_indicator:
                    consecutive_success_checks += 1
                    print(f"✓ WhatsApp appears loaded ({consecutive_success_checks}/{required_consecutive_checks} checks)")
                    
                    if consecutive_success_checks >= required_consecutive_checks:
                        print("✓ WhatsApp successfully loaded and stable!")
                        print("Waiting for full initialization...")
                        time.sleep(3)
                        
                        chat_items = page.locator('[data-testid="cell-frame-container"]')
                        chat_count = chat_items.count()
                        if chat_count > 0:
                            print(f"✓ Confirmed: Found {chat_count} chat(s)")
                        else:
                            print("✓ WhatsApp interface loaded (no chats visible yet)")
                        
                        return True
                    
                    time.sleep(2)
                else:
                    if consecutive_success_checks > 0:
                        print(f"⚠️ Lost success indicator, resetting counter")
                    consecutive_success_checks = 0
                    
                    if elapsed % 10 == 0:
                        print(f"Waiting for WhatsApp to load... ({elapsed}s / {timeout}s)")
                    
                    time.sleep(3)

            except Exception as e:
                consecutive_success_checks = 0
                elapsed = int(time.time() - start_time)
                if elapsed % 10 == 0:
                    print(f"Checking... ({elapsed}s) - Error: {str(e)[:100]}")
                time.sleep(3)

        print("\n⚠️  Timeout reached!")
        page.screenshot(path="timeout_screenshot.png")

        try:
            with open("timeout_page.html", "w", encoding="utf-8") as f:
                f.write(page.content())
            print("Page HTML saved to: timeout_page.html")
        except Exception as e:
            print(f"Error saving debug info: {e}")

        return False

    def find_and_open_chat(self, page, group_name):
        """Find and open a chat/group"""
        print(f"\nSearching for: {group_name}")
        
        self.dismiss_popups(page)
        time.sleep(1)
        
        search_selectors = [
            '[data-testid="chat-list-search"]',
            'div[contenteditable="true"][data-tab="3"]',
            '[aria-label="Search input textbox"]',
            'div[role="textbox"][title="Search input textbox"]',
            'div[role="textbox"][data-tab="3"]',
            'div.selectable-text[contenteditable="true"][data-tab="3"]',
            'div[contenteditable="true"][spellcheck="true"][data-tab="3"]',
            'div#side div[role="textbox"]',
            'div[data-testid="chatlist-header"] div[role="textbox"]',
            'header div[contenteditable="true"]',
            'div.copyable-text[contenteditable="true"]',
        ]

        search_box = None
        max_attempts = 40
        
        for attempt in range(max_attempts):
            if attempt > 0 and attempt % 10 == 0:
                self.dismiss_popups(page)
            
            if attempt % 10 == 0 and attempt > 0:
                print(f"Still waiting for search box... ({attempt}/{max_attempts})")
                page.screenshot(path=f"search_wait_{attempt}s.png")
            
            for selector in search_selectors:
                try:
                    elements = page.locator(selector)
                    if elements.count() > 0:
                        elem = elements.first
                        if elem.is_visible(timeout=500):
                            try:
                                contenteditable = elem.get_attribute("contenteditable")
                                if contenteditable == "true" or contenteditable is None:
                                    search_box = elem
                                    print(f"✓ Found search box: {selector}")
                                    break
                            except:
                                search_box = elem
                                print(f"✓ Found search box: {selector}")
                                break
                except Exception as e:
                    continue

            if search_box:
                break
            
            if attempt > 15:
                try:
                    placeholder_search = page.get_by_placeholder("Search or start new chat")
                    if placeholder_search.count() > 0:
                        search_box = placeholder_search.first
                        print("✓ Found search box by placeholder text")
                        break
                except:
                    pass
            
            if attempt > 20:
                try:
                    search_icon = page.locator('span[data-icon="search"]')
                    if search_icon.count() > 0 and search_icon.first.is_visible():
                        print("Attempting to click search icon...")
                        search_icon.first.click()
                        time.sleep(2)
                        continue
                except:
                    pass

            time.sleep(1)

        if not search_box:
            page.screenshot(path="no_search_box.png")
            raise Exception("Could not find search box. Check no_search_box.png")

        print("Attempting to activate search box...")
        for click_attempt in range(5):
            try:
                search_box.click()
                time.sleep(0.5)
                break
            except Exception as e:
                if click_attempt == 4:
                    raise Exception(f"Could not click search box: {e}")
                print(f"Click attempt {click_attempt + 1} failed, retrying...")
                time.sleep(0.5)

        time.sleep(1)
        
        print(f"Typing '{group_name}' in search...")
        page.keyboard.type(group_name, delay=100)
        print(f"✓ Typed '{group_name}' in search")
        time.sleep(3)

        page.screenshot(path="search_results.png")

        # Find and click on the chat
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

        # Verify chat opened
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

    def find_attach_button(self, page, max_wait=30):
        """Robust attach button finder"""
        print("\nLooking for attach button...")
        
        priority_selectors = [
            'div[aria-label="Attach"]',
            'button[aria-label="Attach"]',
            '[data-testid="clip"]',
            'span[data-icon="clip"]',
        ]
        
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            for selector in priority_selectors:
                try:
                    elements = page.locator(selector)
                    count = elements.count()
                    
                    if count > 0:
                        for i in range(count):
                            elem = elements.nth(i)
                            
                            if not elem.is_visible():
                                continue
                            
                            try:
                                in_footer = elem.evaluate("el => !!el.closest('footer')")
                                
                                if in_footer:
                                    print(f"✓ Found attach button")
                                    return elem
                                    
                            except:
                                continue
                                
                except:
                    continue
            
            # Try footer buttons
            try:
                footer_buttons = page.locator('footer button')
                if footer_buttons.count() > 0:
                    for i in range(min(3, footer_buttons.count())):
                        btn = footer_buttons.nth(i)
                        if not btn.is_visible():
                            continue
                        
                        aria_label = btn.get_attribute("aria-label") or ""
                        
                        if "voice" in aria_label.lower():
                            continue
                        
                        has_clip = btn.locator('span[data-icon="clip"]').count() > 0
                        
                        if has_clip or i == 0:
                            print(f"✓ Found attach button in footer")
                            return btn
            except:
                pass
            
            time.sleep(1)
        
        print("❌ Could not find attach button")
        page.screenshot(path="attach_button_not_found.png")
        return None

    def verify_send_simple(self, page):
        """Simple verification - check dialog close AND checkmarks"""
        print("\nVerifying send...")
        
        # STEP 1: Wait for dialog to close
        print("  Waiting for dialog to close...")
        dialog_closed = False
        for wait in range(15):
            dialog_open = False
            for sel in ['div[role="dialog"]', 'div[data-testid="media-viewer"]', 'div[data-testid="document-viewer"]']:
                try:
                    if page.locator(sel).count() > 0:
                        dialog_open = True
                        break
                except:
                    pass
            
            if not dialog_open:
                print(f"  ✓ Dialog closed after {wait}s")
                dialog_closed = True
                break
            time.sleep(1)
        
        if not dialog_closed:
            page.screenshot(path="dialog_not_closed.png")
            return False, "Dialog did not close - send failed"
        
        time.sleep(5)  # Wait for message to appear and get checkmark
        
        # STEP 2: Wait for checkmark (proof of send) - INCREASED TO 60 SECONDS
        print("  Waiting for delivery confirmation...")
        checkmark_found = False
        
        for wait in range(60):  # CHANGED FROM 30 TO 60 SECONDS
            try:
                # Look for ANY checkmark in the last outgoing message
                last_messages = page.locator('div.message-out').all()
                if last_messages:
                    last_msg = last_messages[-1]
                    
                    # Check for error icon in THIS message
                    has_error_icon = last_msg.locator('span[data-icon="msg-dblcheck-error"], span[data-icon="error"]').count() > 0
                    if has_error_icon:
                        page.screenshot(path="message_has_error_icon.png")
                        return False, "Last message has error icon"
                    
                    # Check for retry button in THIS message
                    has_retry = last_msg.locator('span:has-text("Retry")').count() > 0
                    if has_retry:
                        page.screenshot(path="message_has_retry.png")
                        return False, "Last message has retry button"
                    
                    # Check for any type of checkmark
                    single_check = last_msg.locator('span[data-icon="msg-check"]').count() > 0
                    double_check = last_msg.locator('span[data-icon="msg-dblcheck"]').count() > 0
                    blue_check = last_msg.locator('span[data-icon="msg-dblcheck-ack"]').count() > 0
                    
                    if single_check or double_check or blue_check:
                        print(f"  ✓ Checkmark found at {wait}s")
                        checkmark_found = True
                        break
            except Exception as e:
                if wait % 10 == 0:
                    print(f"  Check at {wait}s...")
            
            time.sleep(1)
        
        if not checkmark_found:
            page.screenshot(path="no_checkmark.png")
            
            # Final check - see if last message has error
            try:
                last_messages = page.locator('div.message-out').all()
                if last_messages:
                    last_msg = last_messages[-1]
                    has_error_icon = last_msg.locator('span[data-icon="msg-dblcheck-error"], span[data-icon="error"]').count() > 0
                    has_retry = last_msg.locator('span:has-text("Retry")').count() > 0
                    
                    if has_error_icon or has_retry:
                        page.screenshot(path="send_error_confirmed.png")
                        return False, "Message failed - has error/retry indicator"
            except:
                pass
            
            return False, "No checkmark received after 60s - send likely failed"  # CHANGED FROM 30s
        
        print("  ✓ Send verified with checkmark")
        page.screenshot(path="send_success.png")
        return True, "Send successful with delivery confirmation"

    def send_pdf_to_group(self, group_name, pdf_path, message="Sales report for today."):
        """Send PDF file to WhatsApp group"""
        print("\n" + "="*60)
        print("Starting WhatsApp PDF Sender")
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)

        self.validate_pdf(pdf_path)
        pdf_filename = os.path.basename(pdf_path)

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
                    '--disable-gpu',
                    '--window-size=1920,1080',
                    '--disable-notifications',
                    '--disable-popup-blocking',
                    '--disable-infobars',
                ],
                slow_mo=100,
                viewport={"width": 1920, "height": 1080},
            )

            page = browser.pages[0] if browser.pages else browser.new_page()
            
            page.set_extra_http_headers({
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            })

            try:
                print("Navigating to WhatsApp Web...")
                page.goto('https://web.whatsapp.com', wait_until="domcontentloaded", timeout=60000)
                print("Page loaded, waiting for WhatsApp to initialize...")
                time.sleep(3)

                if not self.wait_for_whatsapp_load(page):
                    raise Exception("WhatsApp failed to load. Check debug screenshots")

                print("✓ WhatsApp loaded successfully!")
                time.sleep(5)
                
                self.dismiss_popups(page)
                time.sleep(2)

                self.find_and_open_chat(page, group_name)
                time.sleep(3)

                attach_button = self.find_attach_button(page, max_wait=30)
                
                if not attach_button:
                    raise Exception("Could not find attach button")

                print("Clicking attach button...")
                try:
                    attach_button.click(timeout=5000)
                    print("✓ Attach button clicked")
                except:
                    try:
                        attach_button.click(force=True)
                        print("✓ Attach button clicked (force)")
                    except:
                        attach_button.evaluate("el => el.click()")
                        print("✓ Attach button clicked (JS)")

                time.sleep(3)

                print("Starting file upload...")
                abs_path = os.path.abspath(pdf_path)
                print(f"File path: {abs_path}")

                # Try file input method
                file_inputs = page.locator('input[type="file"]')
                upload_success = False

                if file_inputs.count() > 0:
                    for i in range(file_inputs.count()):
                        try:
                            inp = file_inputs.nth(i)
                            accept = inp.get_attribute('accept') or ''
                            if 'image' not in accept.lower():
                                inp.set_input_files(abs_path)
                                upload_success = True
                                print("✓ File uploaded")
                                break
                        except:
                            continue

                if not upload_success:
                    try:
                        with page.expect_file_chooser(timeout=10000) as fc_info:
                            doc_button = page.locator('li[data-testid="mi-attach-document"]')
                            if doc_button.count() > 0:
                                doc_button.first.click()
                            else:
                                page.locator('li[role="button"]').nth(1).click()
                        file_chooser = fc_info.value
                        file_chooser.set_files(abs_path)
                        upload_success = True
                        print("✓ File uploaded via chooser")
                    except Exception as e:
                        raise Exception(f"File upload failed: {e}")

                if not upload_success:
                    raise Exception("File upload failed")

                print("Waiting for file processing...")
                time.sleep(8)

                # Try to type message (optional - don't fail if it doesn't work)
                print("Attempting to type caption...")
                try:
                    page.keyboard.type(message, delay=50)
                    print("✓ Caption typed")
                except:
                    print("⚠️  Could not type caption (will send without)")

                time.sleep(2)

                print("Looking for send button...")
                send_selectors = [
                    '[data-testid="send"]',
                    'span[data-icon="send"]',
                    'button[aria-label="Send"]',
                    'div[role="button"][aria-label="Send"]',
                ]

                send_button = None
                for wait_attempt in range(60):
                    for sel in send_selectors:
                        try:
                            loc = page.locator(sel)
                            if loc.count() > 0 and loc.first.is_visible():
                                send_button = loc.first
                                print(f"✓ Send button found")
                                break
                        except:
                            continue
                    if send_button:
                        break
                    time.sleep(0.5)

                if not send_button:
                    page.screenshot(path="no_send_button.png")
                    raise Exception("Send button not found")

                print("Clicking send button...")
                send_button.click(timeout=5000)
                print("✓ Send button clicked")

                # Simple verification
                success, msg = self.verify_send_simple(page)
                
                if not success:
                    raise Exception(f"Send verification failed: {msg}")

                print("\n" + "="*60)
                print("✅ PDF SENT SUCCESSFULLY")
                print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print("="*60)

            except Exception as e:
                print(f"\n❌ ERROR: {e}")
                try:
                    page.screenshot(path="final_error.png")
                except:
                    pass
                raise

            finally:
                print("\nClosing browser...")
                time.sleep(3)
                browser.close()


def get_yesterday_pdf(directory):
    """Get yesterday's PDF report"""
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    filename = f"sales_report_{yesterday}.pdf"
    pdf_path = os.path.join(directory, filename)

    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    return pdf_path


def main():
    PDF_DIRECTORY = "/home/azureuser/azure_analysis_algorithm/reports"
    GROUP_NAME = "FOFO sales/ and query"
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    MESSAGE = f"Sales report of {yesterday}"

    print("="*60)
    print("WhatsApp PDF Sender - Automated Run")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    try:
        yesterday_pdf = get_yesterday_pdf(PDF_DIRECTORY)
        print(f"✓ PDF found: {yesterday_pdf}")

        sender = WhatsAppSender()
        sender.send_pdf_to_group(GROUP_NAME, yesterday_pdf, message=MESSAGE)
        
        print("\n" + "="*60)
        print("✅ SCRIPT COMPLETED SUCCESSFULLY")
        print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)

    except Exception as e:
        print(f"\n❌ SCRIPT FAILED: {str(e)}")
        print(f"Failed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        raise


if __name__ == "__main__":
    main()