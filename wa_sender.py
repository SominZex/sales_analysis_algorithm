"""
WhatsApp PDF Sender - SIMPLIFIED VERSION
=========================================

MAJOR SIMPLIFICATION: Removed Complex Verification
---------------------------------------------------
Problem: The original verification logic was overly complex and unreliable:
- Message counting was broken (counts decreasing instead of increasing)
- Sync checking caused false positives
- Checkmark detection had timing issues
- All of this led to unnecessary retries and duplicate messages

Solution: Replaced with simple, reliable approach:
1. Click send button
2. Wait for send dialog to close (10 seconds)
3. Wait for processing (15 seconds)  
4. Quick check for obvious error indicators
5. If no errors → assume success ✓

Why This Works:
- WhatsApp Web is reliable - if the send button works and dialog closes, it sent
- The old verification was trying to outsmart WhatsApp and failing
- Simple wait + error check is far more reliable than complex state tracking
- Extended waits ensure PDFs fully upload before verification

This prevents false positives while still catching genuine failures.
"""

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
        """Find the attachment button with multiple strategies"""
        print("\nSearching for attach button...")
        
        attach_selectors = [
            'div[title="Attach"]',
            'span[data-icon="plus"]',
            'span[data-icon="attach-menu-plus"]',
            'button[aria-label="Attach"]',
            'div[aria-label="Attach"]',
            'span[data-icon="clip"]',
        ]
        
        for wait in range(max_wait):
            if wait % 10 == 0 and wait > 0:
                print(f"Still searching for attach button... ({wait}s)")
            
            for selector in attach_selectors:
                try:
                    elements = page.locator(selector)
                    if elements.count() > 0:
                        for i in range(min(elements.count(), 3)):
                            try:
                                btn = elements.nth(i)
                                if btn.is_visible(timeout=500):
                                    print(f"✓ Found attach button: {selector}")
                                    return btn
                            except:
                                continue
                except:
                    continue
            
            try:
                footer_buttons = page.locator('footer div[role="button"]')
                if footer_buttons.count() > 0:
                    for i in range(min(footer_buttons.count(), 5)):
                        btn = footer_buttons.nth(i)
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
        """
        SIMPLIFIED: Just wait for the send to complete
        The complex verification was causing false positives and failures.
        
        INCREASED WAIT TIMES: Extended waits to ensure PDF actually uploads before verification
        """
        print("\n📤 Verifying send...")
        
        # Wait for dialog to close (confirmation that send button was clicked)
        print("  Waiting for send dialog to close...")
        time.sleep(10)  # Increased from 5 to 10 seconds
        
        # Check if dialog is still open (would indicate a problem)
        dialog_selectors = ['div[role="dialog"]', 'div[data-testid="media-viewer"]', 'div[data-testid="document-viewer"]']
        dialog_still_open = False
        for sel in dialog_selectors:
            try:
                if page.locator(sel).count() > 0 and page.locator(sel).first.is_visible(timeout=1000):
                    dialog_still_open = True
                    break
            except:
                pass
        
        if dialog_still_open:
            page.screenshot(path="dialog_stuck_open.png")
            return False, "Send dialog is still open - send may have failed"
        
        # Give WhatsApp time to process the send
        print("  Waiting for message to be processed...")
        time.sleep(15)  # Increased from 8 to 15 seconds
        
        # Quick check for obvious error indicators
        try:
            error_msgs = page.locator('span:has-text("Failed to send"), span:has-text("Retry"), span[data-icon="msg-dblcheck-error"]')
            if error_msgs.count() > 0:
                print("  ⚠️ Found error indicator")
                page.screenshot(path="error_indicator_found.png")
                return False, "Error indicator detected in chat"
        except:
            pass
        
        print("  ✓ Send completed (dialog closed, no errors detected)")
        page.screenshot(path="send_success.png")
        return True, "Send completed successfully"

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

                upload_success = False
                
                # Strategy 1: Try clicking Document button first (most reliable)
                try:
                    print("Looking for Document button in menu...")
                    doc_selectors = [
                        'li[data-testid="mi-attach-document"]',
                        'button:has-text("Document")',
                        'li:has-text("Document")',
                        '[aria-label*="Document"]',
                    ]
                    
                    for selector in doc_selectors:
                        try:
                            doc_btn = page.locator(selector)
                            if doc_btn.count() > 0 and doc_btn.first.is_visible(timeout=2000):
                                print(f"✓ Found Document button: {selector}")
                                
                                # Click document button and wait for file chooser
                                with page.expect_file_chooser(timeout=10000) as fc_info:
                                    doc_btn.first.click(timeout=5000)
                                
                                file_chooser = fc_info.value
                                file_chooser.set_files(abs_path)
                                upload_success = True
                                print("✓ File uploaded via Document button")
                                break
                        except Exception as e:
                            continue
                    
                    if upload_success:
                        pass  # Success, move on
                    else:
                        raise Exception("Document button method failed")
                        
                except Exception as e1:
                    print(f"  Document button method failed: {e1}")
                    
                    # Strategy 2: Try file input (might work for some WhatsApp versions)
                    try:
                        print("Trying file input method...")
                        file_inputs = page.locator('input[type="file"]')
                        
                        if file_inputs.count() > 0:
                            for i in range(file_inputs.count()):
                                try:
                                    inp = file_inputs.nth(i)
                                    accept = inp.get_attribute('accept') or ''
                                    print(f"  Input {i}: accept='{accept}'")
                                    
                                    # Try any file input that's not image-only
                                    if not accept or '*' in accept or 'image' not in accept.lower():
                                        inp.set_input_files(abs_path)
                                        upload_success = True
                                        print("✓ File uploaded via input")
                                        break
                                except Exception as e:
                                    continue
                        
                        if not upload_success:
                            raise Exception("File input method failed")
                            
                    except Exception as e2:
                        print(f"  File input method failed: {e2}")
                        
                        # Strategy 3: Click menu items by position (last resort)
                        try:
                            print("Trying menu position method...")
                            with page.expect_file_chooser(timeout=10000) as fc_info:
                                # Try different menu positions
                                menu_items = page.locator('li[role="button"]')
                                if menu_items.count() > 1:
                                    menu_items.nth(0).click()  # Try first item (often Document)
                                else:
                                    raise Exception("No menu items found")
                            
                            file_chooser = fc_info.value
                            file_chooser.set_files(abs_path)
                            upload_success = True
                            print("✓ File uploaded via menu position")
                        except Exception as e3:
                            raise Exception(f"All upload methods failed. Last error: {e3}")

                if not upload_success:
                    raise Exception("File upload failed - no method worked")

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
                                print(f"✓ Send button found: {sel}")
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
                # Try multiple click strategies to handle overlapping elements
                clicked = False
                
                # Strategy 1: Normal click
                try:
                    send_button.click(timeout=3000)
                    clicked = True
                    print("✓ Send button clicked")
                except Exception as e1:
                    print(f"  Normal click failed: {str(e1)[:100]}")
                
                # Strategy 2: Force click (ignore overlapping elements)
                if not clicked:
                    try:
                        send_button.click(force=True, timeout=3000)
                        clicked = True
                        print("✓ Send button clicked (force)")
                    except Exception as e2:
                        print(f"  Force click failed: {str(e2)[:100]}")
                
                # Strategy 3: JavaScript click
                if not clicked:
                    try:
                        send_button.evaluate("el => el.click()")
                        clicked = True
                        print("✓ Send button clicked (JS)")
                    except Exception as e3:
                        print(f"  JS click failed: {str(e3)[:100]}")
                
                # Strategy 4: Press Enter key
                if not clicked:
                    try:
                        page.keyboard.press("Enter")
                        clicked = True
                        print("✓ Sent via Enter key")
                    except Exception as e4:
                        print(f"  Enter key failed: {str(e4)[:100]}")
                
                if not clicked:
                    page.screenshot(path="send_button_click_failed.png")
                    raise Exception("Could not click send button with any method")


                # Verify the send
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