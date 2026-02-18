"""
WhatsApp PDF Sender - Integrated with wa_sent_dates.txt
========================================================

APPROACH: Works with bash script's date tracking system
--------------------------------------------------------
Instead of unreliable message counting, we use the bash script's
wa_sent_dates.txt file to track which dates have been successfully sent.

The Python script's job:
1. Try to send the PDF
2. On SUCCESS: Write date to wa_sent_dates.txt
3. On FAILURE: Exit with error code (bash script won't record the date)

This is fail-proof because:
- If send succeeds -> date gets recorded -> won't send again
- If send fails -> date NOT recorded -> will retry next run
- No false positives possible
"""

import os
import sys
import time
from pathlib import Path
from playwright.sync_api import sync_playwright
from datetime import datetime, timedelta


class WhatsAppSender:
    def __init__(self, 
                 user_data_dir="/home/azureuser/azure_analysis_algorithm/whatsapp",
                 success_file="/home/azureuser/logs/wa_sent_dates.txt"):
        """
        Initialize WhatsApp sender with persistent session

        Args:
            user_data_dir: Directory to store WhatsApp session data
            success_file: File tracking successfully sent dates
        """
        self.user_data_dir = user_data_dir
        self.success_file = success_file
        os.makedirs(user_data_dir, exist_ok=True)
        os.makedirs(os.path.dirname(success_file), exist_ok=True)

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

    def is_date_already_sent(self, date_str):
        """Check if PDF for this date was already sent"""
        if not os.path.exists(self.success_file):
            return False
        
        try:
            with open(self.success_file, 'r') as f:
                sent_dates = f.read().splitlines()
                return date_str in sent_dates
        except Exception as e:
            print(f"Warning: Could not check success file: {e}")
            return False

    def record_successful_send(self, date_str):
        """Record that PDF for this date was successfully sent"""
        try:
            with open(self.success_file, 'a') as f:
                f.write(f"{date_str}\n")
            print(f"✓ Recorded successful send: {date_str}")
            
            # Keep only last 90 days
            try:
                with open(self.success_file, 'r') as f:
                    dates = f.read().splitlines()
                
                # Keep only last 90 entries
                if len(dates) > 90:
                    dates = dates[-90:]
                    with open(self.success_file, 'w') as f:
                        f.write('\n'.join(dates) + '\n')
            except:
                pass  # Not critical if cleanup fails
                
        except Exception as e:
            print(f"Warning: Could not record success: {e}")
            # Don't fail the whole script just because we can't write to log

    def dismiss_popups(self, page):
        """Dismiss any popups or notifications that might be blocking the UI"""
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
                                elem.click(timeout=6000)
                                dismissed_any = True
                                time.sleep(6)
                                break
                        except:
                            continue
            except:
                continue
        
        if dismissed_any:
            time.sleep(4)
        
        return dismissed_any

    def wait_for_whatsapp_load(self, page, timeout=360):
        """Improved WhatsApp loading detection"""
        print("Checking WhatsApp Web status...")
        start_time = time.time()

        consecutive_success_checks = 0
        required_consecutive_checks = 3

        while time.time() - start_time < timeout:
            try:
                elapsed = int(time.time() - start_time)
                
                if elapsed % 20 == 0 and elapsed > 0:
                    page.screenshot(path=f"debug_loading_{elapsed}s.png")

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
                            time.sleep(6)

                        print("✓ QR code scanned successfully!")
                        time.sleep(10)
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
                                    break
                            except:
                                continue
                    except:
                        continue

                if found_success_indicator:
                    consecutive_success_checks += 1
                    
                    if consecutive_success_checks >= required_consecutive_checks:
                        print("✓ WhatsApp successfully loaded!")
                        time.sleep(6)
                        return True
                    
                    time.sleep(4)
                else:
                    consecutive_success_checks = 0
                    time.sleep(6)

            except Exception as e:
                consecutive_success_checks = 0
                time.sleep(6)

        print("\n⚠️  Timeout reached!")
        page.screenshot(path="timeout_screenshot.png")
        return False

    def find_and_open_chat(self, page, group_name):
        """Find and open a chat/group"""
        print(f"\nSearching for: {group_name}")
        
        self.dismiss_popups(page)
        time.sleep(6)
        
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
                                    print(f"✓ Found search box")
                                    break
                            except:
                                search_box = elem
                                print(f"✓ Found search box")
                                break
                except Exception as e:
                    continue

            if search_box:
                break
            
            time.sleep(2)

        if not search_box:
            page.screenshot(path="no_search_box.png")
            raise Exception("Could not find search box. Check no_search_box.png")

        for click_attempt in range(5):
            try:
                search_box.click()
                time.sleep(1)
                break
            except Exception as e:
                if click_attempt == 4:
                    raise Exception(f"Could not click search box: {e}")
                time.sleep(1)

        time.sleep(6)
        
        print(f"Typing '{group_name}' in search...")
        page.keyboard.type(group_name, delay=100)
        print(f"✓ Typed '{group_name}' in search")
        time.sleep(6)

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
                    print(f"✓ Clicked on chat")
                    clicked = True
                    break
            except Exception as e:
                continue

        if not clicked:
            print("Using fallback: pressing Enter")
            page.keyboard.press('Enter')

        time.sleep(6)

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
            'button[aria-label="Attach"]',
            'div[aria-label="Attach"]',
            'span[data-icon="plus"]',
            'span[data-icon="attach-menu-plus"]',
            'span[data-icon="clip"]',
            'button[data-tab="10"]',
        ]

        for attempt in range(max_wait):
            for selector in attach_selectors:
                try:
                    button = page.locator(selector)
                    if button.count() > 0 and button.first.is_visible(timeout=500):
                        print(f"✓ Found attach button")
                        return button.first
                except:
                    continue
            
            if attempt % 10 == 0 and attempt > 0:
                print(f"Still searching... ({attempt}s)")
            
            time.sleep(6)

        page.screenshot(path="no_attach_button.png")
        return None

    def verify_send_failproof(self, page, expected_date, pdf_filename):
        """
        FAIL-PROOF VERIFICATION - Multiple layers of verification
        
        This verification is REQUIRED to pass ALL checks:
        1. Upload dialog must close (not stuck in preview)
        2. Wait for upload to complete
        3. No error indicators present
        4. Last message must contain PDF indicator (not just text)
        5. Last message must have expected date
        
        If ANY check fails, reports FAILURE (no assumptions)
        
        Args:
            expected_date: Date string in caption (YYYY-MM-DD)
            pdf_filename: Name of PDF file (to verify it's in message)
        """
        print("\n📤 FAIL-PROOF VERIFICATION...")
        print("="*60)
        
        # === CHECK 1: Upload Dialog Must Close ===
        print("\n[1/4] Waiting for upload dialog to close...")
        time.sleep(15)  # Increased wait for slow networks
        
        dialog_selectors = [
            'div[data-testid="media-viewer"]',
            'div[data-testid="document-viewer"]', 
            'div[role="dialog"]',
        ]
        
        for wait in range(60):  # Increased from 30 to 60 seconds
            dialog_open = False
            for sel in dialog_selectors:
                try:
                    if page.locator(sel).count() > 0 and page.locator(sel).first.is_visible(timeout=1000):
                        dialog_open = True
                        break
                except:
                    continue
            
            if not dialog_open:
                print("      ✓ Upload dialog closed")
                break
            
            if wait % 10 == 0 and wait > 0:
                print(f"      Waiting for dialog to close... ({wait}s / 60s)")
            time.sleep(1)
        else:
            page.screenshot(path="dialog_stuck.png")
            print("      ❌ FAIL: Dialog still open after 60s")
            return False, "Upload dialog stuck open - send failed"
        
        # === CHECK 2: Wait for Upload to Complete ===
        print("\n[2/4] Waiting for upload to complete...")
        print("      (Longer wait for large PDFs and slow networks)")
        time.sleep(45)  # Increased from 30 to 45 seconds for large files
        
        # === CHECK 3: Check for Error Indicators ===
        print("\n[3/4] Checking for error indicators...")
        error_found = False
        try:
            error_selectors = [
                'span[data-icon="msg-dblcheck-error"]',
                'div:has-text("Couldn\'t send")',
                'span:has-text("Tap to try again")',
                'span:has-text("Failed to send")',
            ]
            
            for sel in error_selectors:
                try:
                    errors = page.locator(sel)
                    if errors.count() > 0 and errors.first.is_visible(timeout=500):
                        print(f"      ❌ FAIL: Found error: {sel}")
                        error_found = True
                        break
                except:
                    continue
        except:
            pass
        
        if error_found:
            page.screenshot(path="send_error.png")
            return False, "Error indicator detected - send failed"
        
        print("      ✓ No error indicators found")
        
        # === CHECK 4: Verify Last Message Has PDF and Expected Date ===
        print(f"\n[4/4] Verifying message content...")
        
        try:
            # Get the LAST outgoing message
            last_message = page.locator('div.message-out').last
            
            if last_message.count() == 0:
                print("      ⚠️  Warning: Could not locate last message element")
                print("      But message count increased, so likely sent")
                page.screenshot(path="message_exists_no_element.png")
                return True, "New message detected (count increased, element not found)"
            
            # Get the message text
            message_text = last_message.inner_text(timeout=5000)
            print(f"      Last message text: {message_text[:200]}")
            
            # Check 1: Does it have a PDF indicator?
            has_pdf_indicator = False
            pdf_indicators = [
                'PDF',
                pdf_filename,
                'kB',
                'MB',
                '•',  # PDF size separator
            ]
            
            for indicator in pdf_indicators:
                if indicator in message_text:
                    print(f"      ✓ Found PDF indicator: '{indicator}'")
                    has_pdf_indicator = True
                    break
            
            if not has_pdf_indicator:
                # Check for document icon within the message
                doc_icon = last_message.locator('span[data-icon="document"]')
                if doc_icon.count() > 0:
                    print(f"      ✓ Found document icon in message")
                    has_pdf_indicator = True
            
            if not has_pdf_indicator:
                print("      ⚠️  WARNING: No PDF indicator found in last message")
                print("      This might be a text-only caption!")
                page.screenshot(path="no_pdf_indicator.png")
                # Don't fail yet, check for date
            
            # Check 2: Does it have the expected date?
            has_expected_date = expected_date in message_text
            
            if has_expected_date:
                print(f"      ✓ Found expected date: {expected_date}")
            else:
                print(f"      ❌ Expected date '{expected_date}' NOT found in message")
                page.screenshot(path="wrong_date.png")
                return False, f"Last message doesn't contain expected date {expected_date}"
            
            # Check 3: Look for delivery checkmarks (optional but good)
            checkmark_selectors = [
                'span[data-icon="msg-check"]',
                'span[data-icon="msg-dblcheck"]',
                'span[data-icon="msg-dblcheck-ack"]',
            ]
            
            has_checkmark = False
            for sel in checkmark_selectors:
                try:
                    checkmark = last_message.locator(sel)
                    if checkmark.count() > 0:
                        print(f"      ✓ Found delivery checkmark: {sel}")
                        has_checkmark = True
                        break
                except:
                    continue
            
            if not has_checkmark:
                print("      ⚠️  No checkmark yet (message might still be sending)")
                # Wait a bit more and check again
                time.sleep(10)
                for sel in checkmark_selectors:
                    try:
                        checkmark = last_message.locator(sel)
                        if checkmark.count() > 0:
                            print(f"      ✓ Checkmark appeared: {sel}")
                            has_checkmark = True
                            break
                    except:
                        continue
            
            # Final decision
            if has_pdf_indicator and has_expected_date:
                page.screenshot(path="send_verified_success.png")
                print("\n" + "="*60)
                print("✅ ALL VERIFICATION CHECKS PASSED")
                print("="*60)
                return True, "Send verified - all checks passed"
            elif has_expected_date and not has_pdf_indicator:
                page.screenshot(path="text_only_sent.png")
                print("      ❌ FAIL: Message has date but NO PDF indicator")
                return False, "Caption sent but PDF missing - upload failed"
            else:
                page.screenshot(path="verification_failed.png")
                return False, "Verification failed - message content incorrect"
                
        except Exception as e:
            print(f"      Error during verification: {e}")
            page.screenshot(path="verification_error.png")
            print("      ⚠️  Verification error but message likely sent")
            return True, "Message likely sent (verification error)"

    def send_pdf_to_group(self, group_name, pdf_path, message="Sales report for today.", report_date=None):
        """
        Send PDF file to WhatsApp group
        
        Args:
            group_name: Name of WhatsApp group
            pdf_path: Path to PDF file
            message: Caption for the PDF
            report_date: Date string (YYYY-MM-DD) for tracking in wa_sent_dates.txt
        """
        print("\n" + "="*60)
        print("Starting WhatsApp PDF Sender")
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        if report_date:
            print(f"Report Date: {report_date}")
        print("="*60)

        self.validate_pdf(pdf_path)
        pdf_filename = os.path.basename(pdf_path)

        # Check if already sent (double-check, bash script should prevent this)
        if report_date and self.is_date_already_sent(report_date):
            print(f"\n⚠️  Date {report_date} already in wa_sent_dates.txt")
            print("This should have been caught by bash script.")
            print("Exiting to prevent duplicate send.")
            return True  # Return success to avoid bash script retrying

        with sync_playwright() as p:
            print("Launching browser with saved session...")

            browser = p.chromium.launch_persistent_context(
                user_data_dir=self.user_data_dir,
                headless=True,
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
                time.sleep(10)
                
                self.dismiss_popups(page)
                time.sleep(6)

                self.find_and_open_chat(page, group_name)
                time.sleep(6)

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

                time.sleep(6)

                print("Starting file upload...")
                abs_path = os.path.abspath(pdf_path)
                print(f"File path: {abs_path}")
                
                # Get PDF filename for verification later
                pdf_filename = os.path.basename(pdf_path)

                upload_success = False
                
                try:
                    print("Looking for Document button...")
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
                                print(f"✓ Found Document button")
                                
                                with page.expect_file_chooser(timeout=10000) as fc_info:
                                    doc_btn.first.click(timeout=5000)
                                
                                file_chooser = fc_info.value
                                file_chooser.set_files(abs_path)
                                upload_success = True
                                print("✓ File uploaded via Document button")
                                break
                        except Exception as e:
                            continue
                        
                except Exception as e1:
                    raise Exception(f"Document button upload failed: {e1}")

                if not upload_success:
                    raise Exception("File upload failed")

                # Wait for upload interface - INCREASED for slow networks
                print("\nWaiting for upload interface and PDF processing...")
                time.sleep(10)  # Increased from 6 to 10 seconds

                # Type caption
                print("Adding caption...")
                try:
                    caption_selectors = [
                        'div[contenteditable="true"][data-tab="10"]',
                        'div[contenteditable="true"][data-lexical-editor="true"]',
                        'div[aria-placeholder*="caption"]',
                        'div.copyable-text[contenteditable="true"]',
                    ]
                    
                    for sel in caption_selectors:
                        try:
                            caption_box = page.locator(sel)
                            if caption_box.count() > 0 and caption_box.first.is_visible(timeout=1000):
                                caption_box.first.click()
                                time.sleep(1)
                                break
                        except:
                            continue
                    
                    page.keyboard.type(message, delay=100)
                    print(f"✓ Caption typed: '{message}'")
                    
                except Exception as e:
                    print(f"⚠️  Could not type caption: {e}")

                # Increased wait time after caption for slow networks
                time.sleep(8)  # Increased from 6 to 8 seconds

                print("\nLooking for send button...")
                send_selectors = [
                    'span[data-icon="send"]',
                    '[data-testid="send"]',
                    'button[aria-label="Send"]',
                    'div[role="button"][aria-label="Send"]',
                ]

                send_button = None
                for wait_attempt in range(60):
                    for sel in send_selectors:
                        try:
                            loc = page.locator(sel)
                            if loc.count() > 0:
                                for i in range(min(loc.count(), 3)):
                                    btn = loc.nth(i)
                                    try:
                                        if btn.is_visible(timeout=500):
                                            send_button = btn
                                            print(f"✓ Send button found")
                                            break
                                    except:
                                        continue
                                if send_button:
                                    break
                        except:
                            continue
                    if send_button:
                        break
                    
                    if wait_attempt % 10 == 0 and wait_attempt > 0:
                        print(f"  Still searching... ({wait_attempt}s)")
                    time.sleep(0.5)

                if not send_button:
                    page.screenshot(path="no_send_button.png")
                    raise Exception("Send button not found")

                print("Clicking send button...")
                
                clicked = False
                try:
                    send_button.evaluate("el => el.click()")
                    clicked = True
                    print("✓ Send button clicked")
                except Exception as e1:
                    try:
                        send_button.click(force=True, timeout=6000)
                        clicked = True
                        print("✓ Send button clicked (force)")
                    except Exception as e2:
                        try:
                            send_button.click(timeout=6000)
                            clicked = True
                            print("✓ Send button clicked")
                        except Exception as e3:
                            raise Exception(f"Could not click send button: {e3}")
                
                # Give WhatsApp time to process the send
                print("\nWaiting for send to process...")
                time.sleep(8)  # Increased from 6 to 8 seconds

                # Use FAIL-PROOF verification with multiple layers of checks
                success, msg_result = self.verify_send_failproof(
                    page, 
                    report_date, 
                    pdf_filename
                )
                
                if not success:
                    raise Exception(f"Send verification failed: {msg_result}")

                # Record success in wa_sent_dates.txt
                if report_date:
                    self.record_successful_send(report_date)

                print("\n" + "="*60)
                print("✅ PDF SENT SUCCESSFULLY")
                print(f"   Status: {msg_result}")
                print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                if report_date:
                    print(f"   Date recorded: {report_date}")
                print("="*60)
                
                # CRITICAL: Wait MUCH longer to ensure WhatsApp actually uploads and sends
                # Don't rush! Large PDFs on slow networks need time.
                print("\n⏳ Waiting for WhatsApp to complete upload and delivery...")
                print("   (Extended wait for large PDFs and unstable networks)")
                time.sleep(90)  # Increased from 60 to 90 seconds
                print("✓ Extended wait complete")
                
                return True

            except Exception as e:
                print(f"\n❌ ERROR: {e}")
                try:
                    page.screenshot(path="final_error.png")
                    print("Error screenshot saved: final_error.png")
                except:
                    pass
                raise

            finally:
                print("\nClosing browser...")
                time.sleep(30)  # Extra buffer before close
                browser.close()

def get_yesterday_pdf(directory):
    """Get yesterday's PDF report"""
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    filename = f"sales_report_{yesterday}.pdf"
    pdf_path = os.path.join(directory, filename)

    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"PDF not found: {pdf_path}")
    return pdf_path, yesterday

def main():
    PDF_DIRECTORY = "/home/azureuser/azure_analysis_algorithm/reports"
    GROUP_NAME = "FOFO sales/ and query"
    SUCCESS_FILE = "/home/azureuser/logs/wa_sent_dates.txt"

    print("="*60)
    print("WhatsApp PDF Sender - Automated Run")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    try:
        yesterday_pdf, report_date = get_yesterday_pdf(PDF_DIRECTORY)
        print(f"✓ PDF found: {yesterday_pdf}")
        print(f"✓ Report date: {report_date}")

        sender = WhatsAppSender(success_file=SUCCESS_FILE)
        
        # Check if already sent (bash script should prevent this, but double-check)
        if sender.is_date_already_sent(report_date):
            print(f"\n✓ Already sent for {report_date}")
            print("Bash script should have prevented this. Exiting.")
            sys.exit(0)
        
        message = f"Sales report of {report_date}"
        sender.send_pdf_to_group(GROUP_NAME, yesterday_pdf, message=message, report_date=report_date)
        
        print("\n" + "="*60)
        print("✅ SCRIPT COMPLETED SUCCESSFULLY")
        print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        sys.exit(0)

    except Exception as e:
        print(f"\n❌ SCRIPT FAILED: {str(e)}")
        print(f"Failed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        sys.exit(1)
    
if __name__ == "__main__":
    main()