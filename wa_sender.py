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

    def get_initial_message_state(self, page):
        """Get comprehensive state of messages before sending - IMPROVED"""
        try:
            print("\n=== CAPTURING INITIAL MESSAGE STATE ===")
            
            # Count all outgoing messages
            all_outgoing = page.locator('div.message-out').count()
            
            # Count specifically document messages (ALL, not just outgoing)
            all_doc_messages = page.locator('span[data-icon="document"]').count()
            
            # Count OUTGOING document messages specifically
            outgoing_doc_messages = page.locator('div.message-out span[data-icon="document"]').count()
            
            # Get the LAST outgoing message's inner HTML for comparison
            last_outgoing_html = None
            try:
                last_messages = page.locator('div.message-out').all()
                if len(last_messages) > 0:
                    last_msg = last_messages[-1]
                    last_outgoing_html = last_msg.inner_html()[:200]  # First 200 chars as fingerprint
            except:
                pass
            
            state = {
                'total_outgoing': all_outgoing,
                'all_doc_messages': all_doc_messages,
                'outgoing_doc_messages': outgoing_doc_messages,
                'last_outgoing_html': last_outgoing_html,
                'capture_time': time.time()
            }
            
            print(f"  Total outgoing messages: {all_outgoing}")
            print(f"  All document messages: {all_doc_messages}")
            print(f"  Outgoing document messages: {outgoing_doc_messages}")
            print(f"  Last outgoing message captured: {last_outgoing_html is not None}")
            print("=" * 40)
            
            return state
            
        except Exception as e:
            print(f"⚠️  Error capturing initial state: {e}")
            return {
                'total_outgoing': 0,
                'all_doc_messages': 0,
                'outgoing_doc_messages': 0,
                'last_outgoing_html': None,
                'capture_time': time.time()
            }

    def check_for_send_errors(self, page):
        """Check for WhatsApp error indicators - IMPROVED with stricter checks"""
        print("  Checking for error indicators...")
        
        # 1. Check for error icons in recent messages
        try:
            error_icons = page.locator('div.message-out span[data-icon="msg-time"][data-icon="error"], div.message-out span[data-icon="msg-dblcheck-error"]')
            if error_icons.count() > 0:
                print("  ❌ Found error icon in message")
                return True, "Message shows error icon"
        except:
            pass
        
        # 2. Check for pending/clock icons (message stuck sending)
        try:
            pending_icons = page.locator('div.message-out span[data-icon="msg-time"][data-icon="msg-time"], div.message-out span[data-icon="msg-check"]')
            if pending_icons.count() > 0:
                # This is normal initially, but we'll check it over time
                pass
        except:
            pass
        
        # 3. Check for error toast messages
        error_selectors = [
            'div[data-testid="toast-container"]:has-text("couldn\'t send")',
            'div[data-testid="toast-container"]:has-text("failed")',
            'div[data-testid="toast-container"]:has-text("error")',
            'div[role="alert"]:has-text("couldn\'t send")',
            'div[role="alert"]:has-text("failed")',
        ]
        
        for selector in error_selectors:
            try:
                elem = page.locator(selector)
                if elem.count() > 0 and elem.first.is_visible(timeout=500):
                    text = elem.first.inner_text(timeout=1000)
                    if len(text) > 3:
                        print(f"  ❌ Found error toast: {text}")
                        return True, f"Error toast: {text}"
            except:
                continue
        
        # 4. Check for "Retry" or "Send again" buttons
        try:
            retry_btns = page.locator('div.message-out button:has-text("Retry"), div.message-out span:has-text("Retry")')
            if retry_btns.count() > 0:
                print("  ❌ Found 'Retry' button - message failed")
                return True, "Message has 'Retry' button"
        except:
            pass
        
        return False, None

    def verify_message_sent(self, page, pdf_filename, initial_state, timeout=30):
        """
        BULLETPROOF: Check if a NEW message with PDF was sent (not old messages)
        Returns: (success, reason)
        """
        print("\n" + "="*60)
        print("VERIFYING NEW PDF MESSAGE WAS SENT")
        print("="*60)
        print(f"Initial state: {initial_state['total_outgoing']} outgoing messages")
        print(f"Must detect: NEW message after position {initial_state['total_outgoing']}")
        print("="*60)
        
        # Wait for WhatsApp to process
        print("Waiting for send to complete...")
        time.sleep(10)
        
        # Take screenshot for debugging
        page.screenshot(path="after_send.png")
        
        # CRITICAL: Get current message count
        try:
            current_outgoing_count = page.locator('div.message-out').count()
            print(f"\nCurrent outgoing messages: {current_outgoing_count}")
            
            if current_outgoing_count <= initial_state['total_outgoing']:
                print(f"❌ Message count did NOT increase!")
                print(f"   Expected: >{initial_state['total_outgoing']}")
                print(f"   Got: {current_outgoing_count}")
                page.screenshot(path="no_new_message.png")
                return False, f"No new message detected (still {current_outgoing_count})"
            
            print(f"✓ Message count increased: {initial_state['total_outgoing']} → {current_outgoing_count}")
            print(f"✓ New messages: +{current_outgoing_count - initial_state['total_outgoing']}")
            
        except Exception as e:
            print(f"❌ Error counting messages: {e}")
            return False, f"Could not verify message count: {e}"
        
        # Now check the NEWEST message (the one we just sent)
        try:
            print(f"\nExamining the NEWEST outgoing message (position {current_outgoing_count})...")
            outgoing_messages = page.locator('div.message-out').all()
            
            if len(outgoing_messages) == 0:
                print("❌ No outgoing messages found at all")
                return False, "No outgoing messages found"
            
            # Get the LAST message (the newest one)
            newest_message = outgoing_messages[-1]
            
            # Check 1: Does it have a document icon?
            doc_icon_count = newest_message.locator('span[data-icon="document"]').count()
            has_document_icon = doc_icon_count > 0
            
            print(f"  Document icon in newest message: {has_document_icon}")
            
            # Check 2: Does it contain the PDF filename?
            try:
                newest_msg_text = newest_message.inner_text()
                has_pdf_filename = pdf_filename in newest_msg_text
                print(f"  PDF filename in newest message: {has_pdf_filename}")
                if has_pdf_filename:
                    print(f"    Found: '{pdf_filename}'")
            except:
                has_pdf_filename = False
                print(f"  Could not read message text")
            
            # Check 3: Does it have an error icon?
            error_icon_count = newest_message.locator('span[data-icon="error"]').count()
            has_error_icon = error_icon_count > 0
            print(f"  Error icon in newest message: {has_error_icon}")
            
            # Check 4: Compare with the message we saw before
            try:
                current_html = newest_message.inner_html()[:200]
                is_different = (current_html != initial_state['last_outgoing_html'])
                print(f"  Message is different from before: {is_different}")
            except:
                is_different = True  # Assume different if we can't compare
            
            # DECISION LOGIC
            print("\n" + "="*50)
            print("VERIFICATION RESULTS:")
            print("="*50)
            
            # If has error icon, definitely failed
            if has_error_icon:
                print("❌ FAILED: Newest message has ERROR icon")
                page.screenshot(path="message_has_error.png")
                return False, "Newest message has error icon - send failed"
            
            # If has document icon AND (has filename OR is different from before), SUCCESS
            if has_document_icon and (has_pdf_filename or is_different):
                print("✅ SUCCESS: Newest message has document icon")
                if has_pdf_filename:
                    print(f"   AND contains PDF filename: {pdf_filename}")
                if is_different:
                    print("   AND is different from previous last message")
                page.screenshot(path="verification_success.png")
                return True, "New PDF message verified with document icon"
            
            # If has document icon but same as before, might be old message
            if has_document_icon and not is_different:
                print("⚠️  WARNING: Has document icon but message looks same as before")
                print("   This might be the old message, not a new one")
                page.screenshot(path="possible_old_message.png")
                return False, "Document found but appears to be old message"
            
            # No document icon - definitely failed
            print("❌ FAILED: Newest message does NOT have document icon")
            print("   This means only text was sent, not the PDF")
            page.screenshot(path="no_document_icon.png")
            return False, "Newest message has no document icon - PDF not sent"
            
        except Exception as e:
            print(f"❌ Error examining newest message: {e}")
            page.screenshot(path="verification_error.png")
            return False, f"Verification error: {e}"

    def find_attach_button(self, page, max_wait=30):
        """Robust attach button finder"""
        print("\n" + "="*50)
        print("ATTACH BUTTON DETECTION")
        print("="*50)
        
        print("Ensuring compose area is loaded...")
        try:
            page.wait_for_selector('[data-testid="conversation-compose-box-input"]', timeout=10000, state="visible")
            time.sleep(2) 
            print("✓ Compose area loaded")
        except Exception as e:
            print(f"⚠️ Warning: Compose area wait failed: {e}")

        priority_selectors = [
            'div[aria-label="Attach"]',
            'button[aria-label="Attach"]',
            '[data-testid="clip"]',
            'span[data-icon="clip"]',
        ]
        
        start_time = time.time()
        attempt = 0
        
        while time.time() - start_time < max_wait:
            attempt += 1
            
            if attempt % 5 == 0:
                elapsed = int(time.time() - start_time)
                print(f"Still searching for attach button... ({elapsed}s / {max_wait}s)")
                page.screenshot(path=f"attach_search_{elapsed}s.png")
            
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
                                    aria_label = elem.get_attribute("aria-label") or ""
                                    print(f"✓ Found attach button: {selector}")
                                    print(f"  Aria-label: {aria_label}")
                                    return elem
                                    
                            except Exception as e:
                                continue
                                
                except Exception as e:
                    continue
            
            try:
                footer_buttons = page.locator('footer button')
                if footer_buttons.count() > 0:
                    for i in range(min(3, footer_buttons.count())):
                        btn = footer_buttons.nth(i)
                        if not btn.is_visible():
                            continue
                        
                        aria_label = btn.get_attribute("aria-label") or ""
                        title = btn.get_attribute("title") or ""
                        
                        if "voice" in aria_label.lower() or "voice" in title.lower():
                            continue
                        
                        if aria_label == "" and title == "":
                            has_clip = btn.locator('span[data-icon="clip"]').count() > 0
                            has_plus = btn.locator('span[data-icon="plus"]').count() > 0
                            
                            if has_clip or has_plus or i == 0:
                                print(f"✓ Found unlabeled footer button (likely attach): position {i}")
                                return btn
            except Exception as e:
                print(f"Footer button search error: {e}")
              
            if attempt > 5:
                try:
                    compose_clip = page.locator('footer span[data-icon="clip"]').first
                    if compose_clip.is_visible():
                        parent = compose_clip.evaluate_handle("el => el.closest('button, div[role=\"button\"]')")
                        if parent:
                            print("✓ Found attach button via clip icon")
                            return parent.as_element()
                except Exception as e:
                    pass
            
            time.sleep(1)
        
        print("\n❌ Could not find attach button after exhaustive search")
        page.screenshot(path="attach_button_not_found.png")
        return None

    def send_pdf_to_group(self, group_name, pdf_path, message="Sales report for today."):
        """Send PDF file to WhatsApp group with comprehensive verification"""
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

                # CRITICAL: Capture initial state BEFORE any attach actions
                initial_state = self.get_initial_message_state(page)

                print("\nOpening attach menu...")
                page.screenshot(path="before_attach.png")

                attach_button = self.find_attach_button(page, max_wait=30)
                
                if not attach_button:
                    raise Exception("Could not find attach button")

                print("Clicking attach button...")
                clicked = False
                
                try:
                    attach_button.click(timeout=5000)
                    clicked = True
                    print("✓ Clicked attach button")
                except Exception as e:
                    print(f"Regular click failed: {e}")
                
                if not clicked:
                    try:
                        attach_button.click(force=True, timeout=5000)
                        clicked = True
                        print("✓ Force clicked attach button")
                    except Exception as e:
                        print(f"Force click failed: {e}")

                if not clicked:
                    try:
                        attach_button.evaluate("el => el.click()")
                        clicked = True
                        print("✓ JavaScript clicked attach button")
                    except Exception as e:
                        raise Exception("All click methods failed")

                time.sleep(3)
                page.screenshot(path="attach_menu_opened.png")

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
                                print("✓ File uploaded via input element")
                                break
                        except Exception as e:
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
                        print("✓ File uploaded via file chooser")
                    except Exception as e:
                        print(f"File chooser failed: {e}")

                if not upload_success:
                    raise Exception("File upload failed")

                print("Waiting for file processing...")
                time.sleep(8)
                page.screenshot(path="after_file_upload.png")

                print("Waiting for file preview dialog...")
                time.sleep(5)

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

                typed_message = False
                message_selectors = [
                    'div[role="textbox"][data-tab="10"]',
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
                                time.sleep(0.3)
                                loc.first.type(message, delay=50)
                                typed_message = True
                                print(f"✓ Typed message using selector: {sel}")
                                break
                            except Exception as e:
                                print(f"Selector {sel} found but typing failed: {e}")
                                continue
                    except Exception:
                        continue

                if not typed_message:
                    try:
                        page.keyboard.type(message, delay=50)
                        typed_message = True
                        print("✓ Typed message using keyboard fallback.")
                    except Exception as e:
                        print(f"⚠️ Failed to type message: {e}")

                if not typed_message:
                    print("⚠️ Could not attach message. Proceeding without message.")

                time.sleep(2)

                print("Looking for send button...")
                send_selectors = [
                    '[data-testid="send"]',
                    'span[data-icon="send"]',
                    'button[aria-label="Send"]',
                    'div[role="button"][aria-label="Send"]',
                ]

                send_button = None
                for wait_attempt in range(120):
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

                # CRITICAL: Use the improved verification
                print("\n" + "="*60)
                print("STARTING VERIFICATION")
                print("="*60)
                
                verification_success, verification_msg = self.verify_message_sent(
                    page, 
                    pdf_filename, 
                    initial_state,
                    timeout=120
                )

                if not verification_success:
                    error_msg = f"❌ SEND FAILED: {verification_msg}"
                    print(f"\n{error_msg}")
                    raise Exception(error_msg)

                print("\n" + "="*60)
                print("✅ PDF SENT SUCCESSFULLY")
                print(f"   Result: {verification_msg}")
                print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print("="*60)

            except Exception as e:
                print(f"\n❌ ERROR: {e}")
                try:
                    page.screenshot(path="final_error.png")
                    with open("final_error.html", "w", encoding="utf-8") as f:
                        f.write(page.content())
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