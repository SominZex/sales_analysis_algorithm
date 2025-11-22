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
            # Notification prompts
            'div[data-testid="popup-controls-ok"]',
            'button:has-text("OK")',
            'button:has-text("Not now")',
            'div[role="button"]:has-text("Dismiss")',
            '[data-animate-modal-popup="true"]',
            # Close buttons
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
        """
        FIXED: Improved WhatsApp loading detection - focuses on success indicators, not loading indicators
        """
        print("Checking WhatsApp Web status...")
        start_time = time.time()
        
        # Take initial screenshot
        page.screenshot(path="whatsapp_initial.png")
        print("Initial screenshot saved: whatsapp_initial.png")

        # Track consecutive successful checks
        consecutive_success_checks = 0
        required_consecutive_checks = 3

        while time.time() - start_time < timeout:
            try:
                elapsed = int(time.time() - start_time)
                
                # Periodic screenshots for debugging
                if elapsed % 20 == 0 and elapsed > 0:
                    page.screenshot(path=f"debug_loading_{elapsed}s.png")
                    print(f"Debug screenshot saved: debug_loading_{elapsed}s.png")

                # Check for QR code FIRST
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

                # FIXED: Check for SUCCESS indicators (what we WANT to see), not loading indicators
                # This is the key fix - we look for elements that indicate WhatsApp is READY
                success_indicators = [
                    # Primary: Search box (most reliable indicator WhatsApp is ready)
                    '[data-testid="chat-list-search"]',
                    'div[contenteditable="true"][data-tab="3"]',
                    # Secondary: Chat list sidebar
                    '#side',
                    'div#pane-side',
                    'div[data-testid="chatlist-content"]',
                    # Tertiary: Any chat items visible
                    '[data-testid="cell-frame-container"]',
                ]

                found_success_indicator = False
                for selector in success_indicators:
                    try:
                        elements = page.locator(selector)
                        if elements.count() > 0:
                            elem = elements.first
                            # Use a short timeout to check visibility without blocking
                            try:
                                if elem.is_visible(timeout=1000):
                                    found_success_indicator = True
                                    print(f"✓ Success indicator found: {selector}")
                                    break
                            except:
                                continue
                    except:
                        continue

                # If we found a success indicator, increment counter
                if found_success_indicator:
                    consecutive_success_checks += 1
                    print(f"✓ WhatsApp appears loaded ({consecutive_success_checks}/{required_consecutive_checks} checks)")
                    
                    if consecutive_success_checks >= required_consecutive_checks:
                        print("✓ WhatsApp successfully loaded and stable!")
                        print("Waiting for full initialization...")
                        time.sleep(3)
                        
                        # Final verification - check for chat items
                        chat_items = page.locator('[data-testid="cell-frame-container"]')
                        chat_count = chat_items.count()
                        if chat_count > 0:
                            print(f"✓ Confirmed: Found {chat_count} chat(s)")
                        else:
                            print("✓ WhatsApp interface loaded (no chats visible yet)")
                        
                        return True
                    
                    # Wait a bit before next check
                    time.sleep(2)
                else:
                    # Reset counter if we don't find indicator
                    if consecutive_success_checks > 0:
                        print(f"⚠️ Lost success indicator, resetting counter")
                    consecutive_success_checks = 0
                    
                    # Print status periodically
                    if elapsed % 10 == 0:
                        print(f"Waiting for WhatsApp to load... ({elapsed}s / {timeout}s)")
                    
                    time.sleep(3)

            except Exception as e:
                consecutive_success_checks = 0
                elapsed = int(time.time() - start_time)
                if elapsed % 10 == 0:
                    print(f"Checking... ({elapsed}s) - Error: {str(e)[:100]}")
                time.sleep(3)

        # Timeout reached
        print("\n⚠️  Timeout reached!")
        page.screenshot(path="timeout_screenshot.png")

        # Save page HTML for debugging
        try:
            with open("timeout_page.html", "w", encoding="utf-8") as f:
                f.write(page.content())
            print("Page HTML saved to: timeout_page.html")
            
            # Additional debug info
            print("\nDEBUG INFO at timeout:")
            search_box_selector = '[data-testid="chat-list-search"]'
            side_selector = '#side'
            chat_items_selector = '[data-testid="cell-frame-container"]'
            print(f"  Search box count: {page.locator(search_box_selector).count()}")
            print(f"  Side panel count: {page.locator(side_selector).count()}")
            print(f"  Chat items count: {page.locator(chat_items_selector).count()}")
        except Exception as e:
            print(f"Error saving debug info: {e}")

        return False

    def find_and_open_chat(self, page, group_name):
        """Find and open a chat/group with enhanced search box detection"""
        print(f"\nSearching for: {group_name}")
        
        # First, dismiss any popups that might be blocking
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
            # New alternative selectors
            'div#side div[role="textbox"]',
            'div[data-testid="chatlist-header"] div[role="textbox"]',
            'header div[contenteditable="true"]',
            'div.copyable-text[contenteditable="true"]',
        ]

        # Wait for search box with extensive debugging
        search_box = None
        max_attempts = 40  # Reduced from 60 since we already verified WhatsApp is loaded
        
        for attempt in range(max_attempts):
            # Try to dismiss popups periodically
            if attempt > 0 and attempt % 10 == 0:
                self.dismiss_popups(page)
            
            # Debug: Save screenshot every 10 attempts
            if attempt % 10 == 0 and attempt > 0:
                print(f"Still waiting for search box... ({attempt}/{max_attempts})")
                page.screenshot(path=f"search_wait_{attempt}s.png")
            
            # Try all selectors
            for selector in search_selectors:
                try:
                    elements = page.locator(selector)
                    if elements.count() > 0:
                        elem = elements.first
                        if elem.is_visible(timeout=500):
                            # Additional check: ensure it's actually editable
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
            
            # Alternative: Try to find by placeholder text
            if attempt > 15:
                try:
                    placeholder_search = page.get_by_placeholder("Search or start new chat")
                    if placeholder_search.count() > 0:
                        search_box = placeholder_search.first
                        print("✓ Found search box by placeholder text")
                        break
                except:
                    pass
            
            # Alternative: Try to click on the search icon to activate search
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
            
            # Enhanced debugging
            try:
                with open("no_search_box_debug.html", "w", encoding="utf-8") as f:
                    f.write(page.content())
                print("Debug HTML saved: no_search_box_debug.html")
                
                # Try to find any textbox on the page
                print("\nDEBUG: Looking for ANY textbox on the page:")
                all_textboxes = page.locator('div[role="textbox"]')
                print(f"Found {all_textboxes.count()} textbox elements")
                for i in range(min(all_textboxes.count(), 5)):
                    try:
                        tb = all_textboxes.nth(i)
                        if tb.is_visible():
                            aria = tb.get_attribute("aria-label") or ""
                            title = tb.get_attribute("title") or ""
                            tab = tb.get_attribute("data-tab") or ""
                            print(f"  Textbox {i}: aria-label='{aria}', title='{title}', data-tab='{tab}'")
                    except:
                        pass
            except:
                pass
            
            raise Exception("Could not find search box after exhaustive search. Check no_search_box.png and no_search_box_debug.html")

        # Click and type in search box
        print("Attempting to activate search box...")
        for click_attempt in range(5):
            try:
                search_box.click()
                time.sleep(0.5)
                break
            except Exception as e:
                if click_attempt == 4:
                    raise Exception(f"Could not click search box after 5 attempts: {e}")
                print(f"Click attempt {click_attempt + 1} failed, retrying...")
                time.sleep(0.5)

        time.sleep(1)
        
        # Type the group name
        print(f"Typing '{group_name}' in search...")
        page.keyboard.type(group_name, delay=100)
        print(f"✓ Typed '{group_name}' in search")
        time.sleep(3)

        page.screenshot(path="search_results.png")
        print("Screenshot of search results saved as 'search_results.png'")

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

    def check_for_error_toast(self, page):
        """Check if WhatsApp shows an error message"""
        error_selectors = [
            'div[data-testid="toast-container"]',
            'div[role="alert"]',
            'span:has-text("Unsupported file")',
            'span:has-text("unsupported")',
            'span:has-text("failed")',
            'span:has-text("couldn\'t send")',
            'span:has-text("Error")',
        ]

        for selector in error_selectors:
            try:
                if page.locator(selector).count() > 0:
                    error_text = page.locator(selector).first.inner_text()
                    return True, error_text
            except:
                continue

        return False, None

    def get_initial_message_count(self, page):
        """Get the current count of messages in the chat to detect new messages later"""
        try:
            message_selectors = [
                'div[data-testid="msg-container"]',
                'div.message-in',
                'div.message-out',
                'div[role="row"]',
            ]
            
            for selector in message_selectors:
                count = page.locator(selector).count()
                if count > 0:
                    print(f"Initial message count: {count} (using {selector})")
                    return count, selector
            
            print("⚠️ Could not determine initial message count")
            return 0, None
        except Exception as e:
            print(f"⚠️ Error getting initial message count: {e}")
            return 0, None

    def verify_message_sent(self, page, pdf_filename, initial_count, message_selector, timeout=120):
        """
        Robustly verify that the message was actually sent to the group
        Returns: (success, reason)
        """
        print("\n" + "="*60)
        print("VERIFYING MESSAGE WAS SENT - COMPREHENSIVE CHECK")
        print("="*60)
        
        start_time = time.time()
        verification_checks = []
        last_check_count = 0
        stable_count = 0
        
        # Wait initial period for WhatsApp to process
        print("Waiting initial 8 seconds for WhatsApp to process...")
        time.sleep(8)
        
        while time.time() - start_time < timeout:
            elapsed = int(time.time() - start_time)
            current_checks = []
            
            # Check 1: Message count increased
            if message_selector:
                try:
                    current_count = page.locator(message_selector).count()
                    if current_count > initial_count:
                        check_msg = f"✓ Message count increased ({initial_count} → {current_count})"
                        current_checks.append(check_msg)
                        if check_msg not in verification_checks:
                            verification_checks.append(check_msg)
                            print(check_msg)
                except Exception as e:
                    pass
            
            try:
                doc_icons = page.locator('span[data-icon="document"]')
                if doc_icons.count() > 0:
                    check_msg = f"✓ Document icon found in chat"
                    current_checks.append(check_msg)
                    if check_msg not in verification_checks:
                        verification_checks.append(check_msg)
                        print(check_msg)
            except:
                pass
            
            try:
                if page.get_by_text(pdf_filename).count() > 0:
                    check_msg = f"✓ PDF filename '{pdf_filename}' found in chat"
                    current_checks.append(check_msg)
                    if check_msg not in verification_checks:
                        verification_checks.append(check_msg)
                        print(check_msg)
            except:
                pass
            
            # Check 4: Outgoing message indicators
            try:
                sent_selectors = [
                    'div.message-out',
                    'div[data-testid="msg-container"][class*="out"]',
                    'span[data-icon="msg-check"]',
                    'span[data-icon="msg-dblcheck"]',
                    'span[data-icon="msg-dblcheck-ack"]',
                ]
                
                for selector in sent_selectors:
                    count = page.locator(selector).count()
                    if count > 0:
                        check_msg = f"✓ Sent message indicator found: {selector}"
                        current_checks.append(check_msg)
                        if check_msg not in verification_checks:
                            verification_checks.append(check_msg)
                            print(check_msg)
                        break
            except:
                pass
            
            # Check 5: Dialog closed
            try:
                dialog_selectors = [
                    'div[role="dialog"]',
                    'div[data-testid="media-viewer"]',
                ]
                dialog_still_open = False
                for sel in dialog_selectors:
                    if page.locator(sel).count() > 0:
                        dialog_still_open = True
                        break
                
                if not dialog_still_open and elapsed > 8:
                    check_msg = "✓ Send dialog closed successfully"
                    current_checks.append(check_msg)
                    if check_msg not in verification_checks:
                        verification_checks.append(check_msg)
                        print(check_msg)
            except:
                pass
            
            # Check 6: Outgoing document message
            try:
                outgoing_docs = page.locator('div.message-out span[data-icon="document"]')
                if outgoing_docs.count() > 0:
                    check_msg = "✓ Outgoing document message found"
                    current_checks.append(check_msg)
                    if check_msg not in verification_checks:
                        verification_checks.append(check_msg)
                        print(check_msg)
            except:
                pass
            
            # Check for errors
            has_error, error_msg = self.check_for_error_toast(page)
            if has_error:
                print(f"\n❌ ERROR DETECTED: {error_msg}")
                return False, f"WhatsApp error: {error_msg}"
            
            # Stability check
            current_check_count = len(set(current_checks))
            if current_check_count == last_check_count and current_check_count >= 3:
                stable_count += 1
                if stable_count >= 3:
                    print(f"\n✓ VERIFICATION STABLE ({current_check_count} checks, stable for {stable_count} iterations)")
                    break
            else:
                stable_count = 0
            
            last_check_count = current_check_count
            
            # Success criteria
            unique_checks = len(set(verification_checks))
            if unique_checks >= 3 and elapsed > 10:
                if stable_count >= 2:
                    print(f"\n✓ VERIFICATION PASSED ({unique_checks} checks succeeded)")
                    time.sleep(3)
                    
                    page.screenshot(path="verification_success.png")
                    
                    # Final error check
                    time.sleep(2)
                    has_error, error_msg = self.check_for_error_toast(page)
                    if has_error:
                        print(f"\n❌ LATE ERROR DETECTED: {error_msg}")
                        return False, f"WhatsApp error (late): {error_msg}"
                    
                    return True, f"Verified with {unique_checks} checks"
            
            if elapsed % 10 == 0 and elapsed > 0:
                print(f"Verification in progress... ({elapsed}s / {timeout}s) - {unique_checks} checks passed, stable: {stable_count}")
                page.screenshot(path=f"verification_{elapsed}s.png")
            
            time.sleep(2)
        
        # Final assessment
        unique_checks = len(set(verification_checks))
        if unique_checks >= 3:
            print(f"\n✓ VERIFICATION PASSED at timeout ({unique_checks} checks succeeded)")
            page.screenshot(path="verification_success_timeout.png")
            return True, f"Verified with {unique_checks} checks (at timeout)"
        
        print(f"\n❌ VERIFICATION FAILED after {timeout}s")
        print(f"Only {unique_checks} checks passed (need 3+):")
        for check in set(verification_checks):
            print(f"  {check}")
        
        page.screenshot(path="verification_failed.png")
        
        try:
            with open("verification_failed.html", "w", encoding="utf-8") as f:
                f.write(page.content())
            print("Debug HTML saved to: verification_failed.html")
        except:
            pass
        
        return False, f"Insufficient verification ({unique_checks} checks passed, need 3+)"

    def find_attach_button(self, page, max_wait=30):
        """
        Robust attach button finder - handles both labeled and unlabeled attach buttons
        """
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
            
            # Try priority selectors first
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
                        
                        # Skip voice message button
                        if "voice" in aria_label.lower() or "voice" in title.lower():
                            continue
                        
                        # If it's an unlabeled button in footer, likely attach button
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
                        # Get the clickable parent
                        parent = compose_clip.evaluate_handle("el => el.closest('button, div[role=\"button\"]')")
                        if parent:
                            print("✓ Found attach button via clip icon")
                            return parent.as_element()
                except Exception as e:
                    pass
            
            time.sleep(1)
        
        # Failed to find
        print("\n❌ Could not find attach button after exhaustive search")
        page.screenshot(path="attach_button_not_found.png")
        
        try:
            with open("attach_debug.html", "w", encoding="utf-8") as f:
                f.write(page.content())
            print("Page HTML saved to: attach_debug.html")
            
            # Debug info
            print("\nDEBUG: Footer buttons found:")
            footer_btns = page.locator('footer button')
            for i in range(min(footer_btns.count(), 10)):
                try:
                    btn = footer_btns.nth(i)
                    if btn.is_visible():
                        aria = btn.get_attribute("aria-label") or "(empty)"
                        title = btn.get_attribute("title") or "(empty)"
                        has_clip = btn.locator('span[data-icon="clip"]').count() > 0
                        has_plus = btn.locator('span[data-icon="plus"]').count() > 0
                        print(f"  Button {i}: aria='{aria}', title='{title}', has_clip={has_clip}, has_plus={has_plus}")
                except:
                    pass
        except:
            pass
        
        return None

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
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)

        self.validate_pdf(pdf_path)
        pdf_filename = os.path.basename(pdf_path)

        with sync_playwright() as p:
            print("Launching browser with saved session...")

            # FIXED: Better browser configuration for headless mode
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
            
            # Set a more modern user agent
            page.set_extra_http_headers({
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            })

            try:
                print("Navigating to WhatsApp Web...")
                # FIXED: Use domcontentloaded instead of networkidle for faster loading
                page.goto('https://web.whatsapp.com', wait_until="domcontentloaded", timeout=60000)
                print("Page loaded, waiting for WhatsApp to initialize...")
                time.sleep(3)

                if not self.wait_for_whatsapp_load(page):
                    raise Exception("WhatsApp failed to load. Check debug screenshots and timeout_page.html")

                print("✓ WhatsApp loaded successfully!")
                print("Waiting for full initialization...")
                time.sleep(5)
                
                # Additional wait and popup dismissal
                self.dismiss_popups(page)
                time.sleep(2)

                self.find_and_open_chat(page, group_name)
                time.sleep(3)

                initial_count, message_selector = self.get_initial_message_count(page)

                print("Opening attach menu...")
                page.screenshot(path="before_attach.png")

                attach_button = self.find_attach_button(page, max_wait=30)
                
                if not attach_button:
                    raise Exception("Could not find attach button. Check attach_button_not_found.png and attach_debug.html")

                print("Attempting to click attach button...")
                clicked = False
                
                try:
                    attach_button.click(timeout=5000)
                    clicked = True
                    print("✓ Clicked attach button (regular click)")
                except Exception as e:
                    print(f"Regular click failed: {e}")
                
                if not clicked:
                    try:
                        attach_button.click(force=True, timeout=5000)
                        clicked = True
                        print("✓ Clicked attach button (force click)")
                    except Exception as e:
                        print(f"Force click failed: {e}")

                if not clicked:
                    try:
                        attach_button.evaluate("el => el.click()")
                        clicked = True
                        print("✓ Clicked attach button (JavaScript click)")
                    except Exception as e:
                        print(f"JavaScript click failed: {e}")
                
                if not clicked:
                    raise Exception("All click methods failed for attach button")

                time.sleep(3)
                page.screenshot(path="attach_menu_opened.png")

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
                    print("Trying file chooser method...")
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
                        print(f"File chooser method failed: {e}")

                if not upload_success:
                    raise Exception("All file upload methods failed")

                print("Waiting for WhatsApp to process the file...")
                time.sleep(7)
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

                # CRITICAL: Comprehensive verification with longer timeout
                print("\n" + "="*60)
                print("STARTING COMPREHENSIVE SEND VERIFICATION")
                print("="*60)
                
                verification_success, verification_reason = self.verify_message_sent(
                    page, 
                    pdf_filename, 
                    initial_count, 
                    message_selector,
                    timeout=120
                )

                if not verification_success:
                    error_msg = f"Message send verification FAILED: {verification_reason}"
                    print(f"\n❌ {error_msg}")
                    page.screenshot(path="send_verification_failed.png")
                    raise Exception(error_msg)

                # Final check for any delayed errors
                print("\nPerforming final error check...")
                time.sleep(3)
                has_error, error_msg = self.check_for_error_toast(page)
                if has_error:
                    raise Exception(f"Send failed with delayed error: {error_msg}")

                print("\n" + "="*60)
                print("✅ PDF SENT SUCCESSFULLY - VERIFIED")
                print(f"   Verification: {verification_reason}")
                print(f"   Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print("="*60)

            except Exception as e:
                print(f"\n❌ ERROR: {e}")
                print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                try:
                    page.screenshot(path="final_error.png")
                    print("Error screenshot: final_error.png")
                    with open("final_error.html", "w", encoding="utf-8") as f:
                        f.write(page.content())
                    print("Error HTML saved: final_error.html")
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

    print("="*60)
    print("WhatsApp PDF Sender - Automated Run")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    try:
        yesterday_pdf = get_yesterday_pdf(PDF_DIRECTORY)
        print(f"✓ Yesterday's PDF found: {yesterday_pdf}")

        sender = WhatsAppSender()
        sender.send_pdf_to_group(GROUP_NAME, yesterday_pdf, message=MESSAGE)
        
        print("\n" + "="*60)
        print("✅ SCRIPT COMPLETED SUCCESSFULLY")
        print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)

    except Exception as e:
        print(f"\n❌ SCRIPT FAILED: {str(e)}")
        print(f"Failed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        raise

if __name__ == "__main__":
    main()