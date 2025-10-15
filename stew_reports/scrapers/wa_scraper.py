#!/usr/bin/env python3
"""
WA Comprehensive Historical Harness Racing Scraper
Built for collecting complete historical data from racingwa.com.au
Optimized for full historical pulls with resume capability and progress tracking
"""

import os
import time
import logging
import requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Set, Optional, Dict, Tuple
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin
import argparse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

class WAComprehensiveScraper:
    """Comprehensive scraper for WA harness racing XLS files with full historical capability"""
    
    def __init__(self, base_dir: str = None, max_workers: int = 15, headless: bool = False):
        """Initialize the comprehensive WA scraper"""
        self.static_base = "https://static.p.racingwa.com.au/race-files"
        self.base_url = "https://racingwa.com.au"
        
        # Set up directory 
        if base_dir:
            self.base_dir = Path(base_dir)
        else:
            self.base_dir = Path("/Users/rorypearson/Desktop/harness_db/stew_reports/data")
        
        self.raw_dir = self.base_dir / "raw" / "wa"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        
        # Set up logging
        self._setup_logging()
        
        # Selenium setup
        self.driver = None
        self.headless = headless
        
        # Calendar URL for discovering actual meetings
        self.calendar_url = "https://racingwa.com.au/rwa/fullcalendar"
        
        # Track processed files and progress
        self.tracking_file = self.raw_dir / "comprehensive_progress.json"
        self.progress_data = self._load_progress()
        
        # Request session with connection pooling
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        
        # Threading configuration
        self.max_workers = max_workers
        
        # Statistics tracking
        self.stats = {
            'meetings_checked': 0,
            'meetings_found': 0,
            'files_downloaded': 0,
            'files_skipped': 0,
            'errors': 0
        }
    
    def _setup_logging(self):
        """Set up logging configuration"""
        log_dir = self.base_dir.parent / "scrapers" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"wa_comprehensive_{timestamp}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def _load_progress(self) -> Dict:
        """Load progress data from previous runs"""
        if self.tracking_file.exists():
            try:
                with open(self.tracking_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.warning(f"Could not load progress file: {e}")
        
        return {
            'downloaded_urls': set(),
            'checked_meetings': set(),
            'last_completed_year': None,
            'last_completed_month': None,
            'total_files_downloaded': 0
        }
    
    def _save_progress(self):
        """Save current progress to file"""
        try:
            # Convert sets to lists for JSON serialization
            save_data = {
                'downloaded_urls': list(self.progress_data['downloaded_urls']),
                'checked_meetings': list(self.progress_data['checked_meetings']),
                'last_completed_year': self.progress_data['last_completed_year'],
                'last_completed_month': self.progress_data['last_completed_month'],
                'total_files_downloaded': self.progress_data['total_files_downloaded']
            }
            
            with open(self.tracking_file, 'w') as f:
                json.dump(save_data, f, indent=2)
                
        except Exception as e:
            self.logger.error(f"Could not save progress: {e}")
    
    def _init_driver(self):
        """Initialize Selenium WebDriver"""
        if self.driver:
            return
        
        options = Options()
        if self.headless:
            options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        
        try:
            self.driver = webdriver.Chrome(options=options)
            self.logger.debug("WebDriver initialized")
        except Exception as e:
            self.logger.error(f"Failed to initialize WebDriver: {e}")
            raise
    
    def _close_driver(self):
        """Close the WebDriver"""
        if self.driver:
            self.driver.quit()
            self.driver = None
    
    def discover_harness_meetings_brute_force(self, start_date: datetime, end_date: datetime) -> List[str]:
        """Discover harness meetings using brute force URL generation
        Since calendar navigation isn't working, fall back to generating all possible meeting URLs
        """
        # WA harness racing venues and their codes
        wa_venues = {
            'GLOUCESTER PARK': ['GP'],
            'PINJARRA': ['PA'],
            'WANNEROO': ['WQ'],
            'NORTHAM': ['NM'], 
            'BUNBURY': ['BY'],
            'ALBANY': ['AY'],
            'NARROGIN': ['NG'],
            'COLLIE': ['CE'],
            'WAGIN': ['WA'],
            'CENTRAL WHEATBELT': ['ZO']
        }
        
        discovered_urls = []
        current_date = start_date
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            date_code = current_date.strftime('%d%m%y')
            
            for venue_name, venue_codes in wa_venues.items():
                for venue_code in venue_codes:
                    # Generate possible meeting codes
                    meeting_codes = [
                        f"{venue_code}{date_code}",
                        # f"{venue_code}{date_code[0:4]}{date_code[4:6]}"  # Alternative format
                    ]
                    
                    for meeting_code in meeting_codes:
                        # Construct potential meeting URLs
                        potential_urls = [
                            f"https://racingwa.com.au/rwa/meetings/harness/{date_str}/{venue_name.replace(' ', '%20')}/{meeting_code}",
                            # f"https://racingwa.com.au/rwa/meetings/harness/{date_str}/{venue_name.replace(' ', '%20')}/{meeting_code}/fullview"
                        ]
                        
                        for url in potential_urls:
                            try:
                                # Quick check if meeting exists (without downloading files)
                                response = self.session.head(url, timeout=10)
                                if response.status_code == 200:
                                    discovered_urls.append(url)
                                    # self.logger.info(f"Found valid meeting: {url}")
                                    break  # Found valid URL, no need to try other formats
                            except Exception as e:
                                self.logger.debug(f"Failed to check URL {url}: {e}")
                                continue
            
            current_date += timedelta(days=1)
        
        self.logger.info(f"Brute force discovered {len(discovered_urls)} harness meetings")
        return discovered_urls

    def discover_harness_meetings(self, start_date: datetime, end_date: datetime) -> List[str]:
        """Discover harness racing meetings from Racing WA calendar
        
        Returns:
            List of harness meeting URLs
        """
        harness_urls = []
        
        # Try calendar approach first
        try:
            self._init_driver()
            self.logger.info(f"Attempting calendar-based discovery for {start_date.date()} to {end_date.date()}")
            
            # Load the calendar page
            self.driver.get("https://racingwa.com.au/rwa/fullcalendar")
            time.sleep(5)
            
            # Navigate backwards through calendar months until we reach start_date
            months_navigated = 0
            max_months = 50  # Safety limit (going back ~4 years max)
            
            while months_navigated < max_months:
                # Get current calendar date
                current_date = self._get_calendar_date()
                if current_date and current_date < start_date:
                    self.logger.info(f"Reached target start date: {current_date}")
                    break
                
                # Extract harness meetings from current month view
                month_urls = self._extract_harness_meeting_urls()
                if month_urls:
                    harness_urls.extend(month_urls)
                    self.logger.info(f"Found {len(month_urls)} harness meetings in current month")
                
                # Navigate back one month using the correct button
                if not self._navigate_back_one_month():
                    self.logger.warning("Could not navigate back further")
                    break
                
                # Wait for calendar to update
                time.sleep(2)
                months_navigated += 1
                
        except Exception as e:
            self.logger.error(f"Calendar approach failed: {e}")
            self.logger.info("Falling back to brute force method")
            if hasattr(self, 'driver') and self.driver:
                self._close_driver()
            return self.discover_harness_meetings_brute_force(start_date, end_date)
            
        # Remove duplicates and filter by date range
        unique_urls = list(set(harness_urls))
        filtered_urls = []
        
        for url in unique_urls:
            try:
                # Extract date from URL: /rwa/meetings/harness/YYYY-MM-DD/VENUE/CODE
                if "/harness/" in url:
                    parts = url.split("/harness/")
                    if len(parts) > 1:
                        date_str = parts[1].split("/")[0]
                        meeting_date = datetime.strptime(date_str, "%Y-%m-%d")
                        if start_date <= meeting_date <= end_date:
                            filtered_urls.append(url)
                        else:
                            self.logger.debug(f"URL date {meeting_date.date()} outside range {start_date.date()}-{end_date.date()}: {url}")
                    else:
                        self.logger.debug(f"Could not extract date from harness URL: {url}")
                else:
                    self.logger.debug(f"URL not a harness meeting: {url}")
            except Exception as e:
                self.logger.debug(f"Error parsing URL {url}: {e}")
                pass
        
        self.logger.info(f"Total unique harness meetings found in date range: {len(filtered_urls)}")
        return filtered_urls
    
    def _get_calendar_date(self) -> Optional[datetime]:
        """Get the current date displayed in the calendar from the title"""
        try:
            # Find the p element with data-test-name='calendar-title-date'
            date_element = self.driver.find_element(By.CSS_SELECTOR, "p[data-test-name='calendar-title-date']")
            date_text = date_element.text
            self.logger.debug(f"Calendar date text: {date_text}")
            
            # Parse the date text - it might be in format like "Monday 2 September 2024" or "01 Sep 2025"
            # Try different date formats
            date_formats = [
                "%A %d %B %Y",    # Monday 2 September 2024
                "%d %b %Y",       # 01 Sep 2025
                "%d %B %Y",       # 2 September 2024  
                "%B %d, %Y",      # September 2, 2024
                "%Y-%m-%d",       # 2024-09-02
            ]
            
            for fmt in date_formats:
                try:
                    return datetime.strptime(date_text, fmt)
                except:
                    continue
            
            # If we can't parse the exact date, try to extract just the date parts
            import re
            # Look for patterns like "2 September 2024" or "September 2024"
            match = re.search(r'(\d{1,2})\s+(\w+)\s+(\d{4})', date_text)
            if match:
                day, month_name, year = match.groups()
                month_map = {
                    'january': 1, 'february': 2, 'march': 3, 'april': 4,
                    'may': 5, 'june': 6, 'july': 7, 'august': 8,
                    'september': 9, 'october': 10, 'november': 11, 'december': 12
                }
                month = month_map.get(month_name.lower())
                if month:
                    return datetime(int(year), month, int(day))
            
            self.logger.warning(f"Could not parse calendar date: {date_text}")
            return None
            
        except Exception as e:
            self.logger.warning(f"Could not get calendar date: {e}")
            return None
    
    def _navigate_back_one_month(self) -> bool:
        """Navigate back one week using the navigation button"""
        try:
            # Use the correct data-test-id selector
            back_button = self.driver.find_element(By.XPATH, '//button[@data-test-id="calendar-back-one-week"]')
            back_button.click()
            self.logger.debug("Successfully clicked back button")
            return True
            
        except Exception as e:
            self.logger.debug(f"Failed to click back button: {e}")
            # Try alternative selectors as fallback
            selectors = [
                (By.ID, "calendar-back-one-week"),
                (By.CSS_SELECTOR, "#calendar-back-one-week"),
                (By.XPATH, "//button[@id='calendar-back-one-week']"),
            ]
            
            # Try main page context first
            for by, selector in selectors:
                try:
                    back_button = self.driver.find_element(by, selector)
                    if back_button:
                        self.logger.info(f"Found button in main page: {selector}")
                        back_button.click()
                        time.sleep(3)  # Wait for calendar to update
                        return True
                except:
                    continue
            
            # If not found in main page, try iframe
            iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
            if iframes:
                self.logger.info("Switching to iframe to find navigation button")
                self.driver.switch_to.frame(iframes[0])
                
                for by, selector in selectors:
                    try:
                        back_button = self.driver.find_element(by, selector)
                        if back_button:
                            self.logger.info(f"Found button in iframe: {selector}")
                            back_button.click()
                            # Switch back to main content
                            self.driver.switch_to.default_content()
                            time.sleep(3)  # Wait for calendar to update
                            return True
                    except:
                        continue
                
                # Switch back to main content if button not found
                self.driver.switch_to.default_content()
            
            # Try EVERYTHING to click this button via JavaScript
            try:
                # Method 1: Direct getElementById and multiple click methods
                js_code1 = """
                var button = document.getElementById('calendar-back-one-week');
                if (button) {
                    console.log('Found button via getElementById');
                    button.click();
                    button.dispatchEvent(new MouseEvent('click', {bubbles: true}));
                    button.dispatchEvent(new Event('click', {bubbles: true}));
                    return 'clicked_main';
                }
                return null;
                """
                result = self.driver.execute_script(js_code1)
                if result:
                    self.logger.info(f"Clicked button via JavaScript method 1: {result}")
                    time.sleep(3)
                    return True
                    
                # Method 2: QuerySelector with various selectors
                js_code2 = """
                var selectors = [
                    '#calendar-back-one-week',
                    '[id="calendar-back-one-week"]',
                    'button#calendar-back-one-week',
                    '*[id="calendar-back-one-week"]'
                ];
                for (var i = 0; i < selectors.length; i++) {
                    var button = document.querySelector(selectors[i]);
                    if (button) {
                        console.log('Found button with selector: ' + selectors[i]);
                        button.click();
                        return 'clicked_querySelector';
                    }
                }
                return null;
                """
                result = self.driver.execute_script(js_code2)
                if result:
                    self.logger.info(f"Clicked button via JavaScript method 2: {result}")
                    time.sleep(3)
                    return True
                
                # Method 3: Search all elements in DOM
                js_code3 = """
                var allElements = document.getElementsByTagName('*');
                for (var i = 0; i < allElements.length; i++) {
                    if (allElements[i].id === 'calendar-back-one-week') {
                        console.log('Found button in all elements search');
                        allElements[i].click();
                        allElements[i].dispatchEvent(new MouseEvent('click', {bubbles: true, cancelable: true, view: window}));
                        return 'clicked_all_elements';
                    }
                }
                return null;
                """
                result = self.driver.execute_script(js_code3)
                if result:
                    self.logger.info(f"Clicked button via JavaScript method 3: {result}")
                    time.sleep(3)
                    return True
                
                # Method 4: Check shadow DOM
                js_code4 = """
                function findInShadowRoot(root) {
                    var button = root.getElementById ? root.getElementById('calendar-back-one-week') : null;
                    if (button) return button;
                    
                    var elements = root.querySelectorAll('*');
                    for (var i = 0; i < elements.length; i++) {
                        if (elements[i].shadowRoot) {
                            var found = findInShadowRoot(elements[i].shadowRoot);
                            if (found) return found;
                        }
                    }
                    return null;
                }
                var button = findInShadowRoot(document);
                if (button) {
                    console.log('Found button in shadow DOM');
                    button.click();
                    return 'clicked_shadow';
                }
                return null;
                """
                result = self.driver.execute_script(js_code4)
                if result:
                    self.logger.info(f"Clicked button via JavaScript method 4: {result}")
                    time.sleep(3)
                    return True
                
                # Method 5: jQuery if available
                js_code5 = """
                if (typeof jQuery !== 'undefined') {
                    var button = jQuery('#calendar-back-one-week');
                    if (button.length > 0) {
                        console.log('Found button via jQuery');
                        button.trigger('click');
                        button[0].click();
                        return 'clicked_jquery';
                    }
                }
                return null;
                """
                result = self.driver.execute_script(js_code5)
                if result:
                    self.logger.info(f"Clicked button via JavaScript method 5: {result}")
                    time.sleep(3)
                    return True
                
                # Method 6: Search iframes more aggressively
                js_code6 = """
                function searchIframes() {
                    var iframes = document.getElementsByTagName('iframe');
                    for (var i = 0; i < iframes.length; i++) {
                        try {
                            var iframeDoc = iframes[i].contentDocument || iframes[i].contentWindow.document;
                            var button = iframeDoc.getElementById('calendar-back-one-week');
                            if (!button) {
                                button = iframeDoc.querySelector('#calendar-back-one-week');
                            }
                            if (button) {
                                console.log('Found button in iframe ' + i);
                                button.click();
                                button.dispatchEvent(new MouseEvent('click', {bubbles: true}));
                                return 'clicked_iframe';
                            }
                        } catch(e) {
                            console.log('Cannot access iframe ' + i + ': ' + e);
                        }
                    }
                    return null;
                }
                return searchIframes();
                """
                result = self.driver.execute_script(js_code6)
                if result:
                    self.logger.info(f"Clicked button via JavaScript method 6: {result}")
                    time.sleep(3)
                    return True
                
                # Method 7: Force visibility and click
                js_code7 = """
                var button = document.getElementById('calendar-back-one-week');
                if (button) {
                    console.log('Forcing button visibility and clicking');
                    button.style.display = 'block';
                    button.style.visibility = 'visible';
                    button.style.opacity = '1';
                    button.removeAttribute('disabled');
                    button.click();
                    
                    // Try multiple click events
                    var clickEvent = new MouseEvent('click', {
                        view: window,
                        bubbles: true,
                        cancelable: true
                    });
                    button.dispatchEvent(clickEvent);
                    
                    // Try touch event
                    if (window.TouchEvent) {
                        button.dispatchEvent(new TouchEvent('touchstart'));
                        button.dispatchEvent(new TouchEvent('touchend'));
                    }
                    
                    return 'force_clicked';
                }
                return null;
                """
                result = self.driver.execute_script(js_code7)
                if result:
                    self.logger.info(f"Clicked button via JavaScript method 7: {result}")
                    time.sleep(3)
                    return True
                
                # Method 8: Debug - log what we find
                js_debug = """
                console.log('Searching for button...');
                var button = document.getElementById('calendar-back-one-week');
                if (button) {
                    console.log('Button found!');
                    console.log('Button HTML:', button.outerHTML);
                    console.log('Button visible:', button.offsetParent !== null);
                    console.log('Button enabled:', !button.disabled);
                    console.log('Button display:', window.getComputedStyle(button).display);
                    console.log('Attempting click...');
                    button.click();
                    return 'debug_click';
                } else {
                    console.log('Button not found in main document');
                    // List all elements with IDs
                    var withIds = document.querySelectorAll('[id]');
                    console.log('Elements with IDs:', withIds.length);
                    for (var i = 0; i < Math.min(10, withIds.length); i++) {
                        console.log('  - ' + withIds[i].id);
                    }
                }
                return null;
                """
                result = self.driver.execute_script(js_debug)
                if result:
                    self.logger.info(f"Debug click result: {result}")
                    time.sleep(3)
                    return True
                    
            except Exception as e:
                self.logger.error(f"All JavaScript click attempts failed: {e}")
            
            self.logger.error("Could not click calendar navigation button despite all attempts")
            return False
                
        except Exception as e:
            self.logger.error(f"Error navigating calendar: {e}")
            return False
    
    def _extract_harness_meeting_urls(self) -> List[str]:
        """Extract harness meeting URLs from the current calendar view"""
        harness_urls = []
        
        try:
            # Wait for any dynamic content to load
            time.sleep(2)
            
            # Try to extract from main page first
            links = self.driver.find_elements(By.XPATH, "//a[contains(@href, '/rwa/meetings/harness/')]")
            
            for link in links:
                try:
                    href = link.get_attribute("href")
                    if href and "/harness/" in href:
                        # Make sure it's a full URL
                        if href.startswith("/"):
                            href = self.base_url + href
                        if href not in harness_urls:
                            harness_urls.append(href)
                except:
                    continue
            
            # If no URLs found, try iframe
            if not harness_urls:
                iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
                if iframes:
                    self.driver.switch_to.frame(iframes[0])
                    
                    links = self.driver.find_elements(By.XPATH, "//a[contains(@href, '/rwa/meetings/harness/')]")
                    for link in links:
                        try:
                            href = link.get_attribute("href")
                            if href and "/harness/" in href:
                                if href.startswith("/"):
                                    href = self.base_url + href
                                if href not in harness_urls:
                                    harness_urls.append(href)
                        except:
                            continue
                    
                    # Switch back to main content
                    self.driver.switch_to.default_content()
            
            # If no links found with XPath, try CSS selector
            if not harness_urls:
                links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='/rwa/meetings/harness/']")
                for link in links:
                    try:
                        href = link.get_attribute("href")
                        if href and "/harness/" in href:
                            if href.startswith("/"):
                                href = self.base_url + href
                            if href not in harness_urls:
                                harness_urls.append(href)
                    except:
                        continue
            
            # As a fallback, search the page source
            if not harness_urls:
                page_source = self.driver.page_source
                import re
                # Find all harness meeting URLs in the page
                harness_matches = re.findall(r'href=["\']([^"\']*\/rwa\/meetings\/harness\/[^"\']*)["\']', page_source)
                for match in harness_matches:
                    if match.startswith("/"):
                        match = self.base_url + match
                    if match not in harness_urls:
                        harness_urls.append(match)
            
            return harness_urls
            
        except Exception as e:
            self.logger.error(f"Error extracting harness URLs: {e}")
            return []
    
    def _extract_harness_meetings_from_month(self, month_date: datetime) -> List[str]:
        """Extract harness meeting URLs from a specific month in the calendar"""
        try:
            # Navigate to calendar page first
            if not hasattr(self, '_calendar_loaded'):
                self.driver.get(self.calendar_url)
                time.sleep(5)  # Wait for initial load
                self._calendar_loaded = True
            
            # Navigate to specific month/year using JavaScript or UI controls
            self.logger.info(f"Navigating to {month_date.strftime('%B %Y')} in calendar")
            
            # Try to find and click month/year navigation
            try:
                # Look for month/year selectors or navigation buttons
                year_element = self.driver.find_element(By.XPATH, f"//select[@name='year']//option[@value='{month_date.year}']")
                year_element.click()
                time.sleep(1)
                
                month_element = self.driver.find_element(By.XPATH, f"//select[@name='month']//option[@value='{month_date.month}']")
                month_element.click()
                time.sleep(2)
            except:
                # Alternative approach: use URL parameters
                calendar_url = f"{self.calendar_url}?date={month_date.strftime('%Y-%m-01')}"
                self.driver.get(calendar_url)
                time.sleep(3)
            
            # Wait for calendar to load
            time.sleep(3)
            
            # Look for harness racing meeting links
            harness_urls = []
            
            # Try different selectors for meeting links
            selectors = [
                "a[href*='/rwa/meetings/harness/']",  # Direct harness meeting links
                "a[href*='/meetings/harness/']",      # Alternative pattern
                ".harness-meeting a",                 # Class-based selector
                "[data-race-type='harness'] a",       # Data attribute selector
            ]
            
            for selector in selectors:
                try:
                    links = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for link in links:
                        href = link.get_attribute("href")
                        if href and "/harness/" in href and href not in harness_urls:
                            harness_urls.append(href)
                except:
                    continue
            
            # If no direct links found, look in page source
            if not harness_urls:
                page_source = self.driver.page_source
                # Extract harness meeting URLs from page source
                harness_matches = re.findall(r'href="([^"]*\/rwa\/meetings\/harness\/[^"]*)"', page_source)
                harness_urls.extend(harness_matches)
                
                # Also try without quotes
                if not harness_matches:
                    harness_matches2 = re.findall(r'\/rwa\/meetings\/harness\/[^"\s<>]*', page_source)
                    harness_urls.extend(harness_matches2)
            
            # Convert relative URLs to absolute
            absolute_urls = []
            for url in harness_urls:
                if url.startswith("/"):
                    url = self.base_url + url
                absolute_urls.append(url)
            
            # Debug logging
            if absolute_urls:
                self.logger.debug(f"Found URLs for {month_date.strftime('%B %Y')}: {absolute_urls[:2]}")
            
            return list(set(absolute_urls))  # Remove duplicates
            
        except Exception as e:
            self.logger.debug(f"Error extracting meetings from {month_date.strftime('%B %Y')}: {e}")
            return []
    
    def _filter_urls_by_date_range(self, urls: List[str], start_date: datetime, end_date: datetime) -> List[str]:
        """Filter meeting URLs to only include those within the date range"""
        filtered_urls = []
        
        for url in urls:
            try:
                # Extract date from URL pattern: /rwa/meetings/harness/YYYY-MM-DD/VENUE/CODE
                if "/harness/" in url:
                    parts = url.split("/harness/")
                    if len(parts) > 1:
                        date_part = parts[1].split("/")[0]  # Get YYYY-MM-DD part
                        meeting_date = datetime.strptime(date_part, "%Y-%m-%d")
                        
                        if start_date <= meeting_date <= end_date:
                            filtered_urls.append(url)
            except:
                # If we can't parse the date, include it anyway
                filtered_urls.append(url)
        
        return filtered_urls
    
    def check_and_download_meeting(self, meeting_url: str) -> Optional[str]:
        """Check for XLS file by visiting the meeting page and looking for download links"""
        
        # Skip if already checked
        if meeting_url in self.progress_data['checked_meetings']:
            return None
        
        try:
            # Extract meeting info from URL
            # URL format: /rwa/meetings/harness/YYYY-MM-DD/VENUE/CODE
            url_parts = meeting_url.split("/")
            if len(url_parts) >= 6:
                date_str = url_parts[-3]  # YYYY-MM-DD
                venue_name = url_parts[-2].replace('%20', ' ')  # VENUE
                meeting_code = url_parts[-1]  # CODE
            else:
                # Fallback parsing
                date_str = "unknown"
                venue_name = "unknown"
                meeting_code = "unknown"
            
            # Quick check if meeting page exists
            try:
                head_response = self.session.head(meeting_url, timeout=10)
                if head_response.status_code != 200:
                    self.progress_data['checked_meetings'].add(meeting_url)
                    return None
            except:
                self.progress_data['checked_meetings'].add(meeting_url)
                return None
            
            # Initialize driver if needed
            self._init_driver()
            
            # Load the meeting page
            self.driver.get(meeting_url)
            time.sleep(2)  # Wait for page to load
            
            # Look for any links containing XLS files
            all_links = self.driver.find_elements(By.TAG_NAME, "a")
            xls_urls = []
            
            for link in all_links:
                try:
                    href = link.get_attribute("href")
                    if href and '.xls' in href.lower() and 'static.p.racingwa.com.au' in href:
                        xls_urls.append(href)
                except:
                    continue
            
            # If no direct links found, try looking in page source for static URLs
            if not xls_urls:
                page_source = self.driver.page_source
                # Look for static.p.racingwa.com.au URLs with .xls
                static_matches = re.findall(r'https://static\.p\.racingwa\.com\.au[^"\']*\.xls[^"\']*', page_source, re.IGNORECASE)
                xls_urls.extend(static_matches)
            
            if xls_urls:
                # Use the first XLS URL found
                xls_url = xls_urls[0]
                
                # Check if already downloaded
                if xls_url in self.progress_data['downloaded_urls']:
                    self.progress_data['checked_meetings'].add(meeting_url)
                    return None
                
                # Download the file
                result = self._download_file(xls_url, date_str, venue_name, meeting_code)
                if result:
                    self.progress_data['downloaded_urls'].add(xls_url)
                    self.progress_data['total_files_downloaded'] += 1
                
                # Mark as checked
                self.progress_data['checked_meetings'].add(meeting_url)
                return result
            
            # Mark as checked even if no files found
            self.progress_data['checked_meetings'].add(meeting_url)
            
        except Exception as e:
            self.logger.debug(f"Error checking {meeting_url}: {e}")
            self.stats['errors'] += 1
            # Still mark as checked to avoid retrying
            self.progress_data['checked_meetings'].add(meeting_url)
        
        return None
    
    def _download_file(self, xls_url: str, date_str: str, venue_name: str, meeting_code: str) -> Optional[str]:
        """Download XLS file"""
        try:
            # Create filename
            date_clean = date_str.replace('-', '')
            venue_clean = venue_name.replace(' ', '_').upper()
            filename = f"WA_{date_clean}_{venue_clean}_H.xls"
            
            if '.xlsx' in xls_url.lower():
                filename = filename.replace('.xls', '.xlsx')
            
            filepath = self.raw_dir / filename
            
            # Check if file already exists
            if filepath.exists():
                self.stats['files_skipped'] += 1
                return filename
            
            # Download file
            response = self.session.get(xls_url, timeout=30)
            response.raise_for_status()
            
            # Verify it's actually an Excel file (minimum size check)
            if len(response.content) < 1000:
                self.logger.debug(f"File too small, likely not Excel: {xls_url}")
                return None
            
            # Save file
            with open(filepath, 'wb') as f:
                f.write(response.content)
            
            file_size_kb = len(response.content) / 1024
            self.logger.info(f"✓ Downloaded: {filename} ({file_size_kb:.1f} KB) - {meeting_code}")
            
            self.stats['files_downloaded'] += 1
            return filename
            
        except Exception as e:
            self.logger.debug(f"Download failed for {xls_url}: {e}")
            self.stats['errors'] += 1
            return None
    
    def scrape_specific_date(self, target_date: str) -> Dict:
        """Scrape XLS files for a specific date

        Args:
            target_date: Date in YYYY-MM-DD format

        Returns:
            Dictionary with scraping results
        """
        from datetime import datetime
        date_obj = datetime.strptime(target_date, '%Y-%m-%d')

        self.logger.info(f"Scraping WA data for specific date: {target_date}")

        results = {
            'success': True,
            'total_found': 0,
            'downloads': {'successful': 0, 'failed': 0},
            'files': []
        }

        # Discover meetings for this specific date
        meeting_urls = self.discover_harness_meetings_brute_force(date_obj, date_obj)

        if not meeting_urls:
            self.logger.info(f"No meetings found for {target_date}")
            return results

        # Process each meeting
        for meeting_url in meeting_urls:
            try:
                filename = self.check_and_download_meeting(meeting_url)
                if filename:
                    results['total_found'] += 1
                    results['downloads']['successful'] += 1
                    results['files'].append({
                        'filename': filename,
                        'url': meeting_url,
                        'date': target_date,
                        'success': True
                    })
            except Exception as e:
                self.logger.error(f"Error processing meeting {meeting_url}: {e}")
                results['downloads']['failed'] += 1

        self.logger.info(f"WA scraping for {target_date} complete - {results['downloads']['successful']} successful")
        return results

    def scrape_historical_range(self, start_date: datetime, end_date: datetime, save_interval: int = 20):
        """Main method to scrape historical XLS files using calendar-based discovery"""
        try:
            self.logger.info("="*70)
            self.logger.info(f"WA CALENDAR-BASED HISTORICAL SCRAPER")
            self.logger.info(f"Date range: {start_date.date()} to {end_date.date()}")
            self.logger.info(f"Calendar URL: {self.calendar_url}")
            self.logger.info("="*70)
            
            # Discover actual harness meetings from Racing WA calendar
            self.logger.info("Phase 1: Discovering harness meetings from calendar...")
            harness_meeting_urls = self.discover_harness_meetings(start_date, end_date)
            
            if not harness_meeting_urls:
                self.logger.warning("No harness meetings found in calendar!")
                return
            
            self.logger.info(f"Found {len(harness_meeting_urls)} harness meetings from calendar")
            self.logger.info(f"Already checked: {len(self.progress_data['checked_meetings']):,}")
            self.logger.info(f"Previously downloaded: {self.progress_data['total_files_downloaded']}")
            
            # Filter out already checked meetings
            unchecked_meetings = [
                url for url in harness_meeting_urls 
                if url not in self.progress_data['checked_meetings']
            ]
            
            self.logger.info(f"New meetings to check: {len(unchecked_meetings):,}")
            
            if not unchecked_meetings:
                self.logger.info("All discovered meetings already checked!")
                return
            
            # Process meetings sequentially
            self.logger.info("Phase 2: Processing meetings and downloading XLS files...")
            start_time = time.time()
            processed_count = 0
            
            for meeting_url in unchecked_meetings:
                try:
                    result = self.check_and_download_meeting(meeting_url)
                    if result:
                        self.stats['meetings_found'] += 1
                    
                    self.stats['meetings_checked'] += 1
                    processed_count += 1
                    
                    # Progress updates and periodic saves
                    if processed_count % save_interval == 0:
                        elapsed = time.time() - start_time
                        rate = processed_count / elapsed if elapsed > 0 else 0
                        eta_seconds = (len(unchecked_meetings) - processed_count) / rate if rate > 0 else 0
                        eta_minutes = eta_seconds / 60
                        
                        self.logger.info(f"Progress: {processed_count:,}/{len(unchecked_meetings):,} " +
                                       f"({processed_count/len(unchecked_meetings)*100:.1f}%) - " +
                                       f"Rate: {rate:.1f}/sec - ETA: {eta_minutes:.1f}min")
                        self.logger.info(f"Found: {self.stats['meetings_found']}, " +
                                       f"Downloaded: {self.stats['files_downloaded']}, " + 
                                       f"Errors: {self.stats['errors']}")
                        
                        # Save progress periodically
                        self._save_progress()
                        
                except Exception as e:
                    self.logger.debug(f"Error processing {meeting_url}: {e}")
                    self.stats['errors'] += 1
            
            # Final save and summary
            self._save_progress()
            
            # Update completion markers
            self.progress_data['last_completed_year'] = end_date.year
            self.progress_data['last_completed_month'] = end_date.month
            self._save_progress()
            
            # Show final statistics
            elapsed = time.time() - start_time
            self._show_final_summary(elapsed)
            
        except Exception as e:
            self.logger.error(f"Scraping failed: {e}")
            self._save_progress()  # Save progress even on failure
        finally:
            # Clean up driver
            self._close_driver()
    
    def _show_final_summary(self, elapsed_seconds: float):
        """Show comprehensive final summary"""
        self.logger.info("="*70)
        self.logger.info("SCRAPING COMPLETE!")
        self.logger.info("="*70)
        
        # Statistics
        self.logger.info(f"Time elapsed: {elapsed_seconds/60:.1f} minutes")
        self.logger.info(f"Meetings checked: {self.stats['meetings_checked']:,}")
        self.logger.info(f"Meetings with files: {self.stats['meetings_found']:,}")
        self.logger.info(f"Files downloaded this run: {self.stats['files_downloaded']:,}")
        self.logger.info(f"Files skipped (existing): {self.stats['files_skipped']:,}")
        self.logger.info(f"Errors encountered: {self.stats['errors']:,}")
        self.logger.info(f"Total files downloaded ever: {self.progress_data['total_files_downloaded']:,}")
        
        # File system stats
        try:
            xls_files = list(self.raw_dir.glob("*.xls*"))
            total_size_mb = sum(f.stat().st_size for f in xls_files) / (1024 * 1024)
            
            self.logger.info("="*40)
            self.logger.info(f"Files in raw/wa directory: {len(xls_files):,}")
            self.logger.info(f"Total disk usage: {total_size_mb:.1f} MB")
            
            # Monthly breakdown
            monthly_counts = {}
            for f in xls_files:
                try:
                    # Extract date from filename: WA_YYYYMMDD_VENUE_H.xls
                    parts = f.stem.split('_')
                    if len(parts) >= 2:
                        date_part = parts[1]  # YYYYMMDD
                        if len(date_part) == 8:
                            month_key = f"{date_part[:4]}-{date_part[4:6]}"
                            monthly_counts[month_key] = monthly_counts.get(month_key, 0) + 1
                except:
                    continue
            
            if monthly_counts:
                self.logger.info("\nFiles by month:")
                for month in sorted(monthly_counts.keys())[-12:]:  # Show last 12 months
                    self.logger.info(f"  {month}: {monthly_counts[month]} files")
            
            # Show newest files
            if xls_files:
                newest_files = sorted(xls_files, key=lambda x: x.stat().st_mtime)[-10:]
                self.logger.info("\nNewest downloaded files:")
                for f in newest_files:
                    size_kb = f.stat().st_size / 1024
                    mtime = datetime.fromtimestamp(f.stat().st_mtime).strftime('%Y-%m-%d %H:%M')
                    self.logger.info(f"  - {f.name} ({size_kb:.1f} KB) - {mtime}")
                    
        except Exception as e:
            self.logger.debug(f"Error showing file stats: {e}")
        
        self.logger.info("="*70)


def main():
    """Run the comprehensive WA scraper"""
    parser = argparse.ArgumentParser(
        description='Comprehensive WA harness racing XLS scraper for historical data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Default: scrape from June 1, 2024 to present (headless mode)
  python wa_scraper.py
  
  # Discover meetings for August 2024 (no downloads)
  python wa_scraper.py --discover --start-date 2024-08-01 --end-date 2024-08-31
  
  # Download XLS files for August 2024
  python wa_scraper.py --start-date 2024-08-01 --end-date 2024-08-31
  
  # Full historical scrape from June 2024 to present (high performance)
  python wa_scraper.py --workers 20
  
  # Show browser window (disable headless mode)
  python wa_scraper.py --no-headless --discover
  
  # Scrape last 6 months
  python wa_scraper.py --months 6
        """
    )
    
    parser.add_argument('--start-year', type=int, default=2024,
                       help='Start year for scraping (default: 2024)')
    parser.add_argument('--end-year', type=int,
                       help='End year for scraping (default: current year)')
    parser.add_argument('--months', type=int,
                       help='Scrape last N months (overrides year settings)')
    parser.add_argument('--resume', action='store_true',
                       help='Resume from last completed year/month')
    parser.add_argument('--workers', type=int, default=15,
                       help='Number of concurrent workers (default: 15)')
    parser.add_argument('--start-date', type=str,
                       help='Start date (YYYY-MM-DD) - overrides other date options')
    parser.add_argument('--end-date', type=str,
                       help='End date (YYYY-MM-DD) - defaults to today')
    parser.add_argument('--discover', action='store_true',
                       help='Only discover meetings, do not download XLS files')
    parser.add_argument('--no-headless', action='store_true',
                       help='Show browser window (default is headless mode)')
    parser.add_argument('--headless', action='store_true',
                       help='Run browser in headless mode (default behavior)')
    
    args = parser.parse_args()
    
    # Calculate date range
    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    elif args.months:
        start_date = datetime.now() - timedelta(days=30 * args.months)
    elif args.resume:
        # Load existing progress and resume from there
        scraper_temp = WAComprehensiveScraper()
        if scraper_temp.progress_data['last_completed_year']:
            start_date = datetime(scraper_temp.progress_data['last_completed_year'],
                                scraper_temp.progress_data['last_completed_month'] or 1, 1)
        else:
            start_date = datetime(args.start_year, 1, 1)
        del scraper_temp
    else:
        # Default to June 1, 2024 for WA harness racing data collection
        start_date = datetime(2024, 6, 1)
    
    if args.end_date:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    elif args.end_year:
        end_date = datetime(args.end_year, 12, 31)
    else:
        end_date = datetime.now()
    
    # Show configuration
    print(f"\nWA COMPREHENSIVE HARNESS RACING SCRAPER")
    print(f"{'='*50}")
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"Total days: {(end_date - start_date).days + 1:,}")
    print(f"Concurrent workers: {args.workers}")
    if args.resume:
        print(f"Resume mode: ENABLED")
    print(f"{'='*50}\n")
    
    # Confirm for large ranges (skip confirmation for discovery mode)
    total_days = (end_date - start_date).days + 1
    if total_days > 365 and not args.discover:
        print(f"⚠️  Large date range detected: {total_days:,} days")
        print(f"⚠️  This may take considerable time and download many files")
        response = input("Continue? [y/N]: ").strip().lower()
        if response not in ['y', 'yes']:
            print("Scraping cancelled")
            return
    
    # Run scraper (headless by default unless --no-headless is specified)
    headless_mode = not args.no_headless
    scraper = WAComprehensiveScraper(max_workers=args.workers, headless=headless_mode)
    
    if args.discover:
        # Discovery only mode
        print("🔍 DISCOVERING MEETINGS...")
        meeting_urls = scraper.discover_harness_meetings(start_date, end_date)
        
        print("\n" + "=" * 50)
        print("📊 DISCOVERY RESULTS")
        print("=" * 50)
        print(f"Found {len(meeting_urls)} harness racing meetings:")
        print()
        
        if meeting_urls:
            # Group by date for better display
            from collections import defaultdict
            meetings_by_date = defaultdict(list)
            
            for url in meeting_urls:
                if "/harness/" in url:
                    try:
                        parts = url.split("/harness/")
                        if len(parts) > 1:
                            date_str = parts[1].split("/")[0]
                            venue_part = parts[1].split("/")[1] if len(parts[1].split("/")) > 1 else "Unknown"
                            venue = venue_part.replace('%20', ' ')
                            meetings_by_date[date_str].append((venue, url))
                    except:
                        print(f"  {url}")
            
            # Display grouped by date
            for date_str in sorted(meetings_by_date.keys()):
                print(f"📅 {date_str}:")
                for venue, url in meetings_by_date[date_str]:
                    print(f"   🏇 {venue}")
                    print(f"      {url}")
                print()
        
        print(f"✅ Discovery completed! Found {len(meeting_urls)} meetings.")
        scraper._close_driver()
    else:
        # Full scrape mode
        print("🚀 STARTING FULL SCRAPE...")
        scraper.scrape_historical_range(start_date, end_date)


if __name__ == "__main__":
    main()