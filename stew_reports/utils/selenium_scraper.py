import time
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from typing import Optional

class SeleniumScraper:
    def __init__(self, headless: bool = True, timeout: int = 30):
        self.timeout = timeout
        self.driver = None
        self.logger = logging.getLogger(__name__)
        
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        
        try:
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.driver.set_page_load_timeout(self.timeout)
        except Exception as e:
            self.logger.error(f"Failed to initialize Chrome driver: {e}")
            raise
    
    def get_page_with_date_navigation(self, url: str, target_date: str = None) -> Optional[BeautifulSoup]:
        """Get page content with date navigation for NSW-style datepickers"""
        try:
            self.logger.info(f"Loading page: {url}")
            self.driver.get(url)
            
            # Wait for page to load
            WebDriverWait(self.driver, self.timeout).until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )
            
            # If target_date is provided, interact with datepicker
            if target_date:
                try:
                    # Look for datepicker input
                    datepicker = self.driver.find_element(By.CSS_SELECTOR, "input.form-control.datepicker")
                    if datepicker.is_displayed() and datepicker.is_enabled():
                        self.logger.info(f"Found datepicker, setting date to: {target_date}")
                        
                        # Clear existing value and set new date
                        datepicker.clear()
                        datepicker.send_keys(target_date)
                        
                        # Look for associated submit button or trigger change event
                        try:
                            # Try to find a submit button
                            submit_btn = self.driver.find_element(By.CSS_SELECTOR, "button[type='submit'], input[type='submit'], .btn-primary")
                            if submit_btn.is_displayed() and submit_btn.is_enabled():
                                self.logger.info("Clicking submit button")
                                submit_btn.click()
                                time.sleep(5)  # Wait longer for AJAX response and results to load
                        except:
                            # If no submit button, trigger change event
                            self.logger.info("Triggering change event on datepicker")
                            self.driver.execute_script("arguments[0].dispatchEvent(new Event('change'));", datepicker)
                            time.sleep(5)  # Wait longer for potential AJAX and search results
                            
                except Exception as e:
                    self.logger.warning(f"Failed to interact with datepicker: {e}")
            
            # Get final page source
            page_source = self.driver.page_source
            return BeautifulSoup(page_source, 'html.parser')
            
        except Exception as e:
            self.logger.error(f"Failed to get page with date navigation: {e}")
            return None

    def get_page_with_expansions(self, url: str) -> Optional[BeautifulSoup]:
        try:
            self.logger.info(f"Loading page: {url}")
            self.driver.get(url)
            
            # Wait for page to load
            WebDriverWait(self.driver, self.timeout).until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )
            
            expanded_count = 0
            
            # Look for Queensland Racing specific accordion buttons first
            qld_accordion_buttons = self.driver.find_elements(By.CSS_SELECTOR, ".c-sectional-accordion__btn")
            
            self.logger.info(f"Found {len(qld_accordion_buttons)} QLD accordion buttons")
            
            for i, button in enumerate(qld_accordion_buttons):
                try:
                    if button.is_displayed() and button.is_enabled():
                        button_text = button.text.strip()[:50]
                        # self.logger.info(f"Clicking QLD accordion button {i+1}: '{button_text}'")
                        self.driver.execute_script("arguments[0].click();", button)
                        expanded_count += 1
                        # No wait - expand all quickly
                except Exception as e:
                    self.logger.debug(f"Failed to click QLD accordion button {i}: {e}")
            
            # Look for common expansion elements
            expansion_selectors = [
                "button[contains(text(), 'Show')]",
                "button[contains(text(), 'Load')]",
                "button[contains(text(), 'More')]",
                "a[contains(text(), 'Show')]",
                "a[contains(text(), 'Load')]",
                "a[contains(text(), 'More')]",
                ".load-more",
                ".show-more",
                ".expand",
                "[data-toggle]",
                ".collapse-toggle",
                ".btn-load-more"
            ]
            
            for selector in expansion_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        if element.is_displayed() and element.is_enabled():
                            self.logger.info(f"Clicking expansion element: {element.text[:50]}")
                            self.driver.execute_script("arguments[0].click();", element)
                            expanded_count += 1
                            time.sleep(1)  # Wait for content to load
                except Exception as e:
                    self.logger.debug(f"No elements found for selector {selector}: {e}")
            
            # Try scrolling to trigger any lazy loading
            self.logger.info("Scrolling to trigger lazy loading...")
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            
            for i in range(5):  # Try up to 5 scroll attempts
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
            
            # Scroll back to top
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)
            
            # Look for month/year toggles specifically for racing sites
            month_toggles = self.driver.find_elements(By.CSS_SELECTOR, 
                "[class*='month'], [class*='year'], [id*='month'], [id*='year']")
            
            for toggle in month_toggles:
                try:
                    if toggle.is_displayed() and toggle.is_enabled():
                        self.logger.info(f"Clicking potential month/year toggle: {toggle.get_attribute('class')}")
                        self.driver.execute_script("arguments[0].click();", toggle)
                        expanded_count += 1
                        time.sleep(1)
                except Exception as e:
                    self.logger.debug(f"Failed to click toggle: {e}")
            
            # Look for pagination or "Load More" buttons
            pagination_selectors = [
                "a[href*='page']",
                "button[onclick*='load']", 
                "button[onclick*='page']",
                ".pagination a",
                ".pager a",
                "[class*='load']",
                "[class*='page']",
                "[onclick*='ShowMore']",
                "[onclick*='LoadMore']"
            ]
            
            for selector in pagination_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        if element.is_displayed() and element.is_enabled():
                            text = element.text.strip() or element.get_attribute('title') or ''
                            if any(keyword in text.lower() for keyword in ['more', 'load', 'next', 'page']):
                                self.logger.info(f"Clicking pagination element: {text[:50]}")
                                self.driver.execute_script("arguments[0].click();", element)
                                expanded_count += 1
                                time.sleep(2)  # Wait longer for potential AJAX
                except Exception as e:
                    self.logger.debug(f"No pagination elements for selector {selector}: {e}")
            
            # Try to find and expand date ranges or archive sections
            archive_selectors = [
                "select[name*='year']",
                "select[name*='month']", 
                "option[value*='20']",  # Years like 2017, 2018, etc
                "[class*='archive']",
                "[class*='history']",
                "[class*='past']"
            ]
            
            for selector in archive_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        if element.is_displayed() and element.is_enabled():
                            if element.tag_name == 'select':
                                # For select dropdowns, try to select earlier years
                                from selenium.webdriver.support.ui import Select
                                select = Select(element)
                                options = select.options
                                # Select the earliest year available
                                for option in reversed(options):
                                    if '201' in option.text:  # 2017, 2018, etc
                                        self.logger.info(f"Selecting year option: {option.text}")
                                        select.select_by_visible_text(option.text)
                                        expanded_count += 1
                                        time.sleep(2)
                                        break
                            else:
                                self.logger.info(f"Clicking archive element: {element.get_attribute('class')}")
                                self.driver.execute_script("arguments[0].click();", element)
                                expanded_count += 1
                                time.sleep(1)
                except Exception as e:
                    self.logger.debug(f"No archive elements for selector {selector}: {e}")
            
            # Debug: Log some page elements to understand the structure
            try:
                all_buttons = self.driver.find_elements(By.TAG_NAME, "button")
                all_links = self.driver.find_elements(By.TAG_NAME, "a")
                all_selects = self.driver.find_elements(By.TAG_NAME, "select")
                
                self.logger.info(f"Debug: Found {len(all_buttons)} buttons, {len(all_links)} links, {len(all_selects)} selects")
                
                # Log first few buttons and links for inspection
                for i, btn in enumerate(all_buttons[:5]):
                    if btn.is_displayed():
                        text = btn.text.strip()[:30]
                        onclick = btn.get_attribute('onclick') or ''
                        self.logger.info(f"  Button {i}: '{text}' onclick='{onclick[:30]}'")
                
                for i, link in enumerate(all_links[:10]):
                    if link.is_displayed() and 'javascript' in (link.get_attribute('href') or ''):
                        text = link.text.strip()[:30]
                        href = link.get_attribute('href')[:50]
                        self.logger.info(f"  JS Link {i}: '{text}' href='{href}'")
                        
            except Exception as e:
                self.logger.debug(f"Debug logging failed: {e}")
            
            self.logger.info(f"Expanded {expanded_count} sections")
            
            # Get final page source
            page_source = self.driver.page_source
            return BeautifulSoup(page_source, 'html.parser')
            
        except Exception as e:
            self.logger.error(f"Failed to get page with expansions: {e}")
            return None
    
    def close(self):
        if self.driver:
            self.driver.quit()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()