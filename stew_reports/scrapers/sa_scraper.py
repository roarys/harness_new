import sys
from pathlib import Path
from typing import List, Dict, Optional
import time
import random
import re

sys.path.append(str(Path(__file__).parent.parent))

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from utils.base_scraper import BaseScraper
from config.states import STATES_CONFIG

class AntiBot_SeleniumScraper:
    def __init__(self, headless: bool = False):
        self.driver = None
        
        chrome_options = Options()
        
        # Anti-bot measures
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--start-maximized")
        
        # More realistic user agent
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        # Disable automation indicators
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Additional stealth options
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins-discovery")
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--allow-running-insecure-content")
        
        if headless:
            chrome_options.add_argument("--headless")
        
        try:
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Execute script to remove automation flags
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
        except Exception as e:
            print(f"Failed to initialize Chrome driver: {e}")
            raise
    
    def close(self):
        if self.driver:
            self.driver.quit()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

class SAScraper(BaseScraper):
    def __init__(self, base_dir: str = None):
        # Use parent directory to align with other scrapers
        if base_dir is None:
            base_dir = str(Path(__file__).parent.parent)
        super().__init__('sa', base_dir)
        self.config = STATES_CONFIG['sa']
        
        # Setup enhanced session for SA with anti-bot headers
        self.setup_enhanced_session()
    
    def setup_enhanced_session(self):
        """Setup enhanced session with realistic headers for SA Trots"""
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/pdf,text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
            'Referer': 'https://satrots.com.au/'
        })
        
        # Set cookies that might be expected
        self.session.cookies.update({
            'viewed_cookie_policy': 'yes',
            'cookielawinfo-checkbox-necessary': 'yes'
        })
    
    def download_file_enhanced(self, url: str, filename: str) -> bool:
        """Enhanced download method with anti-bot measures for SA"""
        try:
            # Add small delay before download
            time.sleep(random.uniform(0.5, 1.5))
            
            # Use HEAD request first to check if file exists
            head_response = self.session.head(url, timeout=15, allow_redirects=True)
            
            if head_response.status_code != 200:
                self.logger.warning(f"HEAD request failed for {url}: {head_response.status_code}")
                return False
            
            # Now download the file
            response = self.session.get(url, timeout=30, allow_redirects=True)
            response.raise_for_status()
            
            # Verify it's actually a PDF
            content_type = response.headers.get('content-type', '').lower()
            if not ('pdf' in content_type or 'application/octet-stream' in content_type):
                # Check if content looks like PDF (starts with %PDF)
                if not response.content.startswith(b'%PDF'):
                    self.logger.warning(f"Downloaded content doesn't appear to be a PDF: {filename}")
                    return False
            
            # Save file
            file_path = self.base_dir / 'data' / 'raw' / self.state_code / filename
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(file_path, 'wb') as f:
                f.write(response.content)
            
            self.logger.info(f"✓ Downloaded: {filename} ({len(response.content)} bytes)")
            return True
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                self.logger.error(f"✗ 403 Forbidden for {filename} - Server blocking requests")
            elif e.response.status_code == 404:
                self.logger.warning(f"✗ 404 Not Found for {filename} - File may not exist")
            else:
                self.logger.error(f"✗ HTTP {e.response.status_code} for {filename}: {e}")
            return False
            
        except requests.exceptions.Timeout:
            self.logger.error(f"✗ Timeout downloading {filename}")
            return False
            
        except Exception as e:
            self.logger.error(f"✗ Failed to download {filename}: {e}")
            return False
    
    def scrape_page(self, driver, page_num: int, limit: int = None) -> List[Dict]:
        """Scrape PDF links from a specific page"""
        pdf_links = []
        
        # Human-like behavior
        driver.execute_script(f"window.scrollTo(0, {random.randint(200, 500)});")
        time.sleep(random.uniform(1, 3))
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(random.uniform(2, 4))
        
        # Get sectional PDF links (filter out constitution docs)
        all_pdf_links = driver.find_elements(By.XPATH, "//a[contains(@href, '.pdf')]")
        sectional_links = []
        
        for link in all_pdf_links:
            href = link.get_attribute('href')
            text = link.text.strip()
            
            # Filter for sectional times (exclude constitution and other docs)
            if ('Globe_Derby' in href or 'Gawler' in href or 'Port_Pirie' in href or 
                'sectional' in text.lower() or 'derby' in text.lower() or
                'pirie' in text.lower() or 'gawler' in text.lower()):
                
                filename = href.split('/')[-1]
                sectional_links.append({
                    'url': href,
                    'text': text,
                    'filename': filename,
                    'page': page_num
                })
        
        self.logger.info(f"Found {len(sectional_links)} sectional PDF links on page {page_num}")
        
        # Apply limit if specified
        if limit:
            sectional_links = sectional_links[:limit]
        
        return sectional_links
    
    def scrape_all_pages(self, limit_per_page: int = None, max_pages: int = None) -> Dict:
        """Scrape all pages of SA sectional times"""
        self.logger.info("Starting SA scraping with pagination...")
        
        all_results = {
            'success': True,
            'total_found': 0,
            'downloads': {'successful': 0, 'failed': 0},
            'files': [],
            'pages_scraped': 0
        }
        
        with AntiBot_SeleniumScraper(headless=True) as selenium_scraper:
            driver = selenium_scraper.driver
            
            # Initial page load with anti-bot delay
            time.sleep(random.uniform(2, 5))
            
            try:
                driver.get(self.config.harness_url)
                self.logger.info("✓ Page 1 loaded")
                
                # Wait for page to load
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                page_num = 1
                
                while True:
                    if max_pages and page_num > max_pages:
                        self.logger.info(f"Reached maximum pages limit ({max_pages})")
                        break
                    
                    self.logger.info(f"\n--- Processing Page {page_num} ---")
                    
                    # Scrape current page
                    page_links = self.scrape_page(driver, page_num, limit_per_page)
                    
                    if not page_links:
                        self.logger.info("No sectional PDFs found on this page")
                        break
                    
                    # Download files from this page
                    for link in page_links:
                        success = self.download_file_enhanced(link['url'], link['filename'])
                        
                        # Random sleep between downloads
                        sleep_time = random.uniform(0.3, 1.0)
                        time.sleep(sleep_time)
                        
                        if success:
                            all_results['downloads']['successful'] += 1
                        else:
                            all_results['downloads']['failed'] += 1
                        
                        all_results['files'].append({
                            'filename': link['filename'],
                            'text': link['text'],
                            'page': link['page'],
                            'success': success
                        })
                        
                        all_results['total_found'] += 1
                    
                    all_results['pages_scraped'] = page_num
                    
                    # Try to navigate to next page
                    try:
                        next_buttons = driver.find_elements(By.XPATH, "//a[contains(text(), 'NEXT') or contains(text(), 'Next') or contains(text(), '»')]")
                        
                        if next_buttons:
                            next_button = next_buttons[0]
                            
                            # Check if button is disabled or at end
                            button_class = next_button.get_attribute('class') or ''
                            if 'disabled' in button_class.lower():
                                self.logger.info("Next button is disabled - reached end")
                                break
                            
                            self.logger.info(f"Navigating to page {page_num + 1}...")
                            
                            # Scroll to button and click
                            driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                            time.sleep(2)
                            driver.execute_script("arguments[0].click();", next_button)
                            
                            # Wait for next page to load
                            time.sleep(random.uniform(5, 8))
                            
                            page_num += 1
                        else:
                            self.logger.info("No next page button found - reached end")
                            break
                            
                    except Exception as e:
                        self.logger.warning(f"Error navigating to next page: {e}")
                        break
                
            except Exception as e:
                self.logger.error(f"Error during scraping: {e}")
                all_results['success'] = False
        
        self.logger.info(f"SA scraping complete - {all_results['pages_scraped']} pages, {all_results['downloads']['successful']} successful, {all_results['downloads']['failed']} failed")
        return all_results
    
    def scrape_specific_date(self, target_date: str, file_types: List[str] = None) -> Dict:
        """Scrape PDFs for a specific date

        Args:
            target_date: Date in YYYY-MM-DD format
            file_types: List of file types to download (default: ['pdf'])

        Returns:
            Dictionary with scraping results
        """
        if file_types is None:
            file_types = ['pdf']

        from datetime import datetime
        date_obj = datetime.strptime(target_date, '%Y-%m-%d')
        date_str = date_obj.strftime('%d%m%Y')  # SA uses DDMMYYYY format in URLs

        self.logger.info(f"Scraping SA data for specific date: {target_date}")

        results = {
            'success': True,
            'total_found': 0,
            'downloads': {'successful': 0, 'failed': 0},
            'files': []
        }

        with AntiBot_SeleniumScraper(headless=True) as selenium_scraper:
            driver = selenium_scraper.driver

            try:
                driver.get(self.config.harness_url)
                self.logger.info("Page loaded")

                # Wait for page to load
                from selenium.webdriver.support.ui import WebDriverWait
                from selenium.webdriver.support import expected_conditions as EC
                from selenium.webdriver.common.by import By
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )

                # Get all PDF links
                all_pdf_links = driver.find_elements(By.XPATH, "//a[contains(@href, '.pdf')]")

                # Filter for the specific date
                for link in all_pdf_links:
                    href = link.get_attribute('href')
                    text = link.text.strip()

                    # Check if filename contains the target date
                    if date_str in href:
                        filename = href.split('/')[-1]

                        # Download the file
                        success = self.download_file_enhanced(href, filename)

                        if success:
                            results['downloads']['successful'] += 1
                        else:
                            results['downloads']['failed'] += 1

                        results['files'].append({
                            'filename': filename,
                            'text': text,
                            'success': success
                        })

                        results['total_found'] += 1

                        # Small delay between downloads
                        time.sleep(random.uniform(0.3, 1.0))

            except Exception as e:
                self.logger.error(f"Error during SA date-specific scraping: {e}")
                results['success'] = False

        self.logger.info(f"SA scraping for {target_date} complete - {results['downloads']['successful']} successful, {results['downloads']['failed']} failed")
        return results

    def scrape(self, limit: int = None, file_types: List[str] = None, start_date: str = None, end_date: str = None, use_selenium: bool = True) -> Dict:
        """Main scraping method"""
        self.logger.info(f"Starting SA scraping - limit per page: {limit}")
        return self.scrape_all_pages(limit_per_page=limit)

if __name__ == "__main__":
    scraper = SAScraper()
    
    print("SA Sectional Times Scraper")
    print("=" * 60)
    print("Starting full download of all SA sectional PDF files...")
    print("This will scrape all pages with pagination")
    print("Estimated time: 30-60 minutes depending on total pages")
    print("=" * 60)
    
    # Run full scraper to download all files from all pages
    results = scraper.scrape_all_pages()
    
    print(f"\nSA Scraping Complete!")
    print(f"=" * 60)
    print(f"Pages scraped: {results['pages_scraped']}")
    print(f"Total found: {results['total_found']}")
    print(f"Successful downloads: {results['downloads']['successful']}")
    print(f"Failed downloads: {results['downloads']['failed']}")
    
    if results['total_found'] > 0:
        success_rate = results['downloads']['successful']/results['total_found']*100
        print(f"Success rate: {success_rate:.1f}%")
    
    # Show breakdown by page
    pages_summary = {}
    for file_info in results['files']:
        page = file_info['page']
        if page not in pages_summary:
            pages_summary[page] = {'total': 0, 'successful': 0}
        pages_summary[page]['total'] += 1
        if file_info['success']:
            pages_summary[page]['successful'] += 1
    
    print(f"\nBreakdown by page:")
    for page in sorted(pages_summary.keys()):
        stats = pages_summary[page]
        print(f"  Page {page}: {stats['successful']}/{stats['total']} downloaded")
    
    # Show any failures
    failed_files = [f for f in results['files'] if not f['success']]
    if failed_files:
        print(f"\nFailed downloads ({len(failed_files)}):")
        for f in failed_files[:10]:  # Show first 10 failures
            print(f"  ✗ {f['filename']} (Page {f['page']})")
        if len(failed_files) > 10:
            print(f"  ... and {len(failed_files) - 10} more")
    
    print(f"\nAll downloads saved to: {scraper.base_dir}/data/raw/sa/")
    print(f"=" * 60)