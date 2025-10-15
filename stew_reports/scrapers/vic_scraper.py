import sys
from pathlib import Path
from typing import List, Dict, Optional
from urllib.parse import urljoin
import re
import time
import random

sys.path.append(str(Path(__file__).parent.parent))

from utils.base_scraper import BaseScraper
from utils.selenium_scraper import SeleniumScraper
from config.states import STATES_CONFIG

class VICScraper(BaseScraper):
    def __init__(self, base_dir: str = None):
        # Use parent directory to align with other scrapers
        if base_dir is None:
            base_dir = str(Path(__file__).parent.parent)
        super().__init__('vic', base_dir)
        self.config = STATES_CONFIG['vic']
    
    def scrape_all_links(self, limit: int = None) -> List[Dict]:
        """Scrape all PDF links directly from the page HTML"""
        from selenium.webdriver.common.by import By
        
        self.logger.info("Scraping all PDF links from VIC sectionals page...")
        
        pdf_links = []
        
        # Use headless mode for simple link extraction
        with SeleniumScraper(headless=True) as selenium_scraper:
            driver = selenium_scraper.driver
            driver.get(self.config.harness_url)
            time.sleep(3)  # Allow page load
            
            # Find all PDF links on the page
            all_pdf_links = driver.find_elements(By.XPATH, "//a[contains(@href, '.pdf')]")
            self.logger.info(f"Found {len(all_pdf_links)} total PDF links on page")
            
            # Filter for sectional PDFs (hosted on S3)
            sectional_count = 0
            for link in all_pdf_links:
                href = link.get_attribute('href')
                
                # Only get sectional PDFs from S3, not other PDFs
                if 'sectionals.s3.us-west-1.wasabisys.com' in href:
                    # Extract filename and venue info
                    filename = href.split('/')[-1]
                    
                    # Determine if it's end-of-day or track-specific
                    if '/eod/' in href:
                        file_type = 'eod'
                        # Pattern: SP190825_20250819.pdf
                        match = re.search(r'([A-Z]+)(\d{6})_(\d{8})\.pdf', filename)
                        if match:
                            venue_code, date1, date2 = match.groups()
                            display_name = f"{venue_code}_{date1}_{date2}"
                        else:
                            display_name = filename
                        venue = venue_code if match else 'unknown'
                    else:
                        file_type = 'track'
                        # Pattern: Shepparton_19082025.pdf
                        match = re.search(r'([A-Za-z_]+)_(\d{8})\.pdf', filename)
                        if match:
                            venue, date = match.groups()
                            display_name = f"{venue}_{date}"
                        else:
                            display_name = filename
                        venue = venue if match else 'unknown'
                    
                    pdf_links.append({
                        'url': href,
                        'filename': filename,
                        'display_name': display_name,
                        'type': file_type,
                        'venue': venue
                    })
                    
                    sectional_count += 1
                    
                    # Apply limit if specified
                    if limit and sectional_count >= limit:
                        break
            
            self.logger.info(f"Extracted {sectional_count} sectional PDF links total")
        
        return pdf_links
    
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
        date_str = date_obj.strftime('%d%m%Y')  # VIC uses DDMMYYYY format in some filenames
        date_str_alt = date_obj.strftime('%Y%m%d')  # Alternative format YYYYMMDD

        self.logger.info(f"Scraping VIC data for specific date: {target_date}")

        # Get all PDF links
        all_links = self.scrape_all_links(None)

        # Filter links for the specific date
        links = []
        for link in all_links:
            filename = link['filename']
            # Check if filename contains the target date in either format
            if date_str in filename or date_str_alt in filename:
                links.append(link)

        self.logger.info(f"Found {len(links)} files for date {target_date}")

        results = {
            'success': True,
            'total_found': len(links),
            'downloads': {'successful': 0, 'failed': 0},
            'files': []
        }

        # Download files for this date
        for link in links:
            success = self.download_file(link['url'], link['filename'])

            # Random sleep between PDF downloads
            sleep_time = random.uniform(0.2, 0.7)
            time.sleep(sleep_time)

            if success:
                results['downloads']['successful'] += 1
            else:
                results['downloads']['failed'] += 1

            results['files'].append({
                'filename': link['filename'],
                'display_name': link['display_name'],
                'type': link['type'],
                'venue': link['venue'],
                'success': success
            })

        self.logger.info(f"VIC scraping for {target_date} complete - {results['downloads']['successful']} successful, {results['downloads']['failed']} failed")
        return results

    def scrape(self, limit: int = None, file_types: List[str] = None, start_date: str = None, end_date: str = None, use_selenium: bool = True) -> Dict:
        """Main scraping method"""
        self.logger.info(f"Starting VIC scraping - limit: {limit}")

        # Get all PDF links
        links = self.scrape_all_links(limit)

        results = {
            'success': True,
            'total_found': len(links),
            'downloads': {'successful': 0, 'failed': 0},
            'files': []
        }

        self.logger.info(f"Found {len(links)} files to download")
        
        # Download all files
        for link in links:
            success = self.download_file(link['url'], link['filename'])
            
            # Random sleep between PDF downloads (0.2-0.7s)
            sleep_time = random.uniform(0.2, 0.7)
            time.sleep(sleep_time)
            
            if success:
                results['downloads']['successful'] += 1
            else:
                results['downloads']['failed'] += 1
            
            results['files'].append({
                'filename': link['filename'],
                'display_name': link['display_name'],
                'type': link['type'],
                'venue': link['venue'],
                'success': success
            })
        
        self.logger.info(f"Download complete - {results['downloads']['successful']} successful, {results['downloads']['failed']} failed")
        return results

if __name__ == "__main__":
    scraper = VICScraper()
    
    print("VIC Sectional Times Scraper")
    print("=" * 60)
    print("Starting full download of all VIC sectional PDF files...")
    print("Estimated time: 1.5-2 hours for ~1,735 files")
    print("=" * 60)
    
    # Run full scraper to download all files
    results = scraper.scrape()
    
    print(f"\nVIC Scraping Complete!")
    print(f"=" * 60)
    print(f"Total found: {results['total_found']}")
    print(f"Successful downloads: {results['downloads']['successful']}")
    print(f"Failed downloads: {results['downloads']['failed']}")
    
    if results['total_found'] > 0:
        success_rate = results['downloads']['successful']/results['total_found']*100
        print(f"Success rate: {success_rate:.1f}%")
    
    # Show breakdown by file type
    eod_files = len([f for f in results['files'] if f['type'] == 'eod' and f['success']])
    track_files = len([f for f in results['files'] if f['type'] == 'track' and f['success']])
    
    print(f"\nFile breakdown:")
    print(f"EOD files downloaded: {eod_files}")
    print(f"Track files downloaded: {track_files}")
    
    # Show any failures
    failed_files = [f for f in results['files'] if not f['success']]
    if failed_files:
        print(f"\nFailed downloads ({len(failed_files)}):")
        for f in failed_files[:10]:  # Show first 10 failures
            print(f"  ✗ {f['display_name']} ({f['type']})")
        if len(failed_files) > 10:
            print(f"  ... and {len(failed_files) - 10} more")
    
    print(f"\nAll downloads saved to: {scraper.base_dir}/data/raw/vic/")
    print(f"=" * 60)