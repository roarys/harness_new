import sys
from pathlib import Path
from typing import List, Dict, Optional, Generator
import time
import random
from datetime import datetime, timedelta
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

sys.path.append(str(Path(__file__).parent.parent))

from utils.base_scraper import BaseScraper
from config.states import STATES_CONFIG

class TASScraper(BaseScraper):
    def __init__(self, base_dir: str = None):
        if base_dir is None:
            base_dir = str(Path(__file__).parent.parent)
        super().__init__('tas', base_dir)
        
        # TAS specific configuration
        self.base_url = "https://test.tasracing.com.au/wp-content/uploads"
        self.tracks = ['Hobart', 'Launceston', 'Devonport', 'Carrick']
        self.format_cutoff = datetime(2020, 12, 31)  # End of 2020 cutoff: 2020 uses dash, 2021+ uses underscore
        
        # Setup session with headers to avoid blocking
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/pdf,*/*',
            'Referer': 'https://tasracing.com.au/'
        })
        
        # Thread lock for logging
        self.log_lock = threading.Lock()
    
    def generate_date_range(self, start_date: str = "2020-01-01", end_date: str = None) -> Generator[datetime, None, None]:
        """Generate all dates from start_date to end_date"""
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.now() if end_date is None else datetime.strptime(end_date, "%Y-%m-%d")
        
        current = start
        while current <= end:
            yield current
            current += timedelta(days=1)
    
    def get_url_format(self, date: datetime, track: str) -> str:
        """Generate URL based on date and track with correct format (underscore vs dash)"""
        # Format date components
        year = date.strftime("%Y")
        month = date.strftime("%m")
        day_month_year = date.strftime("%d%m%Y")
        
        # Determine separator based on date (End of 2020 cutoff)
        if date <= self.format_cutoff:
            separator = "-"  # Dash for 2020 and earlier
        else:
            separator = "_"  # Underscore for 2021 and later
        
        # Build URL
        filename = f"{track}{separator}{day_month_year}.pdf"
        url = f"{self.base_url}/{year}/{month}/{filename}"
        
        return url, filename
    
    def test_url_exists(self, url: str) -> bool:
        """Test if a URL returns valid content (200 status with PDF content)"""
        try:
            response = self.session.head(url, timeout=10)
            
            # Check for successful response
            if response.status_code == 200:
                # Check if it's actually a PDF
                content_type = response.headers.get('content-type', '').lower()
                if 'pdf' in content_type or 'application/octet-stream' in content_type:
                    return True
                    
                # Some servers don't set content-type correctly, check content-length
                content_length = response.headers.get('content-length')
                if content_length and int(content_length) > 1000:  # PDFs should be >1KB
                    return True
            
            return False
            
        except Exception as e:
            # Log errors but don't fail the whole process
            return False
    
    def download_pdf(self, url: str, filename: str) -> bool:
        """Download a PDF file if it exists"""
        try:
            if not self.test_url_exists(url):
                return False
            
            # Download the file
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Save file
            success = self.download_file_content(response.content, filename)
            
            if success:
                with self.log_lock:
                    self.logger.info(f"✓ Downloaded: {filename} ({len(response.content)} bytes)")
            
            return success
            
        except Exception as e:
            with self.log_lock:
                self.logger.warning(f"✗ Failed to download {filename}: {e}")
            return False
    
    def download_file_content(self, content: bytes, filename: str) -> bool:
        """Save file content to disk"""
        try:
            file_path = self.base_dir / 'data' / 'raw' / 'tas' / filename
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(file_path, 'wb') as f:
                f.write(content)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save {filename}: {e}")
            return False
    
    def scrape_date_range(self, start_date: str = "2020-01-01", end_date: str = None, 
                         max_workers: int = 5, test_mode: bool = False) -> Dict:
        """Scrape TAS PDFs using format-driven approach"""
        
        self.logger.info(f"Starting TAS format-driven scraping from {start_date} to {end_date or 'present'}")
        
        results = {
            'success': True,
            'total_urls_tested': 0,
            'total_found': 0,
            'downloads': {'successful': 0, 'failed': 0},
            'files': [],
            'by_track': {track: {'tested': 0, 'found': 0, 'downloaded': 0} for track in self.tracks},
            'by_year': {}
        }
        
        # Generate all URL combinations to test
        url_tasks = []
        
        for date in self.generate_date_range(start_date, end_date):
            year = date.year
            if year not in results['by_year']:
                results['by_year'][year] = {'tested': 0, 'found': 0, 'downloaded': 0}
            
            for track in self.tracks:
                url, filename = self.get_url_format(date, track)
                url_tasks.append({
                    'url': url,
                    'filename': filename,
                    'date': date,
                    'track': track,
                    'year': year
                })
        
        total_tasks = len(url_tasks)
        self.logger.info(f"Generated {total_tasks} URL combinations to test")
        
        if test_mode:
            # Limit to first 100 URLs for testing
            url_tasks = url_tasks[:100]
            self.logger.info(f"TEST MODE: Limited to {len(url_tasks)} URLs")
        
        # Process URLs with thread pool for efficiency
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_task = {
                executor.submit(self.process_url_task, task): task 
                for task in url_tasks
            }
            
            completed = 0
            
            # Process completed tasks
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                completed += 1
                
                try:
                    result = future.result()
                    
                    # Update statistics
                    results['total_urls_tested'] += 1
                    results['by_track'][task['track']]['tested'] += 1
                    results['by_year'][task['year']]['tested'] += 1
                    
                    if result['found']:
                        results['total_found'] += 1
                        results['by_track'][task['track']]['found'] += 1
                        results['by_year'][task['year']]['found'] += 1
                        
                        if result['downloaded']:
                            results['downloads']['successful'] += 1
                            results['by_track'][task['track']]['downloaded'] += 1
                            results['by_year'][task['year']]['downloaded'] += 1
                        else:
                            results['downloads']['failed'] += 1
                        
                        results['files'].append({
                            'filename': task['filename'],
                            'track': task['track'],
                            'date': task['date'].strftime('%Y-%m-%d'),
                            'year': task['year'],
                            'success': result['downloaded']
                        })
                    
                    # Progress reporting
                    if completed % 100 == 0:
                        progress = (completed / len(url_tasks)) * 100
                        found_rate = (results['total_found'] / results['total_urls_tested']) * 100
                        with self.log_lock:
                            self.logger.info(f"Progress: {progress:.1f}% ({completed}/{len(url_tasks)}) - Found: {results['total_found']} ({found_rate:.2f}%)")
                
                except Exception as e:
                    with self.log_lock:
                        self.logger.error(f"Error processing task {task['filename']}: {e}")
                
                # Small delay to be respectful
                time.sleep(random.uniform(0.05, 0.15))
        
        self.logger.info(f"TAS scraping complete - {results['total_found']} files found from {results['total_urls_tested']} URLs tested")
        return results
    
    def process_url_task(self, task: Dict) -> Dict:
        """Process a single URL task"""
        url = task['url']
        filename = task['filename']
        
        # Test if URL exists
        exists = self.test_url_exists(url)
        
        if exists:
            # Download the file
            downloaded = self.download_pdf(url, filename)
            return {'found': True, 'downloaded': downloaded}
        else:
            return {'found': False, 'downloaded': False}
    
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

        self.logger.info(f"Scraping TAS data for specific date: {target_date}")

        date_obj = datetime.strptime(target_date, "%Y-%m-%d")

        results = {
            'success': True,
            'total_urls_tested': 0,
            'total_found': 0,
            'downloads': {'successful': 0, 'failed': 0},
            'files': []
        }

        # Test all tracks for this specific date
        for track in self.tracks:
            url, filename = self.get_url_format(date_obj, track)

            # Test if URL exists
            if self.test_url_exists(url):
                # Download the file
                downloaded = self.download_pdf(url, filename)

                results['total_found'] += 1
                if downloaded:
                    results['downloads']['successful'] += 1
                else:
                    results['downloads']['failed'] += 1

                results['files'].append({
                    'filename': filename,
                    'track': track,
                    'date': target_date,
                    'success': downloaded
                })

            results['total_urls_tested'] += 1

            # Small delay between attempts
            time.sleep(random.uniform(0.1, 0.3))

        self.logger.info(f"TAS scraping for {target_date} complete - {results['downloads']['successful']} successful from {results['total_found']} found")
        return results

    def scrape(self, limit: int = None, file_types: List[str] = None,
              start_date: str = None, end_date: str = None, use_selenium: bool = False) -> Dict:
        """Main scraping method"""
        start = start_date or "2020-01-01"
        return self.scrape_date_range(start_date=start, end_date=end_date)

if __name__ == "__main__":
    scraper = TASScraper()
    
    print("TAS Sectional Times Scraper - Format-Driven Approach")
    print("=" * 70)
    print("Testing URL patterns for all tracks and dates since Jan 2020")
    print("Tracks: Hobart, Launceston, Devonport, Carrick")
    print("Format: 2020 uses dashes, 2021+ uses underscores")
    print("=" * 70)
    
    # Ask user for options
    test_mode = input("Run in test mode (first 100 URLs only)? (y/N): ").strip().lower() == 'y'
    
    if test_mode:
        print("\nRunning in TEST MODE - first 100 URLs only")
        results = scraper.scrape_date_range(start_date="2020-01-01", test_mode=True)
    else:
        print(f"\nRunning FULL SCAN from January 1, 2020 to present")
        print("This will test 8,240 URL combinations - estimated time: 2.3 hours")
        print("Expected to find ~457 files based on 5.6% find rate from testing")
        confirm = input("Continue with full scan? (y/N): ").strip().lower()
        
        if confirm == 'y':
            results = scraper.scrape_date_range(start_date="2020-01-01")
        else:
            print("Cancelled.")
            exit()
    
    # Print results
    print(f"\n" + "=" * 70)
    print("TAS SCRAPING RESULTS")
    print("=" * 70)
    
    print(f"URLs tested: {results['total_urls_tested']}")
    print(f"Files found: {results['total_found']}")
    print(f"Successfully downloaded: {results['downloads']['successful']}")
    print(f"Failed downloads: {results['downloads']['failed']}")
    
    if results['total_urls_tested'] > 0:
        find_rate = (results['total_found'] / results['total_urls_tested']) * 100
        print(f"Find rate: {find_rate:.2f}%")
    
    if results['total_found'] > 0:
        success_rate = (results['downloads']['successful'] / results['total_found']) * 100
        print(f"Download success rate: {success_rate:.1f}%")
    
    # Breakdown by track
    print(f"\nBreakdown by track:")
    for track in scraper.tracks:
        stats = results['by_track'][track]
        print(f"  {track}: {stats['downloaded']}/{stats['found']} downloaded from {stats['tested']} URLs tested")
    
    # Breakdown by year
    print(f"\nBreakdown by year:")
    for year in sorted(results['by_year'].keys()):
        stats = results['by_year'][year]
        if stats['found'] > 0:
            print(f"  {year}: {stats['downloaded']}/{stats['found']} downloaded from {stats['tested']} URLs tested")
    
    # Show recent successful downloads
    recent_files = [f for f in results['files'] if f['success']][-10:]
    if recent_files:
        print(f"\nRecent successful downloads:")
        for f in recent_files:
            print(f"  ✓ {f['filename']} - {f['track']} ({f['date']})")
    
    print(f"\nAll downloads saved to: {scraper.base_dir}/data/raw/tas/")
    print("=" * 70)