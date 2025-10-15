import sys
from pathlib import Path
from typing import List, Dict, Optional
from urllib.parse import urljoin
import re
import time
import random
from datetime import datetime, timedelta

sys.path.append(str(Path(__file__).parent.parent))

from utils.base_scraper import BaseScraper
from utils.selenium_scraper import SeleniumScraper
from config.states import STATES_CONFIG

class NSWScraper(BaseScraper):
    def __init__(self, base_dir: str = None):
        # Use parent directory to align with QLD data structure
        if base_dir is None:
            base_dir = str(Path(__file__).parent.parent)
        super().__init__('nsw', base_dir)
        self.config = STATES_CONFIG['nsw']
    
    def scrape_date_range(self, days_back: int = 30, file_types: List[str] = None, start_from_date: str = None) -> Dict:
        """Scrape historical data by iterating through date range using datepicker"""
        if file_types is None:
            file_types = ['pdf']

        self.logger.info(f"Starting NSW historical scraping - {days_back} days back")

        all_results = {
            'success': True,
            'total_found': 0,
            'downloads': {'successful': 0, 'failed': 0},
            'files': []
        }

        # Generate date range (going backwards from today or specified start date)
        if start_from_date:
            # Parse the start_from_date (format: dd/mm/yyyy)
            end_date = datetime.strptime(start_from_date, '%d/%m/%Y')
        else:
            end_date = datetime.now()

        if days_back:
            start_date = end_date - timedelta(days=days_back)
        else:
            # Default to 24/02/2015 for full historical scraping
            start_date = datetime(2015, 2, 24)

        current_date = end_date
        total_days = (end_date - start_date).days

        self.logger.info(f"Scraping from {current_date.strftime('%d/%m/%Y')} back to {start_date.strftime('%d/%m/%Y')} ({total_days} days)")
        
        with SeleniumScraper() as selenium_scraper:
            day_count = 0
            while current_date >= start_date:
                day_count += 1
                date_str = current_date.strftime('%d/%m/%Y')  # Common AU date format
                
                # Progress tracking for long operations
                if day_count % 50 == 0 or day_count == 1:
                    progress = ((end_date - current_date).days / total_days) * 100
                    self.logger.info(f"Progress: {progress:.1f}% - Day {day_count}/{total_days} - Current date: {date_str}")
                    self.logger.info(f"So far: {all_results['total_found']} files found, {all_results['downloads']['successful']} downloaded")
                
                # Get page with specific date (with retry logic)
                soup = None
                for attempt in range(3):  # Try up to 3 times
                    try:
                        soup = selenium_scraper.get_page_with_date_navigation(self.config.harness_url, date_str)
                        if soup:
                            break
                    except Exception as e:
                        self.logger.warning(f"Attempt {attempt + 1} failed for date {date_str}: {e}")
                        if attempt < 2:  # Don't sleep on last attempt
                            time.sleep(5)  # Wait before retry
                
                if soup:
                    patterns = {ft: self.config.file_patterns[ft] for ft in file_types if ft in self.config.file_patterns}
                    links = self.extract_search_results_links(soup, patterns)
                    
                    # Process links for this date
                    for link in links:
                        filename = link['filename']
                        # Extract date and venue from filename
                        match = re.match(r'(\d{6})\s+(.+)', filename.replace('.pdf', ''))
                        if match:
                            date_part, venue_part = match.groups()
                            if len(date_part) == 6:
                                year = '20' + date_part[:2]
                                full_date = year + date_part[2:]
                                link['date'] = full_date
                                link['venue'] = venue_part.strip()
                        
                        # Only download if we haven't seen this file before
                        if not any(f['filename'] == filename for f in all_results['files']):
                            full_url = urljoin(self.config.base_url, link['url'])
                            success = self.download_file(full_url, link['filename'])
                            
                            # Random sleep between PDF downloads (0.2-0.7s)
                            sleep_time = random.uniform(0.2, 0.7)
                            time.sleep(sleep_time)
                            
                            if success:
                                all_results['downloads']['successful'] += 1
                            else:
                                all_results['downloads']['failed'] += 1
                            
                            all_results['files'].append({
                                'filename': link['filename'],
                                'type': link['type'],
                                'date': link['date'],
                                'venue': link['venue'],
                                'success': success
                            })
                            
                            all_results['total_found'] += 1
                
                # Wait between date changes (1.5-3s)
                date_change_sleep = random.uniform(1.5, 3.0)
                time.sleep(date_change_sleep)
                
                # Move to previous day
                current_date -= timedelta(days=1)
        
        self.logger.info(f"Historical scraping complete - {all_results['downloads']['successful']} successful, {all_results['downloads']['failed']} failed")
        return all_results

    def extract_search_results_links(self, soup, patterns: Dict[str, str]) -> List[Dict]:
        """Extract file links ONLY from search results section, NOT reports/results section"""
        links = []
        
        # Debug: Log all elements with IDs on page
        all_elements_with_ids = soup.find_all(lambda tag: tag.get('id'))
        element_ids = [elem.get('id') for elem in all_elements_with_ids]
        # self.logger.info(f"DEBUG: Found elements with IDs: {element_ids}")
        
        # Look for div with ID containing "SearchResults"
        search_results_section = soup.find('div', id=lambda x: x and 'searchresults' in x.lower())
        
        if search_results_section:
            self.logger.info(f"DEBUG: Found SearchResults div with ID: {search_results_section.get('id')}")
        else:
            # Fallback: look for any element with ID containing "search" and "result"
            search_results_section = soup.find(lambda tag: tag.get('id') and 'search' in tag.get('id').lower() and 'result' in tag.get('id').lower())
            if search_results_section:
                self.logger.info(f"DEBUG: Found search results element with ID: {search_results_section.get('id')}")
            else:
                self.logger.warning("DEBUG: Could not find SearchResults div by ID")
        
        if not search_results_section:
            self.logger.warning("Could not find 'Search Results' section with PDF links - returning no links")
            return []
        
        # Only get links from the identified search results section
        valid_links = search_results_section.find_all('a', href=True)
        self.logger.info(f"Found search results section with {len(valid_links)} total links")
        
        # Process only the valid links from search results
        for link in valid_links:
            href = link['href']
            for file_type, pattern in patterns.items():
                match = re.search(pattern, href)
                if match:
                    links.append({
                        'url': href,
                        'type': file_type,
                        'date': match.group(1) if match.groups() else None,
                        'venue': match.group(2) if len(match.groups()) > 1 else None,
                        'filename': self.generate_filename(match, file_type)
                    })
        
        self.logger.info(f"Found {len(links)} PDF files in SEARCH RESULTS section only")
        return links

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

        # Convert date format from YYYY-MM-DD to dd/mm/yyyy for NSW
        date_obj = datetime.strptime(target_date, '%Y-%m-%d')
        date_str = date_obj.strftime('%d/%m/%Y')

        self.logger.info(f"Scraping NSW data for specific date: {date_str}")

        results = {
            'success': True,
            'total_found': 0,
            'downloads': {'successful': 0, 'failed': 0},
            'files': []
        }

        with SeleniumScraper() as selenium_scraper:
            # Get page with specific date
            soup = selenium_scraper.get_page_with_date_navigation(self.config.harness_url, date_str)

            if soup:
                patterns = {ft: self.config.file_patterns[ft] for ft in file_types if ft in self.config.file_patterns}
                links = self.extract_search_results_links(soup, patterns)

                # Process links for this date
                for link in links:
                    filename = link['filename']
                    # Extract date and venue from filename
                    match = re.match(r'(\d{6})\s+(.+)', filename.replace('.pdf', ''))
                    if match:
                        date_part, venue_part = match.groups()
                        if len(date_part) == 6:
                            year = '20' + date_part[:2]
                            full_date = year + date_part[2:]
                            link['date'] = full_date
                            link['venue'] = venue_part.strip()

                    # Download the file
                    full_url = urljoin(self.config.base_url, link['url'])
                    success = self.download_file(full_url, link['filename'])

                    if success:
                        results['downloads']['successful'] += 1
                    else:
                        results['downloads']['failed'] += 1

                    results['files'].append({
                        'filename': link['filename'],
                        'type': link['type'],
                        'date': link.get('date'),
                        'venue': link.get('venue'),
                        'success': success
                    })

                    results['total_found'] += 1

                    # Small delay between downloads
                    time.sleep(random.uniform(0.2, 0.7))

        self.logger.info(f"NSW scraping for {date_str} complete - {results['downloads']['successful']} successful, {results['downloads']['failed']} failed")
        return results

    def scrape(self, limit: int = None, file_types: List[str] = None, start_date: str = None, end_date: str = None, use_selenium: bool = True) -> Dict:
        if file_types is None:
            file_types = ['pdf']  # Default to PDFs only
        
        self.logger.info(f"Starting NSW scraping - limit: {limit}, types: {file_types}, selenium: {use_selenium}")
        
        if use_selenium:
            # Use Selenium to handle potential dropdown interactions
            with SeleniumScraper() as selenium_scraper:
                soup = selenium_scraper.get_page_with_expansions(self.config.harness_url)
        else:
            # Fallback to regular requests
            soup = self.get_page_content(self.config.harness_url)
        
        if not soup:
            return {'success': False, 'error': 'Failed to get page content'}
        
        patterns = {ft: self.config.file_patterns[ft] for ft in file_types if ft in self.config.file_patterns}
        links = self.extract_search_results_links(soup, patterns)
        
        # NSW specific: Extract date and venue from filename instead of URL pattern
        for link in links:
            filename = link['filename']
            # Try to extract date and venue from filename like "250819 Newcastle.pdf"
            match = re.match(r'(\d{6})\s+(.+)', filename.replace('.pdf', ''))
            if match:
                date_part, venue_part = match.groups()
                # Convert YYMMDD to YYYYMMDD format
                if len(date_part) == 6:
                    year = '20' + date_part[:2]  # Assume 20xx for now
                    full_date = year + date_part[2:]
                    link['date'] = full_date
                    link['venue'] = venue_part.strip()
        
        if start_date or end_date:
            links = self.filter_links_by_date(links, start_date, end_date)
        
        # Sort by date (newest first)
        links.sort(key=lambda x: x['date'] or '0', reverse=True)
        
        if limit:
            links = links[:limit]
        
        self.logger.info(f"Found {len(links)} files to download")
        
        results = {
            'success': True,
            'total_found': len(links),
            'downloads': {'successful': 0, 'failed': 0},
            'files': []
        }
        
        # Download all files
        for link in links:
            full_url = urljoin(self.config.base_url, link['url'])
            success = self.download_file(full_url, link['filename'])
            
            # Random sleep between PDF downloads (0.2-0.7s)
            sleep_time = random.uniform(0.2, 0.7)
            time.sleep(sleep_time)
            
            if success:
                results['downloads']['successful'] += 1
            else:
                results['downloads']['failed'] += 1
            
            results['files'].append({
                'filename': link['filename'],
                'type': link['type'],
                'date': link['date'],
                'venue': link['venue'],
                'success': success
            })
        
        self.logger.info(f"Download complete - {results['downloads']['successful']} successful, {results['downloads']['failed']} failed")
        return results
    
    def extract_track_options(self, soup) -> List[str]:
        """Extract all track options from the dropdown selector"""
        tracks = []
        try:
            # Look for select element with track options
            track_selects = soup.find_all('select')
            for select in track_selects:
                options = select.find_all('option')
                for option in options:
                    if option.get('value') and option.text.strip():
                        tracks.append(option.text.strip())
            
            self.logger.info(f"Found {len(tracks)} track options")
            return tracks
        except Exception as e:
            self.logger.error(f"Failed to extract track options: {e}")
            return []

if __name__ == "__main__":
    scraper = NSWScraper()
    
    print("NSW Sectional Times Scraper")
    print("=" * 60)
    print("1. Current data (last 2 weeks)")
    print("2. Custom date range (specify days back)")
    print("3. Full historical scraping (back to 24/02/2015)")
    print("4. Test mode (last 5 days)")
    
    choice = input("\nSelect option (1-4): ").strip()
    
    if choice == "1":
        print("\nScraping current data (last 14 days)...")
        results = scraper.scrape_date_range(days_back=14, file_types=['pdf'])
    
    elif choice == "2":
        days = int(input("Enter number of days back: "))
        print(f"\nScraping last {days} days...")
        results = scraper.scrape_date_range(days_back=days, file_types=['pdf'])
    
    elif choice == "3":
        confirm = input("\nThis will scrape ALL data back to 24/02/2015 (~10 years). Continue? (y/N): ")
        if confirm.lower() == 'y':
            print("\nStarting full historical scraping...")
            results = scraper.scrape_date_range(days_back=None, file_types=['pdf'])
        else:
            print("Cancelled.")
            exit()
    
    elif choice == "4":
        print("\nTest mode: scraping last 5 days...")
        results = scraper.scrape_date_range(days_back=5, file_types=['pdf'])
    
    else:
        print("Invalid choice. Exiting.")
        exit()
    
    print(f"\nScraping Results:")
    print(f"Total found: {results['total_found']}")
    print(f"Successful downloads: {results['downloads']['successful']}")
    print(f"Failed downloads: {results['downloads']['failed']}")
    
    if results['total_found'] > 0:
        print(f"Success rate: {results['downloads']['successful']/results['total_found']*100:.1f}%")
    
    # Show results appropriately based on count
    if len(results['files']) > 20:
        print(f"\nFirst 10 files:")
        for file_info in results['files'][:10]:
            status = "✓" if file_info['success'] else "✗"
            print(f"{status} {file_info['filename']} ({file_info['type']}) - {file_info['date']} - {file_info['venue']}")
        
        print(f"\n... ({len(results['files'])-20} files omitted) ...\n")
        
        print(f"Last 10 files:")
        for file_info in results['files'][-10:]:
            status = "✓" if file_info['success'] else "✗"
            print(f"{status} {file_info['filename']} ({file_info['type']}) - {file_info['date']} - {file_info['venue']}")
    else:
        print(f"\nFiles downloaded:")
        for file_info in results['files']:
            status = "✓" if file_info['success'] else "✗"
            print(f"{status} {file_info['filename']} ({file_info['type']}) - {file_info['date']} - {file_info['venue']}")
    
    print(f"\nDownloads saved to: {scraper.base_dir}/data/raw/nsw/")