import sys
import os
from pathlib import Path
from typing import List, Dict, Optional
from urllib.parse import urljoin
import time
import random

sys.path.append(str(Path(__file__).parent.parent))

from utils.base_scraper import BaseScraper
from utils.selenium_scraper import SeleniumScraper
from config.states import STATES_CONFIG

class QLDScraper(BaseScraper):
    def __init__(self, base_dir: str = None):
        super().__init__('qld', base_dir)
        self.config = STATES_CONFIG['qld']
    
    def _has_extracted_csv(self, pdf_filename: str) -> bool:
        """Check if a PDF has already been extracted to CSV"""
        csv_filename = pdf_filename.replace('.pdf', '_extracted.csv')
        processed_dir = os.path.join(self.base_dir, 'data', 'processed', 'qld')
        csv_path = os.path.join(processed_dir, csv_filename)
        return os.path.exists(csv_path)
    
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

        self.logger.info(f"Scraping QLD data for specific date: {target_date}")

        # Convert date to expected format for filtering
        from datetime import datetime
        date_obj = datetime.strptime(target_date, '%Y-%m-%d')
        date_str = date_obj.strftime('%Y%m%d')  # QLD uses YYYYMMDD format in filenames

        # Use Selenium to get all available PDFs
        with SeleniumScraper() as selenium_scraper:
            soup = selenium_scraper.get_page_with_expansions(self.config.harness_url)

        if not soup:
            return {'success': False, 'error': 'Failed to get page content'}

        patterns = {ft: self.config.file_patterns[ft] for ft in file_types if ft in self.config.file_patterns}
        all_links = self.extract_file_links(soup, patterns)

        # Filter links for the specific date
        links = []
        for link in all_links:
            if link.get('date') == date_str:
                links.append(link)

        self.logger.info(f"Found {len(links)} files for date {target_date}")

        results = {
            'success': True,
            'total_found': len(links),
            'downloads': {'successful': 0, 'failed': 0, 'skipped': 0},
            'files': []
        }

        # Download files for this date
        for link in links:
            # Skip PDFs that already have extracted CSVs
            if link['type'] == 'pdf' and self._has_extracted_csv(link['filename']):
                self.logger.info(f"Skipping {link['filename']} - already extracted")
                results['downloads']['skipped'] += 1
                results['files'].append({
                    'filename': link['filename'],
                    'type': link['type'],
                    'date': link['date'],
                    'venue': link['venue'],
                    'success': True,
                    'skipped': True
                })
                continue

            from urllib.parse import urljoin
            full_url = urljoin(self.config.base_url, link['url'])
            success = self.download_file(full_url, link['filename'])

            if success:
                results['downloads']['successful'] += 1
                import time
                import random
                time.sleep(random.uniform(0.2, 0.7))
            else:
                results['downloads']['failed'] += 1

            results['files'].append({
                'filename': link['filename'],
                'type': link['type'],
                'date': link['date'],
                'venue': link['venue'],
                'success': success,
                'skipped': False
            })

        self.logger.info(f"QLD scraping for {target_date} complete - {results['downloads']['successful']} successful, {results['downloads']['failed']} failed, {results['downloads']['skipped']} skipped")
        return results

    def scrape(self, limit: int = None, file_types: List[str] = None, start_date: str = None, end_date: str = None, use_selenium: bool = True) -> Dict:
        if file_types is None:
            file_types = ['pdf']  # Default to PDFs only

        self.logger.info(f"Starting QLD scraping - limit: {limit}, types: {file_types}, selenium: {use_selenium}")

        if use_selenium:
            # Use Selenium to expand all sections and get complete content
            with SeleniumScraper() as selenium_scraper:
                soup = selenium_scraper.get_page_with_expansions(self.config.harness_url)
        else:
            # Fallback to regular requests
            soup = self.get_page_content(self.config.harness_url)

        if not soup:
            return {'success': False, 'error': 'Failed to get page content'}

        patterns = {ft: self.config.file_patterns[ft] for ft in file_types if ft in self.config.file_patterns}
        links = self.extract_file_links(soup, patterns)

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
            'downloads': {'successful': 0, 'failed': 0, 'skipped': 0},
            'files': []
        }
        
        # Download all files
        for link in links:
            # Skip PDFs that already have extracted CSVs
            if link['type'] == 'pdf' and self._has_extracted_csv(link['filename']):
                self.logger.info(f"Skipping {link['filename']} - already extracted")
                results['downloads']['skipped'] += 1
                results['files'].append({
                    'filename': link['filename'],
                    'type': link['type'],
                    'date': link['date'],
                    'venue': link['venue'],
                    'success': True,
                    'skipped': True
                })
                continue
            full_url = urljoin(self.config.base_url, link['url'])
            success = self.download_file(full_url, link['filename'])
            
            if success:
                results['downloads']['successful'] += 1
                time.sleep(random.uniform(0.2, 0.7))
            else:
                results['downloads']['failed'] += 1
            
            results['files'].append({
                'filename': link['filename'],
                'type': link['type'],
                'date': link['date'],
                'venue': link['venue'],
                'success': success,
                'skipped': False
            })
        
        self.logger.info(f"Download complete - {results['downloads']['successful']} successful, {results['downloads']['failed']} failed, {results['downloads']['skipped']} skipped")
        return results

if __name__ == "__main__":
    scraper = QLDScraper()
    
    # Download all PDF files after expanding all accordion sections
    print("Starting bulk download of all QLD sectional PDFs...")
    print("This will download all 2000+ files - it may take a while!")
    
    results = scraper.scrape(limit=None, file_types=['pdf'], use_selenium=True)
    
    print(f"\nScraping Results:")
    print(f"Total found: {results['total_found']}")
    print(f"Successful downloads: {results['downloads']['successful']}")
    print(f"Failed downloads: {results['downloads']['failed']}")
    print(f"Skipped (already extracted): {results['downloads']['skipped']}")
    print(f"Success rate: {results['downloads']['successful']/results['total_found']*100:.1f}%")
    
    # Only show first 10 and last 10 results to avoid spam
    if len(results['files']) > 20:
        print(f"\nFirst 10 files:")
        for file_info in results['files'][:10]:
            if file_info.get('skipped', False):
                status = "◦"  # Skip indicator
            else:
                status = "✓" if file_info['success'] else "✗"
            print(f"{status} {file_info['filename']} ({file_info['type']}) - {file_info['date']} - {file_info['venue']}")
        
        print(f"\n... ({len(results['files'])-20} files omitted) ...\n")
        
        print(f"Last 10 files:")
        for file_info in results['files'][-10:]:
            if file_info.get('skipped', False):
                status = "◦"  # Skip indicator
            else:
                status = "✓" if file_info['success'] else "✗"
            print(f"{status} {file_info['filename']} ({file_info['type']}) - {file_info['date']} - {file_info['venue']}")
    else:
        for file_info in results['files']:
            if file_info.get('skipped', False):
                status = "◦"  # Skip indicator
            else:
                status = "✓" if file_info['success'] else "✗"
            print(f"{status} {file_info['filename']} ({file_info['type']}) - {file_info['date']} - {file_info['venue']}")
    
    print(f"\nDownloads saved to: {scraper.base_dir}/data/raw/qld/")