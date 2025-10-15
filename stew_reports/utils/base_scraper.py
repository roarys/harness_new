import requests
import re
import os
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

class BaseScraper:
    def __init__(self, state_code: str, base_dir: str = None):
        self.state_code = state_code
        self.base_dir = Path(base_dir) if base_dir else Path.cwd()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        self.setup_logging()
        self.setup_directories()
    
    def setup_logging(self):
        log_dir = self.base_dir / 'logs'
        log_dir.mkdir(exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_dir / f'{self.state_code}_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(f'{self.state_code}_scraper')
    
    def setup_directories(self):
        for data_type in ['raw', 'processed']:
            dir_path = self.base_dir / 'data' / data_type / self.state_code
            dir_path.mkdir(parents=True, exist_ok=True)
    
    def download_file(self, url: str, filename: str, file_type: str = 'raw') -> bool:
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            file_path = self.base_dir / 'data' / file_type / self.state_code / filename
            
            with open(file_path, 'wb') as f:
                f.write(response.content)
            
            self.logger.info(f"Downloaded: {filename} ({len(response.content)} bytes)")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to download {url}: {str(e)}")
            return False
    
    def get_page_content(self, url: str) -> Optional[BeautifulSoup]:
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except Exception as e:
            self.logger.error(f"Failed to get page content from {url}: {str(e)}")
            return None
    
    def extract_file_links(self, soup: BeautifulSoup, patterns: Dict[str, str]) -> List[Dict]:
        links = []
        for link in soup.find_all('a', href=True):
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
        return links
    
    def generate_filename(self, match, file_type: str) -> str:
        date = match.group(1) if match.groups() else 'unknown'
        venue = match.group(2) if len(match.groups()) > 1 else 'unknown'
        return f"{date}_{venue}_H.{file_type}"
    
    def filter_links_by_date(self, links: List[Dict], start_date: str = None, end_date: str = None) -> List[Dict]:
        if not start_date and not end_date:
            return links
        
        filtered = []
        for link in links:
            if link['date']:
                try:
                    link_date = datetime.strptime(link['date'], '%Y%m%d')
                    if start_date:
                        start = datetime.strptime(start_date, '%Y%m%d')
                        if link_date < start:
                            continue
                    if end_date:
                        end = datetime.strptime(end_date, '%Y%m%d')
                        if link_date > end:
                            continue
                    filtered.append(link)
                except ValueError:
                    continue
        return filtered
    
    def scrape(self, limit: int = None, file_types: List[str] = None) -> Dict:
        raise NotImplementedError("Subclasses must implement scrape method")