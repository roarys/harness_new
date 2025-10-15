#!/usr/bin/env python3

import sys
import argparse
from pathlib import Path
from datetime import datetime

sys.path.append(str(Path(__file__).parent))

from scrapers.qld_scraper import QLDScraper
from config.states import STATES_CONFIG

def main():
    parser = argparse.ArgumentParser(description='Australian Racing Data Scraper')
    parser.add_argument('state', choices=['qld', 'nsw', 'vic', 'sa', 'wa', 'tas', 'nt'], 
                       help='State to scrape data from')
    parser.add_argument('--limit', type=int, help='Limit number of files to download')
    parser.add_argument('--types', nargs='+', choices=['pdf', 'csv', 'zip'], 
                       default=['pdf'], help='File types to download')
    parser.add_argument('--start-date', help='Start date (YYYYMMDD format)')
    parser.add_argument('--end-date', help='End date (YYYYMMDD format)')
    parser.add_argument('--output-dir', help='Output directory (default: current directory)')
    parser.add_argument('--no-selenium', action='store_true', help='Disable Selenium expansion (faster but may miss content)')
    
    args = parser.parse_args()
    
    if args.state == 'qld':
        scraper = QLDScraper(args.output_dir)
    else:
        print(f"Scraper for {args.state.upper()} not yet implemented")
        return 1
    
    print(f"Starting {args.state.upper()} scraper...")
    print(f"File types: {args.types}")
    if args.limit:
        print(f"Limit: {args.limit} files")
    if args.start_date or args.end_date:
        print(f"Date range: {args.start_date or 'start'} to {args.end_date or 'end'}")
    
    results = scraper.scrape(
        limit=args.limit,
        file_types=args.types,
        start_date=args.start_date,
        end_date=args.end_date,
        use_selenium=not args.no_selenium
    )
    
    if results['success']:
        print(f"\n✓ Scraping completed successfully!")
        print(f"Total files found: {results['total_found']}")
        print(f"Successful downloads: {results['downloads']['successful']}")
        print(f"Failed downloads: {results['downloads']['failed']}")
        if 'skipped' in results['downloads']:
            print(f"Skipped (already extracted): {results['downloads']['skipped']}")
        
        if results['files']:
            print(f"\nFiles processed:")
            for file_info in results['files']:
                if file_info.get('skipped', False):
                    status = "◦"  # Skip indicator
                else:
                    status = "✓" if file_info['success'] else "✗"
                print(f"  {status} {file_info['filename']} ({file_info['type']}) - {file_info['venue']}")
    else:
        print(f"✗ Scraping failed: {results.get('error', 'Unknown error')}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())