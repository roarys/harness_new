#!/usr/bin/env python3
"""
Enhanced NSW PDF batch processor with comprehensive extraction and analysis.
Processes all PDFs in data/raw/nsw and saves extracted data to format-specific folders
in data/processed/nsw with detailed logging and error handling.

Supports three distinct NSW formats:
1. Standard PJ Format (Penrith): Horse names on separate lines
2. PJ Sub-format (Tamworth): Multi-race format similar to TAS
3. Standard TripleS Format (PE files): Table-based sectional data

Based on successful patterns from QLD, TAS, and VIC extraction codebases.
"""

import os
import sys
import traceback
import glob
import pandas as pd
from datetime import datetime
from pathlib import Path

from pathlib import Path
from typing import Dict, List, Any
import logging
import argparse

# Add the parent directory to the path to import pdf_extractor
sys.path.append(str(Path(__file__).parent))
from pdf_extractor import PDFExtractor

def setup_logging(sample_mode: bool = False):
    """Set up logging for the NSW processing script"""
    log_dir = "/Users/rorypearson/Desktop/harness_db/stew_reports/scrapers/logs"
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_suffix = "_sample" if sample_mode else ""
    log_file = os.path.join(log_dir, f"nsw_batch_process_{timestamp}{log_suffix}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger(__name__)

def extract_track_from_filename_nsw(filename: str) -> str:
    """Extract track name from NSW filename"""
    # NSW track mappings
    track_mapping = {
        'PE': 'Penrith', 'TA': 'Tamworth', 'WA': 'Wagga',
        'BA': 'Bathurst', 'DU': 'Dubbo', 'NE': 'Newcastle',
        'ME': 'Menangle', 'YO': 'Young', 'PC': 'Penrith'
    }

    # Check for track codes at the beginning
    if len(filename) >= 2:
        prefix = filename[:2].upper()
        if prefix in track_mapping:
            return track_mapping[prefix]

    # Check for full track names
    filename_lower = filename.lower()
    full_names = {
        'penrith': 'Penrith', 'tamworth': 'Tamworth', 'wagga': 'Wagga',
        'bathurst': 'Bathurst', 'dubbo': 'Dubbo', 'newcastle': 'Newcastle',
        'menangle': 'Menangle', 'young': 'Young'
    }

    for track_key, track_name in full_names.items():
        if track_key in filename_lower:
            return track_name

    return None

def process_single_nsw_pdf(pdf_file: str, output_dir: str, extractor=None) -> Dict:
    """Process a single NSW PDF file with track extraction"""
    if extractor is None:
        from pdf_extractor import PDFExtractor
        extractor = PDFExtractor()

    filename = os.path.basename(pdf_file)
    result = {
        'filename': filename,
        'success': False,
        'format': None,
        'track': None,
        'runners': 0,
        'races': 0,
        'output_path': None,
        'error': None
    }

    try:
        extracted_data = extractor.extract_pdf_data(pdf_file)

        if extracted_data and 'runners' in extracted_data and len(extracted_data['runners']) > 0:
            runners = extracted_data['runners']
            format_type = extracted_data.get('format', 'unknown')

            # Extract track name
            track_name = None
            if 'metadata' in extracted_data and extracted_data['metadata']:
                track_name = extracted_data['metadata'].get('venue')
            if not track_name:
                track_name = extract_track_from_filename_nsw(filename)
            if not track_name and runners:
                track_name = runners[0].get('track')

            # Add track to all runners
            if track_name:
                for runner in runners:
                    runner['track'] = track_name

            # Analyze and save
            analysis = analyze_extraction_results(extracted_data, filename)
            sub_format = analysis['sub_format']

            csv_path = save_format_specific_csv(
                runners, format_type, sub_format, output_dir, filename, track_name
            )

            result['success'] = True
            result['format'] = format_type
            result['track'] = track_name
            result['runners'] = len(runners)
            result['races'] = analysis['races']
            result['output_path'] = csv_path

        else:
            result['error'] = 'No runners extracted'

    except Exception as e:
        result['error'] = str(e)

    return result

def analyze_extraction_results(extracted_data: Dict, filename: str) -> Dict:
    """Analyze extraction results and return summary statistics"""
    analysis = {
        'filename': filename,
        'format': extracted_data.get('format', 'unknown'),
        'runners': len(extracted_data.get('runners', [])),
        'races': 0,
        'has_triples_data': False,
        'has_pj_data': False,
        'venues': set(),
        'dates': set(),
        'distances': set(),
        'sub_format': 'standard'
    }
    
    runners = extracted_data.get('runners', [])
    if runners:
        # Count unique races
        race_numbers = {r.get('race_number') for r in runners if r.get('race_number')}
        analysis['races'] = len(race_numbers) if race_numbers else 1
        
        # Check data types
        first_runner = runners[0]
        if any(key in first_runner for key in ['top_speed', 'fastest_section', 'first_quarter', 'second_quarter']):
            analysis['has_triples_data'] = True
        if any(key in first_runner for key in ['final_time', 'margin', 'last_800m', 'last_400m']):
            analysis['has_pj_data'] = True
            
        # Detect NSW sub-formats
        if 'tab_number' in first_runner and analysis['races'] > 1:
            analysis['sub_format'] = 'tamworth'  # Multi-race format
        elif 'tab_number' in first_runner:
            analysis['sub_format'] = 'penrith'   # Standard PJ with tab numbers
        elif analysis['has_triples_data']:
            analysis['sub_format'] = 'pe_triples' # TripleS format
            
        # Collect venue/date/distance info
        for runner in runners:
            if runner.get('track'):
                analysis['venues'].add(runner['track'])
            if runner.get('date'):
                analysis['dates'].add(runner['date'])
            if runner.get('distance'):
                analysis['distances'].add(runner['distance'])
    
    # Convert sets to lists for JSON serialization
    analysis['venues'] = list(analysis['venues'])
    analysis['dates'] = list(analysis['dates'])
    analysis['distances'] = list(analysis['distances'])
    
    return analysis

def save_format_specific_csv(runners: List[Dict], format_type: str, sub_format: str, output_dir: str, filename: str, track_name: str = None):
    """Save runners to format-specific CSV file with proper organization"""
    if not runners:
        return None

    # Create format-specific directory - all PJ variants go to 'pj', TripleS goes to 'triples'
    if format_type == 'pj':
        format_dir = os.path.join(output_dir, "pj")
    elif format_type == 'triples':
        format_dir = os.path.join(output_dir, "triples")
    else:
        format_dir = os.path.join(output_dir, format_type)

    os.makedirs(format_dir, exist_ok=True)

    # Generate CSV filename
    base_name = os.path.splitext(filename)[0]
    csv_filename = f"{base_name}_{format_type}.csv"
    csv_path = os.path.join(format_dir, csv_filename)

    # Add track name to each runner if provided
    if track_name:
        for runner in runners:
            runner['track'] = track_name

    # Create DataFrame and save
    df = pd.DataFrame(runners)

    # Ensure track column is first if it exists
    if 'track' in df.columns:
        cols = ['track'] + [col for col in df.columns if col != 'track']
        df = df[cols]

    df.to_csv(csv_path, index=False)

    return csv_path

def process_all_nsw_pdfs(input_dir=None, output_dir=None, logger=None, sample_size: int = None, debug: bool = False, force_reprocess: bool = True):
    """Enhanced NSW PDF processing with comprehensive analysis and error handling

    Args:
        input_dir: Input directory path (optional)
        output_dir: Output directory path (optional)
        logger: Logger instance (optional)
        sample_size: Number of files to process (optional)
        debug: Enable debug mode (optional)
        force_reprocess: If True, reprocess files even if they already exist (default: True for meta_processor)
    """

    # Setup logging
    if logger is None:
        logger = setup_logging(sample_mode=bool(sample_size))
    logger.info(f"Starting NSW PDF batch processing{' (SAMPLE MODE)' if sample_size else ''}")

    # Initialize extractor
    extractor = PDFExtractor()

    # Define paths - use defaults if not provided
    if input_dir is None:
        input_dir = "/Users/rorypearson/Desktop/harness_db/stew_reports/data/raw/nsw"
    if output_dir is None:
        output_dir = "/Users/rorypearson/Desktop/harness_db/stew_reports/data/processed/nsw"
    
    # Find all PDF files in NSW folder
    pdf_files = glob.glob(os.path.join(input_dir, "*.pdf"))
    pdf_files.sort()  # Process in alphabetical order
    
    if not pdf_files:
        logger.warning(f"No PDF files found in {input_dir}")
        return
    
    # Apply sample limitation if specified
    if sample_size:
        pdf_files = pdf_files[:sample_size]
        logger.info(f"SAMPLE MODE: Processing first {len(pdf_files)} files only")
    
    logger.info(f"Found {len(pdf_files)} NSW PDF files to process")
    logger.info(f"Input directory: {input_dir}")
    logger.info(f"Output directory: {output_dir}")
    logger.info("=" * 80)
    
    # Ensure base output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Track progress and statistics
    stats = {
        'processed': 0,
        'successful': 0,
        'failed': 0,
        'skipped': 0,
        'total_runners': 0,
        'total_races': 0,
        'format_counts': {},
        'sub_format_counts': {},
        'venue_counts': {},
        'errors': [],
        'analyses': [],
        'files': []  # Add files list for meta_processor compatibility
    }
    
    start_time = datetime.now()
    
    for i, pdf_file in enumerate(pdf_files, 1):
        filename = os.path.basename(pdf_file)
        base_name = os.path.splitext(filename)[0]
        
        # Check if file already exists in any format folder
        if not force_reprocess:
            already_exists = False
            existing_location = None
            for format_dir in ['pj', 'triples', 'unknown']:
                check_path = os.path.join(output_dir, format_dir, f"{base_name}_*.csv")
                existing_files = glob.glob(check_path)
                if existing_files:
                    print(f"[{i:3d}/{len(pdf_files)}] SKIPPED: {filename} (already exists in {format_dir}/)")
                    already_exists = True
                    existing_location = format_dir
                    stats['skipped'] += 1
                    break

            if already_exists:
                continue
        
        print(f"[{i:3d}/{len(pdf_files)}] Processing: {filename}")
        
        try:
            # Extract data
            extracted_data = extractor.extract_pdf_data(pdf_file)
            
            if extracted_data and 'runners' in extracted_data and len(extracted_data['runners']) > 0:
                # Extract track name
                track_name = None
                if 'metadata' in extracted_data and extracted_data['metadata']:
                    track_name = extracted_data['metadata'].get('venue')
                if not track_name:
                    track_name = extract_track_from_filename_nsw(filename)
                if not track_name and extracted_data['runners']:
                    track_name = extracted_data['runners'][0].get('track')

                # Analyze extraction results
                analysis = analyze_extraction_results(extracted_data, filename)
                stats['analyses'].append(analysis)

                runners = analysis['runners']
                races = analysis['races']
                format_type = analysis['format']
                sub_format = analysis['sub_format']

                # Save to format-specific CSV
                csv_path = save_format_specific_csv(
                    extracted_data['runners'],
                    format_type,
                    sub_format,
                    output_dir,
                    filename,
                    track_name
                )
                
                # Update counters
                stats['successful'] += 1
                stats['total_runners'] += runners
                stats['total_races'] += races
                stats['format_counts'][format_type] = stats['format_counts'].get(format_type, 0) + 1
                stats['sub_format_counts'][sub_format] = stats['sub_format_counts'].get(sub_format, 0) + 1

                # Track venues
                for venue in analysis['venues']:
                    stats['venue_counts'][venue] = stats['venue_counts'].get(venue, 0) + 1

                # Add file info for meta_processor compatibility
                stats['files'].append({
                    'filename': filename,
                    'format': format_type,
                    'sub_format': sub_format,
                    'runners': runners,
                    'races': races,
                    'path': csv_path
                })
                
                print(f"  ✓ SUCCESS: {runners} runners, {races} races ({format_type}/{sub_format} format)")
                if csv_path:
                    print(f"  ✓ Saved to: {os.path.relpath(csv_path, output_dir)}")
                
                # Log detailed analysis for significant extractions
                if runners > 50 or races > 5:
                    logger.info(f"Large extraction: {filename} - {runners} runners, {races} races, venues: {analysis['venues']}")
                
            else:
                stats['failed'] += 1
                error_msg = f"No runners extracted from {filename}"
                stats['errors'].append(error_msg)
                print(f"  ✗ FAILED: No runners extracted")
                logger.warning(error_msg)
                
        except Exception as e:
            stats['failed'] += 1
            error_msg = f"Error processing {filename}: {str(e)}"
            stats['errors'].append(error_msg)
            print(f"  ✗ FAILED: {str(e)}")
            logger.error(error_msg)
            
            # Print full traceback for debugging
            if debug:
                traceback.print_exc()
                logger.error(f"Full traceback for {filename}:", exc_info=True)
        
        stats['processed'] += 1
        
        # Progress update every 10 files
        if stats['processed'] % 10 == 0:
            elapsed = datetime.now() - start_time
            rate = stats['processed'] / elapsed.total_seconds() * 60 if elapsed.total_seconds() > 0 else 0
            print(f"\nProgress: {stats['processed']}/{len(pdf_files)} files processed ({rate:.1f} files/min)")
            print(f"Success: {stats['successful']}, Failed: {stats['failed']}, Skipped: {stats['skipped']}")
            print("-" * 40)
    
    # Final summary
    elapsed = datetime.now() - start_time
    print("\n" + "=" * 80)
    print("NSW BATCH PROCESSING COMPLETE")
    print("=" * 80)
    print(f"Total PDFs found: {len(pdf_files)}")
    print(f"Files processed: {stats['processed']}")
    print(f"Files skipped: {stats['skipped']}")
    print(f"Successful extractions: {stats['successful']}")
    print(f"Failed extractions: {stats['failed']}")
    success_rate = stats['successful']/(stats['successful']+stats['failed'])*100 if (stats['successful']+stats['failed']) > 0 else 0
    print(f"Success rate: {success_rate:.1f}%")
    print(f"Total runners extracted: {stats['total_runners']}")
    print(f"Total races extracted: {stats['total_races']}")
    print(f"Processing time: {elapsed}")
    avg_rate = stats['processed']/elapsed.total_seconds()*60 if elapsed.total_seconds() > 0 else 0
    print(f"Average rate: {avg_rate:.1f} files/minute")
    
    # Format distribution
    if stats['format_counts']:
        print(f"\nFormat distribution:")
        for format_type, count in sorted(stats['format_counts'].items()):
            print(f"  {format_type}: {count} files")
    
    # NSW sub-format distribution
    if stats['sub_format_counts']:
        print(f"\nNSW sub-format distribution:")
        for sub_format, count in sorted(stats['sub_format_counts'].items()):
            print(f"  {sub_format}: {count} files")
    
    # Top venues
    if stats['venue_counts']:
        print(f"\nTop venues processed:")
        sorted_venues = sorted(stats['venue_counts'].items(), key=lambda x: x[1], reverse=True)
        for venue, count in sorted_venues[:10]:
            print(f"  {venue}: {count} files")
        if len(sorted_venues) > 10:
            print(f"  ... and {len(sorted_venues)-10} more venues")
    
    # Error summary
    if stats['errors']:
        print(f"\nErrors encountered ({len(stats['errors'])}):")
        for error in stats['errors'][:10]:  # Show first 10 errors
            print(f"  - {error}")
        if len(stats['errors']) > 10:
            print(f"  ... and {len(stats['errors'])-10} more errors")
    
    # Save detailed analysis results
    if stats['analyses']:
        results_df = pd.DataFrame(stats['analyses'])
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = os.path.join(output_dir, f"nsw_processing_results_{timestamp}.csv")
        results_df.to_csv(results_file, index=False)
        print(f"\nDetailed analysis saved to: {os.path.basename(results_file)}")
    
    print(f"\nExtracted CSV files saved to format-specific folders in: {output_dir}")
    print("Folder structure:")
    print("  pj/ - All PJ format files (standard Penrith and alternate Tamworth formats)")
    print("  triples/ - All TripleS format files (PE files with comprehensive sectional timing)")
    print("  unknown/ - Unidentified format files")
    print("=" * 80)
    
    logger.info(f"NSW batch processing complete - {stats['successful']} successful, {stats['failed']} failed")
    return stats

def main():
    """Main function with command line options"""
    parser = argparse.ArgumentParser(description="NSW PDF Batch Processor")
    parser.add_argument('--sample', type=int, metavar='N', 
                       help='Process only first N files for testing')
    parser.add_argument('--debug', action='store_true',
                       help='Show full error tracebacks and detailed logging')
    
    args = parser.parse_args()
    
    print("NSW PDF Batch Processor")
    print("=" * 40)
    if args.sample:
        print(f"SAMPLE MODE: Processing first {args.sample} files only")
    if args.debug:
        print("DEBUG MODE: Full error details will be shown")
    print("")
    
    print("This script supports NSW formats:")
    print("1. PJ formats (Standard Penrith and alternate Tamworth) -> saved to pj/ folder")
    print("2. TripleS format (PE files with sectional data) -> saved to triples/ folder")
    print("")
    
    print("Starting batch processing of NSW PDFs...")
    print("")
    
    stats = process_all_nsw_pdfs(sample_size=args.sample, debug=args.debug)
    
    # Return stats for potential further use
    return stats

if __name__ == "__main__":
    main()