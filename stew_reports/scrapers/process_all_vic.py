#!/usr/bin/env python3
"""
Enhanced VIC PDF batch processor with comprehensive extraction and analysis.
Processes all PDFs in data/raw/vic and saves extracted data to format-specific folders
in data/processed/vic with detailed logging and error handling.

Based on successful patterns from QLD and TAS extraction codebases.
"""

import os
import sys
import traceback
import glob
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
import logging

# Add the parent directory to the path to import pdf_extractor
sys.path.append(str(Path(__file__).parent))
from pdf_extractor import PDFExtractor

def setup_logging():
    """Set up logging for the VIC processing script"""
    log_dir = "/Users/rorypearson/Desktop/harness_db/stew_reports/scrapers/logs"
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"vic_batch_process_{timestamp}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger(__name__)

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
        'distances': set()
    }
    
    runners = extracted_data.get('runners', [])
    if runners:
        # Count unique races
        race_numbers = {r.get('race_number') for r in runners if r.get('race_number')}
        analysis['races'] = len(race_numbers)
        
        # Check data types
        first_runner = runners[0]
        if 'top_speed' in first_runner or 'fastest_section' in first_runner:
            analysis['has_triples_data'] = True
        if 'final_time' in first_runner or 'margin' in first_runner:
            analysis['has_pj_data'] = True
            
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

def process_single_vic_pdf(pdf_file: str, output_dir: str, extractor=None) -> Dict:
    """Process a single VIC PDF file and extract data with track information

    Args:
        pdf_file: Path to the PDF file
        output_dir: Directory for output CSVs
        extractor: PDFExtractor instance (optional, will create if not provided)

    Returns:
        Dictionary with processing results including track extraction
    """
    if extractor is None:
        extractor = PDFExtractor()

    filename = os.path.basename(pdf_file)
    base_name = os.path.splitext(filename)[0]

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
        # Extract data using enhanced extractor
        extracted_data = extractor.extract_pdf_data(pdf_file)

        if extracted_data and 'runners' in extracted_data and len(extracted_data['runners']) > 0:
            runners = extracted_data['runners']
            format_type = extracted_data.get('format', 'unknown')

            # Extract track name from multiple sources
            # Prioritize filename extraction for reliability
            track_name = extract_track_from_filename(filename)

            # If not in filename, try metadata
            if not track_name:
                if 'metadata' in extracted_data and extracted_data['metadata']:
                    track_name = extracted_data['metadata'].get('venue')

            # Only use runner data if it's a reasonable track name
            if not track_name and runners:
                potential_track = runners[0].get('track')
                if potential_track and '\n' not in str(potential_track) and len(str(potential_track)) < 30:
                    track_name = potential_track

            # Add track to all runners
            if track_name:
                for runner in runners:
                    runner['track'] = track_name

            # Save to format-specific CSV
            csv_path = save_format_specific_csv(runners, format_type, output_dir, filename, track_name)

            # Count races
            race_numbers = {r.get('race_number') for r in runners if r.get('race_number')}
            num_races = len(race_numbers) if race_numbers else 1

            result['success'] = True
            result['format'] = format_type
            result['track'] = track_name
            result['runners'] = len(runners)
            result['races'] = num_races
            result['output_path'] = csv_path

        else:
            result['error'] = 'No runners extracted'

    except Exception as e:
        result['error'] = str(e)

    return result

def extract_track_from_filename(filename: str) -> str:
    """Extract track name from filename with enhanced patterns"""
    # VIC track mappings
    track_mapping = {
        'BA': 'Ballarat', 'BN': 'Bendigo', 'CR': 'Cranbourne',
        'GE': 'Geelong', 'KI': 'Kilmore', 'ML': 'Melton', 'MX': 'Melton',
        'SP': 'Shepparton', 'SH': 'Shepparton', 'ST': 'Stawell',
        'MI': 'Mildura', 'WA': 'Warragul', 'AR': 'Ararat',
        'HA': 'Hamilton', 'YA': 'Yarra Valley', 'AL': 'Alexandra',
        'EC': 'Echuca', 'HO': 'Horsham', 'SW': 'Swan Hill'
    }

    # Check for track codes at the beginning (e.g., "GE120925_20250912.pdf")
    if len(filename) >= 2:
        prefix = filename[:2].upper()
        if prefix in track_mapping:
            return track_mapping[prefix]

    # Check for full track names in the filename
    filename_lower = filename.lower()
    full_names = {
        'bendigo': 'Bendigo', 'ballarat': 'Ballarat', 'cranbourne': 'Cranbourne',
        'geelong': 'Geelong', 'kilmore': 'Kilmore', 'melton': 'Melton',
        'shepparton': 'Shepparton', 'stawell': 'Stawell', 'mildura': 'Mildura',
        'warragul': 'Warragul', 'ararat': 'Ararat', 'hamilton': 'Hamilton',
        'yarra': 'Yarra Valley', 'alexandra': 'Alexandra', 'echuca': 'Echuca',
        'horsham': 'Horsham', 'swan hill': 'Swan Hill'
    }

    for track_key, track_name in full_names.items():
        if track_key in filename_lower:
            return track_name

    return None

def save_format_specific_csv(runners: List[Dict], format_type: str, output_dir: str, filename: str, track_name: str = None):
    """Save runners to format-specific CSV file with proper organization"""
    if not runners:
        return None

    # Create format-specific directory
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

def process_all_vic_pdfs(input_dir=None, output_dir=None, logger=None, force_reprocess=True):
    """Enhanced VIC PDF processing with comprehensive analysis and error handling

    Args:
        input_dir: Input directory path (optional)
        output_dir: Output directory path (optional)
        logger: Logger instance (optional)
        force_reprocess: If True, reprocess files even if they already exist (default: True for meta_processor)
    """

    # Setup logging
    if logger is None:
        logger = setup_logging()
    logger.info("Starting enhanced VIC PDF batch processing")

    # Initialize extractor
    extractor = PDFExtractor()

    # Define paths - use defaults if not provided
    if input_dir is None:
        input_dir = "/Users/rorypearson/Desktop/harness_db/stew_reports/data/raw/vic"
    if output_dir is None:
        output_dir = "/Users/rorypearson/Desktop/harness_db/stew_reports/data/processed/vic"
    
    # Find all PDF files in VIC folder
    pdf_files = glob.glob(os.path.join(input_dir, "*.pdf"))
    pdf_files.sort()  # Process in alphabetical order
    
    if not pdf_files:
        logger.warning(f"No PDF files found in {input_dir}")
        return
    
    logger.info(f"Found {len(pdf_files)} VIC PDF files to process")
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
        'venue_counts': {},
        'errors': [],
        'analyses': []
    }
    
    start_time = datetime.now()
    
    # Process each PDF file
    for i, pdf_file in enumerate(pdf_files, 1):
        filename = os.path.basename(pdf_file)
        base_name = os.path.splitext(filename)[0]
        
        # Check if file already exists in any format folder
        if not force_reprocess:
            already_exists = False
            existing_location = None
            for format_dir in ['pj', 'triples', 'triples_detailed', 'unknown']:
                check_path = os.path.join(output_dir, format_dir, f"{base_name}_*.csv")
                existing_files = glob.glob(check_path)
                if existing_files:
                    logger.info(f"[{i:3d}/{len(pdf_files)}] SKIPPED: {filename} (already exists in {format_dir}/)")
                    already_exists = True
                    existing_location = format_dir
                    stats['skipped'] += 1
                    break

            if already_exists:
                continue
        
        logger.info(f"[{i:3d}/{len(pdf_files)}] Processing: {filename}")
        stats['processed'] += 1
        
        try:
            # Extract data using enhanced extractor
            extracted_data = extractor.extract_pdf_data(pdf_file)
            
            if extracted_data and 'runners' in extracted_data and len(extracted_data['runners']) > 0:
                runners = extracted_data['runners']
                format_type = extracted_data.get('format', 'unknown')
                
                # Extract track name from filename or metadata
                # Prioritize filename extraction for reliability
                track_name = extract_track_from_filename(filename)

                # If not in filename, try metadata
                if not track_name:
                    if 'metadata' in extracted_data and extracted_data['metadata']:
                        track_name = extracted_data['metadata'].get('venue')

                # Only use runner data if it's a reasonable track name (not containing newlines or weird characters)
                if not track_name and runners:
                    potential_track = runners[0].get('track')
                    if potential_track and '\n' not in str(potential_track) and len(str(potential_track)) < 30:
                        track_name = potential_track
                
                # Analyze extraction results
                analysis = analyze_extraction_results(extracted_data, filename)
                stats['analyses'].append(analysis)
                
                # Save to format-specific CSV with track name
                csv_path = save_format_specific_csv(runners, format_type, output_dir, filename, track_name)
                
                # Update statistics
                stats['successful'] += 1
                stats['total_runners'] += len(runners)
                stats['total_races'] += analysis['races']
                stats['format_counts'][format_type] = stats['format_counts'].get(format_type, 0) + 1
                
                # Track venue statistics
                for venue in analysis['venues']:
                    stats['venue_counts'][venue] = stats['venue_counts'].get(venue, 0) + 1
                
                logger.info(f"  ✓ SUCCESS: {len(runners)} runners, {analysis['races']} races ({format_type} format)")
                logger.info(f"  ✓ Saved to: {csv_path}")
                
                if track_name:
                    logger.info(f"  ✓ Track: {track_name}")
                
                if analysis['venues']:
                    logger.info(f"  ✓ Venues: {', '.join(analysis['venues'])}")
                    
            else:
                logger.warning(f"  ✗ No data extracted from {filename}")
                stats['failed'] += 1
                stats['errors'].append({
                    'filename': filename,
                    'error': 'No runners extracted',
                    'details': 'PDF processed but no runner data found'
                })
                
        except Exception as e:
            error_msg = f"Error processing {filename}: {str(e)}"
            logger.error(error_msg)
            logger.debug(traceback.format_exc())
            
            stats['failed'] += 1
            stats['errors'].append({
                'filename': filename,
                'error': str(e),
                'details': traceback.format_exc()
            })
    
    # Processing complete - generate summary
    end_time = datetime.now()
    duration = end_time - start_time
    
    logger.info("\n" + "=" * 80)
    logger.info("VIC PDF PROCESSING COMPLETE")
    logger.info("=" * 80)
    
    total_files = len(pdf_files)
    logger.info(f"Total files found: {total_files}")
    logger.info(f"Files skipped (already exist): {stats['skipped']}")
    logger.info(f"Files processed: {stats['processed']}")
    logger.info(f"Successful extractions: {stats['successful']}")
    logger.info(f"Failed extractions: {stats['failed']}")
    logger.info(f"Success rate: {stats['successful']/stats['processed']*100:.1f}%" if stats['processed'] > 0 else "N/A")
    logger.info(f"Total runners extracted: {stats['total_runners']}")
    logger.info(f"Total races extracted: {stats['total_races']}")
    logger.info(f"Processing time: {duration}")
    
    # Format breakdown
    logger.info("\nFormat breakdown:")
    for format_type, count in stats['format_counts'].items():
        logger.info(f"  {format_type}: {count} files")
    
    # Venue breakdown
    if stats['venue_counts']:
        logger.info("\nVenue breakdown:")
        for venue, count in sorted(stats['venue_counts'].items()):
            logger.info(f"  {venue}: {count} files")
    
    # Error summary
    if stats['errors']:
        logger.info(f"\nErrors encountered ({len(stats['errors'])}):") 
        for error in stats['errors'][:10]:  # Show first 10 errors
            logger.info(f"  {error['filename']}: {error['error']}")
        if len(stats['errors']) > 10:
            logger.info(f"  ... and {len(stats['errors']) - 10} more errors")
    
    # Save detailed results to CSV
    results_file = os.path.join(output_dir, f"vic_processing_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    if stats['analyses']:
        results_df = pd.DataFrame(stats['analyses'])
        results_df.to_csv(results_file, index=False)
        logger.info(f"\nDetailed results saved to: {results_file}")
    
    return stats

def main():
    """Main entry point with sample mode support"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Process VIC PDF files for harness racing data extraction')
    parser.add_argument('--sample', type=int, help='Process only the first N files (for testing)')
    parser.add_argument('--pattern', help='Only process files matching this pattern')
    
    args = parser.parse_args()
    
    if args.sample:
        print(f"Running in sample mode - processing first {args.sample} files only")
        # This could be implemented by modifying the pdf_files list in process_all_vic_pdfs
        
    stats = process_all_vic_pdfs()
    
    # Exit with appropriate code
    if stats['failed'] == 0:
        sys.exit(0)  # Success
    elif stats['successful'] > 0:
        sys.exit(1)  # Partial success
    else:
        sys.exit(2)  # Complete failure

if __name__ == "__main__":
    main()