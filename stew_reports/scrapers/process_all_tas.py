#!/usr/bin/env python3
"""
Batch process all TAS PDFs and extract to CSV files with format-based organization.
This script will process all PDFs in data/raw/tas and save extracted data to format-specific folders
in data/processed/tas
"""

import os
import sys
import traceback
import glob
from datetime import datetime
from pdf_extractor import PDFExtractor

def process_all_tas_pdfs(input_dir=None, output_dir=None, logger=None, force_reprocess=True):
    """Process all TAS PDFs and extract to format-specific CSV folders

    Args:
        input_dir: Input directory path (optional)
        output_dir: Output directory path (optional)
        logger: Logger instance (optional)
        force_reprocess: If True, reprocess files even if they already exist (default: True for meta_processor)
    """

    # Initialize extractor
    extractor = PDFExtractor()

    # Define paths - use defaults if not provided
    if input_dir is None:
        input_dir = "/Users/rorypearson/Desktop/harness_db/stew_reports/data/raw/tas"
    if output_dir is None:
        output_dir = "/Users/rorypearson/Desktop/harness_db/stew_reports/data/processed/tas"

    # Setup logger if not provided
    if logger is None:
        import logging
        logger = logging.getLogger(__name__)
    
    # Find all PDF files in TAS folder
    pdf_files = glob.glob(os.path.join(input_dir, "*.pdf"))
    pdf_files.sort()  # Process in alphabetical order
    
    if not pdf_files:
        print(f"No PDF files found in {input_dir}")
        return
    
    print(f"Found {len(pdf_files)} TAS PDF files to process")
    print(f"Input directory: {input_dir}")
    print(f"Output directory: {output_dir}")
    print("=" * 80)
    
    # Ensure base output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Track progress
    processed = 0
    successful = 0
    failed = 0
    skipped = 0
    total_runners = 0
    total_races = 0
    format_counts = {}
    errors = []
    
    start_time = datetime.now()
    
    for i, pdf_file in enumerate(pdf_files, 1):
        filename = os.path.basename(pdf_file)
        output_filename = filename.replace('.pdf', '_extracted.csv')
        
        # Check if file already exists in any format folder
        if not force_reprocess:
            already_exists = False
            existing_location = None
            for format_type in ['pj', 'triples', 'triples_detailed', 'unknown']:
                format_output_path = os.path.join(output_dir, format_type, output_filename)
                if os.path.exists(format_output_path):
                    print(f"[{i:3d}/{len(pdf_files)}] SKIPPED: {filename} (already exists in {format_type}/)")
                    already_exists = True
                    existing_location = format_type
                    skipped += 1
                    break

            if already_exists:
                continue
        
        print(f"[{i:3d}/{len(pdf_files)}] Processing: {filename}")
        
        try:
            # Extract data
            extracted_data = extractor.extract_pdf_data(pdf_file)
            
            if extracted_data and 'runners' in extracted_data and len(extracted_data['runners']) > 0:
                runners = len(extracted_data['runners'])
                races = len(set(runner['race_number'] for runner in extracted_data['runners'] if 'race_number' in runner))
                format_type = extracted_data.get('format', 'unknown')
                
                # Create format-specific subdirectory
                format_output_dir = os.path.join(output_dir, format_type)
                os.makedirs(format_output_dir, exist_ok=True)
                
                # Update output path to use format-specific directory
                format_output_path = os.path.join(format_output_dir, output_filename)
                
                # Export to CSV (preserving original TAS column names)
                extractor.export_to_csv(extracted_data, format_output_path)
                
                # Update counters
                successful += 1
                total_runners += runners
                total_races += races
                format_counts[format_type] = format_counts.get(format_type, 0) + 1
                
                print(f"  ✓ SUCCESS: {runners} runners, {races} races ({format_type} format)")
                print(f"  ✓ Saved to: {format_type}/{output_filename}")
                
            else:
                failed += 1
                error_msg = f"No runners extracted from {filename}"
                errors.append(error_msg)
                print(f"  ✗ FAILED: No runners extracted")
                
        except Exception as e:
            failed += 1
            error_msg = f"Error processing {filename}: {str(e)}"
            errors.append(error_msg)
            print(f"  ✗ FAILED: {str(e)}")
            
            # Print full traceback for debugging
            if "--debug" in sys.argv:
                traceback.print_exc()
        
        processed += 1
        
        # Progress update every 10 files
        if processed % 10 == 0:
            elapsed = datetime.now() - start_time
            rate = processed / elapsed.total_seconds() * 60  # files per minute
            print(f"\nProgress: {processed}/{len(pdf_files)} files processed ({rate:.1f} files/min)")
            print(f"Success: {successful}, Failed: {failed}, Skipped: {skipped}")
            print("-" * 40)
    
    # Final summary
    elapsed = datetime.now() - start_time
    print("\n" + "=" * 80)
    print("TAS BATCH PROCESSING COMPLETE")
    print("=" * 80)
    print(f"Total PDFs found: {len(pdf_files)}")
    print(f"Files processed: {processed}")
    print(f"Files skipped: {skipped}")
    print(f"Successful extractions: {successful}")
    print(f"Failed extractions: {failed}")
    print(f"Success rate: {successful/(successful+failed)*100:.1f}%" if (successful+failed) > 0 else "N/A")
    print(f"Total runners extracted: {total_runners}")
    print(f"Total races extracted: {total_races}")
    print(f"Processing time: {elapsed}")
    print(f"Average rate: {processed/elapsed.total_seconds()*60:.1f} files/minute" if elapsed.total_seconds() > 0 else "N/A")
    
    # Format distribution
    if format_counts:
        print(f"\nFormat distribution:")
        for format_type, count in sorted(format_counts.items()):
            print(f"  {format_type}: {count} files")
    
    # Error summary
    if errors:
        print(f"\nErrors encountered ({len(errors)}):")
        for error in errors[:10]:  # Show first 10 errors
            print(f"  - {error}")
        if len(errors) > 10:
            print(f"  ... and {len(errors)-10} more errors")
    
    print(f"\nExtracted CSV files saved to format-specific folders in: {output_dir}")
    print("Folder structure:")
    print("  pj/ - PJ format files")
    print("  triples/ - TripleS format files")
    print("  triples_detailed/ - Detailed TripleS format files")
    print("  unknown/ - Unidentified format files")
    print("=" * 80)

    # Return results dictionary for meta_processor
    return {
        'processed': processed,
        'successful': successful,
        'failed': failed,
        'skipped': skipped,
        'total_runners': total_runners,
        'total_races': total_races,
        'files': [],  # Could populate this with file details if needed
        'format_counts': format_counts,
        'errors': errors
    }

def main():
    """Main function with command line options"""
    
    if len(sys.argv) > 1:
        if sys.argv[1] in ['-h', '--help']:
            print("TAS PDF Batch Processor")
            print("Usage: python process_all_tas.py [--debug]")
            print("")
            print("Options:")
            print("  --debug    Show full error tracebacks")
            print("  -h, --help Show this help message")
            print("")
            print("This script will:")
            print("- Process all PDF files in data/raw/tas")
            print("- Extract sectional times data with preserved column names")
            print("- Save CSV files to format-specific folders in data/processed/tas")
            print("  (pj/, triples/, triples_detailed/, unknown/)")
            print("- Skip files that already exist (to resume interrupted runs)")
            print("- Properly extract First 100m timing data where available")
            return
    
    print("TAS PDF Batch Processor")
    print("Starting batch processing of all TAS PDFs...")
    print("Use --debug flag to see detailed error messages")
    print("")
    
    process_all_tas_pdfs()

if __name__ == "__main__":
    main()