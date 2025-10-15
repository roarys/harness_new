#!/usr/bin/env python3
"""
Batch process all WA XLS files and convert them to CSV format.
This script will process all XLS files in data/raw/wa and save converted data to data/processed/wa

The script reads WA harness racing XLS files with the following structure:
- Date, Location, ClubCode, Race No, TAB No, Horse, Country, Place, Margin, 
  800Mar, 400Mar, 800Width, 400Width, Time, 800T, 400T

And converts them to CSV format (direct conversion - no column mapping):
"""

import os
import sys
import traceback
import glob
import pandas as pd
from datetime import datetime
from pathlib import Path


def process_wa_xls_file(xls_file: str, output_path: str) -> tuple:
    """
    Process a single WA XLS file and convert to CSV format (direct conversion)
    
    Args:
        xls_file: Path to the input XLS file
        output_path: Path for the output CSV file
    
    Returns:
        tuple: (success: bool, rows_count: int, error_msg: str)
    """
    try:
        # Read the XLS file directly
        df = pd.read_excel(xls_file)
        
        if df.empty:
            return False, 0, "Empty XLS file"
        
        # Write directly to CSV (no column mapping or processing)
        df.to_csv(output_path, index=False)
        
        return True, len(df), ""
        
    except Exception as e:
        return False, 0, str(e)


def process_all_wa_files(input_dir=None, output_dir=None, logger=None, force_reprocess=True):
    """Process all WA XLS files and convert to CSV format

    Args:
        input_dir: Input directory path (optional)
        output_dir: Output directory path (optional)
        logger: Logger instance (optional)
        force_reprocess: If True, reprocess files even if they already exist (default: True for meta_processor)
    """

    # Define paths - use defaults if not provided
    if input_dir is None:
        input_dir = "/Users/rorypearson/Desktop/harness_db/stew_reports/data/raw/wa"
    if output_dir is None:
        output_dir = "/Users/rorypearson/Desktop/harness_db/stew_reports/data/processed/wa/pj"

    # Setup logger if not provided
    if logger is None:
        import logging
        logger = logging.getLogger(__name__)
    
    # Find all XLS files in WA folder
    xls_files = []
    for ext in ['*.xls', '*.xlsx']:
        xls_files.extend(glob.glob(os.path.join(input_dir, ext)))
    
    # Sort files for consistent processing order
    xls_files.sort()
    
    # Filter out progress files and non-racing files
    xls_files = [f for f in xls_files if not os.path.basename(f).startswith('.') and 
                 not 'progress' in os.path.basename(f).lower()]
    
    if not xls_files:
        print(f"No XLS files found in {input_dir}")
        return
    
    print(f"Found {len(xls_files)} WA XLS files to process")
    print(f"Input directory: {input_dir}")
    print(f"Output directory: {output_dir}")
    print("=" * 80)
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Track progress
    processed = 0
    successful = 0
    failed = 0
    skipped = 0
    total_rows = 0
    errors = []
    
    start_time = datetime.now()
    
    for i, xls_file in enumerate(xls_files, 1):
        filename = os.path.basename(xls_file)
        
        # Generate output filename (no timestamp)
        base_name = filename.replace('.xlsx', '').replace('.xls', '')
        output_filename = f"{base_name}.csv"
        output_path = os.path.join(output_dir, output_filename)
        
        # Check if already processed
        if os.path.exists(output_path) and not force_reprocess:
            print(f"[{i:3d}/{len(xls_files)}] SKIPPED: {filename} (already processed)")
            skipped += 1
            continue
        elif os.path.exists(output_path) and force_reprocess:
            print(f"[{i:3d}/{len(xls_files)}] REPROCESSING: {filename} (force mode)")
        
        print(f"[{i:3d}/{len(xls_files)}] Processing: {filename}")
        
        try:
            # Process the XLS file
            success, rows, error_msg = process_wa_xls_file(xls_file, output_path)
            
            if success:
                successful += 1
                total_rows += rows
                
                print(f"  ✓ SUCCESS: {rows} rows")
                print(f"  ✓ Saved to: {output_filename}")
            else:
                failed += 1
                error_info = f"File: {filename}, Error: {error_msg}"
                errors.append(error_info)
                print(f"  ✗ FAILED: {error_msg}")
                
                # Remove failed output file if it exists
                if os.path.exists(output_path):
                    os.remove(output_path)
            
        except Exception as e:
            failed += 1
            error_info = f"File: {filename}, Exception: {str(e)}"
            errors.append(error_info)
            print(f"  ✗ EXCEPTION: {str(e)}")
            
            # Remove failed output file if it exists
            if os.path.exists(output_path):
                os.remove(output_path)
        
        processed += 1
        
        # Progress update every 10 files
        if processed % 10 == 0 or processed == len(xls_files):
            elapsed = datetime.now() - start_time
            rate = processed / elapsed.total_seconds() * 60 if elapsed.total_seconds() > 0 else 0
            print(f"  Progress: {processed}/{len(xls_files)} files ({rate:.1f} files/min)")
    
    # Final summary
    elapsed_time = datetime.now() - start_time
    
    print("\n" + "=" * 80)
    print("WA XLS PROCESSING COMPLETED")
    print("=" * 80)
    print(f"Files processed: {processed:,}")
    print(f"Successful: {successful:,}")
    print(f"Failed: {failed:,}")
    print(f"Skipped: {skipped:,}")
    print(f"Total rows processed: {total_rows:,}")
    print(f"Processing time: {elapsed_time}")
    print(f"Average rate: {processed / elapsed_time.total_seconds() * 60:.1f} files/min")
    
    if errors:
        print(f"\nErrors encountered ({len(errors)}):")
        for error in errors[:10]:  # Show first 10 errors
            print(f"  - {error}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more errors")
    
    
    if successful > 0:
        print(f"\n✅ Processing completed successfully!")
        print(f"Processed CSV files are available in: {output_dir}")
    else:
        print(f"\n⚠️ No files were successfully processed")

    # Return results dictionary for meta_processor
    return {
        'processed': processed,
        'successful': successful,
        'failed': failed,
        'skipped': skipped,
        'total_rows': total_rows,
        'files': [],  # Could populate this with file details if needed
        'format_counts': {'wa_xls': successful} if successful > 0 else {}
    }


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Process WA harness racing XLS files and convert them to CSV format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all WA XLS files (default)
  python process_all_wa.py
  
  # Make executable and run
  chmod +x process_all_wa.py
  ./process_all_wa.py
  
  # Check help
  python process_all_wa.py --help

Input:  /Users/rorypearson/Desktop/harness_db/stew_reports/data/raw/wa/*.xls
Output: /Users/rorypearson/Desktop/harness_db/stew_reports/data/processed/wa/pj/*.csv

The script performs direct XLS to CSV conversion with no column mapping.
        """
    )
    
    parser.add_argument('--force', action='store_true',
                       help='Force reprocessing of already processed files')
    
    args = parser.parse_args()
    
    try:
        print("WA XLS TO CSV PROCESSOR")
        print("=" * 50)
        print()
        
        if args.force:
            print("⚠️ Force mode enabled - will reprocess all files")
            print()
        
        process_all_wa_files(force_reprocess=args.force)
        
    except KeyboardInterrupt:
        print("\n\n🛑 Processing interrupted by user")
    except Exception as e:
        print(f"\n❌ Error occurred: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()