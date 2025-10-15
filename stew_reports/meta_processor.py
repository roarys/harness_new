#!/usr/bin/env python3
"""
Meta Processor for Australian Harness Racing Sectional Data

This script coordinates the complete workflow:
1. Scrapes PDFs from specified states and date ranges
2. Processes PDFs to extract sectional data
3. Consolidates all extracted data into a single DataFrame

Usage:
    python meta_processor.py --states nsw vic --dates 2024-01-01 2024-01-07
    python meta_processor.py --states all --days-back 7
"""

import os
import sys
import shutil
import glob
import pandas as pd
import logging
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import json
import traceback
import gc
import psutil

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent))

# Import scrapers
from scrapers.nsw_scraper import NSWScraper
from scrapers.vic_scraper import VICScraper
from scrapers.qld_scraper import QLDScraper
from scrapers.sa_scraper import SAScraper
from scrapers.tas_scraper import TASScraper
from scrapers.wa_scraper import WAComprehensiveScraper as WAScraper

# Import processors - these will be called directly
import scrapers.process_all_vic
import scrapers.process_all_qld
import scrapers.process_all_sa
import scrapers.process_all_tas
import scrapers.process_all_wa
import scrapers.process_all_nsw


# Import State Processor (uses enhanced process_all_{state} logic)
from scrapers.state_processor import StateProcessor

# Import format cleaners and mergers
try:
    from format_cleaners import (
        # Individual state/format cleaners
        clean_nsw_pj, clean_nsw_triples,
        clean_vic_pj, clean_vic_triples,
        clean_qld_pj, clean_qld_triples,
        clean_sa_pj,
        clean_tas_pj,
        clean_wa_pj,
        # Master merge functions
        merge_all_pj_states,
        merge_all_triples_states,
        master_merge_pjs_and_triples
    )
    CLEANERS_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Could not import format_cleaners: {e}")
    CLEANERS_AVAILABLE = False

class MetaProcessor:
    """Coordinates scraping, processing, and consolidation of harness racing data"""
    
    def __init__(self, base_dir: str = None):
        """Initialize the meta processor"""
        if base_dir is None:
            base_dir = str(Path(__file__).parent)
        self.base_dir = Path(base_dir)

        # Create directory structure
        self.processing_dir = self.base_dir / "processing"
        self.processed_dir = self.base_dir / "processed"
        self.data_dir = self.base_dir / "data"
        self.cleaned_dir = self.base_dir / "cleaned"  # For format-specific cleaned data
        self.merged_dir = self.base_dir / "merged"    # For final merged data

        # Ensure directories exist
        self.processing_dir.mkdir(exist_ok=True)
        self.processed_dir.mkdir(exist_ok=True)
        self.data_dir.mkdir(exist_ok=True)
        self.cleaned_dir.mkdir(exist_ok=True)
        self.merged_dir.mkdir(exist_ok=True)
        
        # Setup logging
        self.setup_logging()
        
        # Initialize scrapers
        self.scrapers = {
            'nsw': NSWScraper,
            'vic': VICScraper,
            'qld': QLDScraper,
            'sa': SAScraper,
            'tas': TASScraper,
            'wa': WAScraper
        }
        
        # Initialize processors - we'll handle these directly
        self.processors = {
            'nsw': 'process_all_nsw',
            'vic': 'process_all_vic',
            'qld': 'process_all_qld',
            'sa': 'process_all_sa',
            'tas': 'process_all_tas',
            'wa': 'process_all_wa'
        }
        # Check if cleaners are available
        self.cleaners_available = CLEANERS_AVAILABLE

        # Initialize the state processor with enhanced logic
        self.state_processor = StateProcessor(base_dir=self.base_dir)
    
    def setup_logging(self):
        """Setup logging configuration"""
        log_dir = self.base_dir / "logs"
        log_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"meta_processor_{timestamp}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        self.logger = logging.getLogger(__name__)
    
    def scrape_state_data(self, state: str, dates: Optional[List[str]] = None,
                         days_back: Optional[int] = None) -> Dict:
        """
        Scrape PDFs for a specific state and date range

        Args:
            state: State code (nsw, vic, qld, sa, tas, wa)
            dates: List of dates in YYYY-MM-DD format
            days_back: Number of days to go back from today

        Returns:
            Dictionary with scraping results
        """
        self.logger.info(f"Starting scraping for {state.upper()}")

        # Create state-specific processing directory
        state_processing_dir = self.processing_dir / state
        state_processing_dir.mkdir(exist_ok=True)

        # Initialize scraper with base directory (it will create data/raw/{state} structure)
        if state not in self.scrapers:
            self.logger.error(f"Scraper not available for state: {state}")
            return {'success': False, 'error': f'Scraper not available for {state}'}

        scraper_class = self.scrapers[state]
        # Pass the processing directory as base_dir so scraper creates data/raw/{state} under it
        scraper = scraper_class(str(state_processing_dir))

        # Prepare date list
        target_dates = []
        if dates is not None:
            target_dates = dates
        elif days_back:
            # Generate dates for the past N days
            for i in range(days_back):
                date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
                target_dates.append(date)
        else:
            # Default to today only
            print('No dates or days_back provided, defaulting to today only')
            target_dates = [datetime.now().strftime("%Y-%m-%d")]

        # Aggregate results from all dates
        aggregate_results = {
            'success': True,
            'total_found': 0,
            'downloads': {'successful': 0, 'failed': 0, 'skipped': 0},
            'files': [],
            'dates_processed': []
        }

        # Scrape each date using the new date-specific methods
        for target_date in target_dates:
            self.logger.info(f"Scraping {state.upper()} for date: {target_date}")

            try:
                # All scrapers now have scrape_specific_date method
                if hasattr(scraper, 'scrape_specific_date'):
                    results = scraper.scrape_specific_date(target_date)
                else:
                    # Fallback for any scraper that doesn't have the new method yet
                    self.logger.warning(f"Using fallback scraping for {state} - date-specific method not available")
                    # For backwards compatibility, use the old methods
                    if state == 'tas':
                        results = scraper.scrape_date_range(target_date, target_date)
                    elif state in ['nsw']:
                        # Convert to days_back for NSW
                        date_obj = datetime.strptime(target_date, "%Y-%m-%d")
                        days_diff = (datetime.now() - date_obj).days
                        results = scraper.scrape_date_range(days_back=1, start_from_date=date_obj.strftime('%d/%m/%Y'))
                    else:
                        # Skip if no date-specific method available
                        self.logger.warning(f"Skipping {state} for {target_date} - no date-specific scraping available")
                        continue

                # Aggregate results
                if results.get('success', False):
                    aggregate_results['total_found'] += results.get('total_found', 0)
                    downloads = results.get('downloads', {})
                    aggregate_results['downloads']['successful'] += downloads.get('successful', 0)
                    aggregate_results['downloads']['failed'] += downloads.get('failed', 0)
                    aggregate_results['downloads']['skipped'] += downloads.get('skipped', 0)

                    # Add files with date information
                    for file_info in results.get('files', []):
                        file_info['scrape_date'] = target_date
                        aggregate_results['files'].append(file_info)

                    aggregate_results['dates_processed'].append(target_date)

            except Exception as e:
                self.logger.error(f"Error scraping {state} for {target_date}: {str(e)}")
                aggregate_results['success'] = False

        self.logger.info(f"Scraping completed for {state}: {aggregate_results['total_found']} files found across {len(aggregate_results['dates_processed'])} dates")
        return aggregate_results
    
    def process_state_pdfs(self, state: str, session_files: List[str] = None) -> Dict:
        """
        Process PDFs for a specific state using the enhanced StateProcessor

        Args:
            state: State code
            session_files: Optional list of specific files to process (from current scraping session)

        Returns:
            Dictionary with processing results including format breakdown
        """
        self.logger.info(f"Starting processing for {state.upper()}")

        state_processing_dir = self.processing_dir / state
        state_processed_dir = self.processed_dir / state
        state_processed_dir.mkdir(exist_ok=True)

        # If session files are specified, create a temporary directory with only those files
        temp_processing_dir = None
        if session_files:
            # Create session-specific temporary directory
            session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
            temp_processing_dir = self.processing_dir / f"session_{session_id}_{state}"
            temp_processing_dir.mkdir(exist_ok=True)

            # Copy only the session files to the temp directory
            files_copied = 0
            for filename in session_files:
                source_paths = [
                    state_processing_dir / filename,
                    state_processing_dir / "data" / "raw" / state / filename,
                    state_processing_dir / "raw" / state / filename,
                ]

                for source_path in source_paths:
                    if source_path.exists():
                        dest_path = temp_processing_dir / filename
                        shutil.copy2(source_path, dest_path)
                        files_copied += 1
                        self.logger.debug(f"Copied {filename} to session directory")
                        break

            self.logger.info(f"Copied {files_copied}/{len(session_files)} session files to temporary processing directory")

            # Use the temp directory for processing
            actual_processing_dir = temp_processing_dir
        else:
            # No session files specified, use regular directory (backwards compatibility)
            actual_processing_dir = state_processing_dir

        # First, move PDFs and XLS files from nested structure to flat structure if they exist
        nested_dirs_to_check = [
            actual_processing_dir / "data" / "raw" / state,  # Standard nested structure
            actual_processing_dir / "raw" / state,           # WA nested structure
        ]

        for nested_dir in nested_dirs_to_check:
            if nested_dir.exists():
                # Move PDF files
                nested_pdfs = list(nested_dir.glob("*.pdf"))
                for nested_pdf in nested_pdfs:
                    flat_path = actual_processing_dir / nested_pdf.name
                    if not flat_path.exists():
                        nested_pdf.rename(flat_path)
                        self.logger.info(f"Moved {nested_pdf.name} to flat structure")

                # Move XLS files (for WA)
                nested_xls = list(nested_dir.glob("*.xls*"))
                for nested_file in nested_xls:
                    flat_path = actual_processing_dir / nested_file.name
                    if not flat_path.exists():
                        nested_file.rename(flat_path)
                        self.logger.info(f"Moved {nested_file.name} to flat structure")

        # Clean up nested directory structures after moving files
        nested_dirs_to_clean = [
            actual_processing_dir / "data",
            actual_processing_dir / "raw",
        ]

        for nested_data_dir in nested_dirs_to_clean:
            if nested_data_dir.exists():
                try:
                    shutil.rmtree(nested_data_dir)
                    self.logger.info(f"Cleaned up nested directory structure for {state}")
                except Exception as e:
                    self.logger.warning(f"Could not clean up nested directory: {e}")

        # Use the StateProcessor with enhanced process_all_{state} logic
        try:
            results = self.state_processor.process_state_files(
                state=state,
                input_dir=str(actual_processing_dir),  # Use the actual (possibly temp) directory
                output_dir=str(state_processed_dir),
                logger=self.logger
            )

            # Clean up temp directory if we created one
            if session_files and temp_processing_dir and temp_processing_dir.exists():
                try:
                    shutil.rmtree(temp_processing_dir)
                    self.logger.info(f"Cleaned up temporary session directory")
                except Exception as e:
                    self.logger.warning(f"Could not clean up temp directory: {e}")

            # Ensure results is a valid dictionary
            if results is None:
                self.logger.error(f"StateProcessor returned None for {state}")
                results = {'processed': 0, 'successful': 0, 'failed': 0, 'files': []}
            elif not isinstance(results, dict):
                self.logger.error(f"StateProcessor returned invalid type for {state}: {type(results)}")
                results = {'processed': 0, 'successful': 0, 'failed': 0, 'files': []}

        except Exception as e:
            self.logger.error(f"Error in StateProcessor for {state}: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            results = {'processed': 0, 'successful': 0, 'failed': 1, 'files': [], 'error': str(e)}

        # Move processed files to their respective directories (maintaining backward compatibility)
        if results and results.get('files'):
            for file_info in results['files']:
                try:
                    source_file = state_processing_dir / file_info['filename']
                    if source_file.exists():
                        # Determine destination based on format
                        format_type = file_info.get('format', 'unknown')
                        if 'triples' in format_type.lower():
                            dest_dir = state_processed_dir / 'triples'
                        elif 'pj' in format_type.lower():
                            dest_dir = state_processed_dir / 'pj'
                        else:
                            dest_dir = state_processed_dir / 'unknown'

                        dest_dir.mkdir(exist_ok=True)
                        dest_file = dest_dir / file_info['filename']

                        # Move the original file to processed directory
                        if not dest_file.exists():
                            shutil.move(str(source_file), str(dest_file))
                            self.logger.debug(f"Moved {file_info['filename']} to {dest_dir.name}/")
                except Exception as e:
                    self.logger.warning(f"Could not move {file_info['filename']}: {e}")

        return results

    def merge_all_formats(self, states: List[str], chunk_size: int = None) -> Dict:
        """
        New merging workflow:
        1. Load raw CSVs for each state/format
        2. Pass state CSVs to merge_all_pj_states and merge_all_triples_states
        3. Pass the two merged dataframes to master_merge_pjs_and_triples

        Args:
            states: List of state codes

        Returns:
            Dictionary with merging results
        """
        self.logger.info("Starting complete format merging workflow")

        if not self.cleaners_available:
            self.logger.error("Format cleaners not available. Please ensure format_cleaners.py is properly imported.")
            return {'success': False, 'error': 'Format cleaners not available'}

        results = {
            'success': True,
            'loaded_states': {},
            'format_merged': {'pj': False, 'triples': False},
            'final_merged': False,
            'errors': []
        }

        # Step 1: Load all state/format CSVs into dictionaries
        state_pj_dfs = {}
        state_triples_dfs = {}

        for state in states:
            state_processed_dir = self.processed_dir / state
            if not state_processed_dir.exists():
                self.logger.warning(f"No processed directory for {state}")
                continue

            results['loaded_states'][state] = {'pj': 0, 'triples': 0}

            # Load PJ format CSVs - state-specific directory handling
            csv_files = []

            # Standard location for most states (recent processing)
            pj_dir = state_processed_dir / 'pj'
            if pj_dir.exists():
                csv_files.extend(list(pj_dir.glob("*.csv")))

            # Special handling for states with historical data issues
            if state == 'sa' and not csv_files:
                # Only use alternative location if no recent data
                alt_pj_dir = self.data_dir / 'processed' / state / 'pj'
                if alt_pj_dir.exists():
                    csv_files.extend(list(alt_pj_dir.glob("*.csv")))

            if state == 'wa' and not csv_files:
                # Only check WA root directory if no pj folder data
                if state_processed_dir.exists():
                    wa_files = list(state_processed_dir.glob("*.csv"))
                    csv_files.extend([f for f in wa_files if 'WA_' in f.name])

            if csv_files:
                    state_pj_list = []
                    total_files = len(csv_files)
                    self.logger.info(f"Loading {total_files} PJ files for {state.upper()}")

                    for i, csv_file in enumerate(csv_files, 1):
                        try:
                            # Progress logging for large datasets
                            interval = getattr(self, 'progress_interval', 10)
                            if i % interval == 0 or i == total_files:
                                self.logger.info(f"  Loading {state} PJ file {i}/{total_files}: {csv_file.name}")

                            # Memory check during processing
                            if hasattr(self, 'memory_limit') and i % (interval * 2) == 0:
                                current_memory = psutil.virtual_memory().percent
                                if current_memory > self.memory_limit:
                                    self.logger.warning(f"Memory usage {current_memory:.1f}% exceeds limit {self.memory_limit}%")
                                    self.logger.info("Forcing garbage collection...")
                                    gc.collect()

                            # Optimized CSV reading with low memory usage
                            try:
                                df = pd.read_csv(csv_file, low_memory=False)
                            except Exception:
                                # Fallback to python engine for problematic files
                                df = pd.read_csv(csv_file, engine='python', low_memory=False)

                            # Only add essential columns to reduce memory
                            df['source_file'] = csv_file.name
                            state_pj_list.append(df)
                        except Exception as e:
                            self.logger.error(f"Error reading {csv_file}: {e}")

                    if state_pj_list:
                        # Efficient concatenation with memory optimization
                        self.logger.info(f"Concatenating {len(state_pj_list)} dataframes for {state} PJ data...")
                        state_pj_dfs[state] = pd.concat(state_pj_list, ignore_index=True, copy=False)

                        # Clear the list to free memory
                        del state_pj_list

                        results['loaded_states'][state]['pj'] = len(state_pj_dfs[state])
                        self.logger.info(f"✓ Loaded {state} PJ data: {len(state_pj_dfs[state])} rows from {total_files} files")

            # Load TripleS format CSVs - state-specific directory handling (same as PJ)
            csv_files = []

            # Standard location for most states (recent processing)
            triples_dir = state_processed_dir / 'triples'
            if triples_dir.exists():
                csv_files.extend(list(triples_dir.glob("*.csv")))

            # Special handling for states with historical data issues
            if state == 'sa' and not csv_files:
                # Only use alternative location if no recent data
                alt_triples_dir = self.data_dir / 'processed' / state / 'triples'
                if alt_triples_dir.exists():
                    csv_files.extend(list(alt_triples_dir.glob("*.csv")))

            if csv_files:
                    state_triples_list = []
                    total_files = len(csv_files)
                    self.logger.info(f"Loading {total_files} TripleS files for {state.upper()}")

                    for i, csv_file in enumerate(csv_files, 1):
                        try:
                            # Progress logging for large datasets
                            interval = getattr(self, 'progress_interval', 10)
                            if i % interval == 0 or i == total_files:
                                self.logger.info(f"  Loading {state} TripleS file {i}/{total_files}: {csv_file.name}")

                            # Optimized CSV reading
                            try:
                                df = pd.read_csv(csv_file, low_memory=False)
                            except Exception:
                                df = pd.read_csv(csv_file, engine='python', low_memory=False)

                            df['source_file'] = csv_file.name
                            state_triples_list.append(df)
                        except Exception as e:
                            self.logger.error(f"Error reading {csv_file}: {e}")

                    if state_triples_list:
                        # Efficient concatenation with memory optimization
                        self.logger.info(f"Concatenating {len(state_triples_list)} dataframes for {state} TripleS data...")
                        state_triples_dfs[state] = pd.concat(state_triples_list, ignore_index=True, copy=False)

                        # Clear the list to free memory
                        del state_triples_list

                        results['loaded_states'][state]['triples'] = len(state_triples_dfs[state])
                        self.logger.info(f"✓ Loaded {state} TripleS data: {len(state_triples_dfs[state])} rows from {total_files} files")

        # Step 2: Call merge_all_pj_states with state dataframes
        merged_pj_df = pd.DataFrame()
        if state_pj_dfs:
            try:
                self.logger.info(f"Merging PJ data from {len(state_pj_dfs)} states")

                # Prepare arguments in the order expected by merge_all_pj_states
                df_vic_pj = state_pj_dfs.get('vic', pd.DataFrame())
                df_qld_pj = state_pj_dfs.get('qld', pd.DataFrame())
                df_sa_pj = state_pj_dfs.get('sa', pd.DataFrame())
                df_tas_pj = state_pj_dfs.get('tas', pd.DataFrame())
                df_nsw_pj = state_pj_dfs.get('nsw', pd.DataFrame())
                df_wa_pj = state_pj_dfs.get('wa', pd.DataFrame())

                # Memory monitoring
                memory_before = psutil.virtual_memory().percent
                self.logger.info(f"About to call merge_all_pj_states with: VIC={len(df_vic_pj)}, QLD={len(df_qld_pj)}, SA={len(df_sa_pj)}, TAS={len(df_tas_pj)}, NSW={len(df_nsw_pj)}, WA={len(df_wa_pj)}")
                self.logger.info(f"Memory usage before merge: {memory_before:.1f}%")

                # Force garbage collection before intensive operation
                gc.collect()

                start_time = datetime.now()
                merged_pj_df = merge_all_pj_states(df_vic_pj, df_qld_pj, df_sa_pj, df_tas_pj, df_nsw_pj, df_wa_pj)
                end_time = datetime.now()

                memory_after = psutil.virtual_memory().percent
                duration = end_time - start_time
                self.logger.info(f"✓ merge_all_pj_states completed in {duration} - Memory: {memory_after:.1f}%")

                # Clean up individual state dataframes to free memory
                del df_vic_pj, df_qld_pj, df_sa_pj, df_tas_pj, df_nsw_pj, df_wa_pj
                gc.collect()

                # Save merged PJ data
                pj_output = self.merged_dir / "all_states_pj_merged.csv"
                merged_pj_df.to_csv(pj_output, index=False)
                self.logger.info(f"Saved merged PJ data: {len(merged_pj_df)} rows")
                results['format_merged']['pj'] = True
            except Exception as e:
                self.logger.error(f"Error merging PJ states: {e}")
                results['errors'].append(str(e))

        # Step 3: Call merge_all_triples_states with state dataframes
        merged_triples_df = pd.DataFrame()
        if state_triples_dfs:
            try:
                self.logger.info(f"Merging TripleS data from {len(state_triples_dfs)} states")

                # Prepare arguments in the order expected by merge_all_triples_states
                df_nsw_triples = state_triples_dfs.get('nsw', pd.DataFrame())
                df_vic_triples = state_triples_dfs.get('vic', pd.DataFrame())
                df_qld_triples = state_triples_dfs.get('qld', pd.DataFrame())
                df_sa_triples = state_triples_dfs.get('sa', pd.DataFrame())
                df_tas_triples = state_triples_dfs.get('tas', pd.DataFrame())
                df_wa_triples = state_triples_dfs.get('wa', pd.DataFrame())

                merged_triples_df = merge_all_triples_states(df_nsw_triples, df_vic_triples, df_qld_triples, df_sa_triples, df_tas_triples, df_wa_triples)

                # Save merged TripleS data
                triples_output = self.merged_dir / "all_states_triples_merged.csv"
                merged_triples_df.to_csv(triples_output, index=False)
                self.logger.info(f"Saved merged TripleS data: {len(merged_triples_df)} rows")
                results['format_merged']['triples'] = True
            except Exception as e:
                self.logger.error(f"Error merging TripleS states: {e}")
                results['errors'].append(str(e))

        # Step 4: Final merge of both formats using master_merge_pjs_and_triples
        if (not merged_pj_df.empty or not merged_triples_df.empty):
            try:
                self.logger.info("Performing final merge of PJ and TripleS formats")
                final_df = master_merge_pjs_and_triples(merged_pj_df, merged_triples_df)

                # Save final merged data
                final_output = self.merged_dir / "final_merged_data.csv"
                final_df.to_csv(final_output, index=False)
                self.logger.info(f"Saved final merged data: {len(final_df)} rows")
                results['final_merged'] = True
            except Exception as e:
                self.logger.error(f"Error in final merge: {e}")
                results['errors'].append(str(e))

        return results

    def clean_and_merge_state_data(self, state: str) -> Dict:
        """
        Clean format-specific data and merge for a single state

        Args:
            state: State code

        Returns:
            Dictionary with cleaning and merging results
        """
        self.logger.info(f"Starting cleaning and merging for {state.upper()}")

        state_processed_dir = self.processed_dir / state
        state_cleaned_dir = self.cleaned_dir / state
        state_cleaned_dir.mkdir(exist_ok=True)

        results = {
            'success': True,
            'cleaned': {'pj': 0, 'triples': 0},
            'merged': False,
            'errors': []
        }

        # Process each format
        for format_type in ['pj', 'triples']:
            format_dir = state_processed_dir / format_type
            if not format_dir.exists():
                self.logger.warning(f"No {format_type} directory for {state}")
                continue

            csv_files = list(format_dir.glob("*.csv"))
            if not csv_files:
                self.logger.info(f"No {format_type} CSV files for {state}")
                continue

            # Consolidate format-specific files
            format_dfs = []
            for csv_file in csv_files:
                try:
                    df = pd.read_csv(csv_file)
                    df['source_file'] = csv_file.name
                    df['format'] = format_type
                    format_dfs.append(df)
                except Exception as e:
                    self.logger.error(f"Error reading {csv_file}: {e}")
                    results['errors'].append(str(e))

            if format_dfs:
                # Combine all files for this format
                format_df = pd.concat(format_dfs, ignore_index=True)

                # Apply format-specific cleaning if available
                cleaner = self.format_cleaners.get(state, {}).get(format_type)
                if cleaner and callable(cleaner):
                    try:
                        self.logger.info(f"Applying {state} {format_type} cleaner")
                        format_df = cleaner(format_df)
                        results['cleaned'][format_type] = len(format_df)
                    except Exception as e:
                        self.logger.error(f"Error applying {state} {format_type} cleaner: {e}")
                        results['errors'].append(str(e))
                else:
                    self.logger.info(f"No cleaner defined for {state} {format_type}, using raw data")
                    results['cleaned'][format_type] = len(format_df)

                # Save cleaned format-specific data
                output_file = state_cleaned_dir / f"{state}_{format_type}_cleaned.csv"
                format_df.to_csv(output_file, index=False)
                self.logger.info(f"Saved {len(format_df)} rows to {output_file}")

        # Merge formats for this state if merger is available
        merger = self.state_mergers.get(state)
        if merger and callable(merger):
            try:
                pj_file = state_cleaned_dir / f"{state}_pj_cleaned.csv"
                triples_file = state_cleaned_dir / f"{state}_triples_cleaned.csv"

                pj_df = pd.read_csv(pj_file) if pj_file.exists() else pd.DataFrame()
                triples_df = pd.read_csv(triples_file) if triples_file.exists() else pd.DataFrame()

                if not pj_df.empty or not triples_df.empty:
                    self.logger.info(f"Applying {state} merger")
                    merged_df = merger(pj_df, triples_df)

                    # Save merged state data
                    merged_file = self.merged_dir / f"{state}_merged.csv"
                    merged_df.to_csv(merged_file, index=False)
                    self.logger.info(f"Saved merged {state} data: {len(merged_df)} rows")
                    results['merged'] = True
            except Exception as e:
                self.logger.error(f"Error merging {state} data: {e}")
                results['errors'].append(str(e))
        else:
            self.logger.info(f"No merger defined for {state}, keeping formats separate")

        return results

    def consolidate_data(self, states: List[str]) -> pd.DataFrame:
        """
        Consolidate all merged or cleaned data into a single DataFrame

        Args:
            states: List of state codes to consolidate

        Returns:
            Consolidated DataFrame
        """
        self.logger.info(f"Consolidating data for states: {states}")

        # First check for final merged file (highest priority)
        final_merged_file = self.merged_dir / "final_merged_data.csv"
        if final_merged_file.exists():
            try:
                df = pd.read_csv(final_merged_file)
                self.logger.info(f"Loaded final merged data: {len(df)} rows")
                return df
            except Exception as e:
                self.logger.error(f"Error reading final merged file: {e}")

        # Check for format-level merged files (second priority)
        format_dfs = []
        pj_merged_file = self.merged_dir / "all_states_pj_merged.csv"
        if pj_merged_file.exists():
            try:
                df = pd.read_csv(pj_merged_file)
                format_dfs.append(df)
                self.logger.info(f"Loaded merged PJ data: {len(df)} rows")
            except Exception as e:
                self.logger.error(f"Error reading merged PJ file: {e}")

        triples_merged_file = self.merged_dir / "all_states_triples_merged.csv"
        if triples_merged_file.exists():
            try:
                df = pd.read_csv(triples_merged_file)
                format_dfs.append(df)
                self.logger.info(f"Loaded merged TripleS data: {len(df)} rows")
            except Exception as e:
                self.logger.error(f"Error reading merged TripleS file: {e}")

        if format_dfs:
            consolidated_df = pd.concat(format_dfs, ignore_index=True)
            self.logger.info(f"Consolidated format-level merged data: {len(consolidated_df)} rows")
            return consolidated_df

        # Fallback to state-level data (legacy approach)
        all_dfs = []

        for state in states:
            # First check for merged data
            merged_file = self.merged_dir / f"{state}_merged.csv"
            if merged_file.exists():
                try:
                    df = pd.read_csv(merged_file)
                    if 'state' not in df.columns:
                        df['state'] = state.upper()
                    df['source'] = 'merged'
                    all_dfs.append(df)
                    self.logger.info(f"Loaded merged data for {state}: {len(df)} rows")
                    continue
                except Exception as e:
                    self.logger.error(f"Error reading merged file for {state}: {e}")

            # If no merged data, check for cleaned format-specific data
            state_cleaned_dir = self.cleaned_dir / state
            if state_cleaned_dir.exists():
                for format_type in ['pj', 'triples']:
                    cleaned_file = state_cleaned_dir / f"{state}_{format_type}_cleaned.csv"
                    if cleaned_file.exists():
                        try:
                            df = pd.read_csv(cleaned_file)
                            if 'state' not in df.columns:
                                df['state'] = state.upper()
                            df['source'] = f'cleaned_{format_type}'
                            all_dfs.append(df)
                            self.logger.info(f"Loaded cleaned {format_type} data for {state}: {len(df)} rows")
                        except Exception as e:
                            self.logger.error(f"Error reading cleaned {format_type} file for {state}: {e}")

            # Fallback to raw processed data if no cleaned/merged data
            if not any(df['state'].iloc[0].lower() == state.upper() for df in all_dfs if isinstance(df, pd.DataFrame)):
                state_processed_dir = self.processed_dir / state
                if state_processed_dir.exists():
                    for format_type in ['pj', 'triples', 'unknown']:
                        format_dir = state_processed_dir / format_type
                        if format_dir.exists():
                            csv_files = list(format_dir.glob("*.csv"))
                            for csv_file in csv_files:
                                try:
                                    df = pd.read_csv(csv_file)
                                    if 'state' not in df.columns:
                                        df['state'] = state.upper()
                                    df['source'] = f'raw_{format_type}'
                                    df['source_file'] = csv_file.name
                                    all_dfs.append(df)
                                except Exception as e:
                                    self.logger.error(f"Error reading {csv_file}: {e}")

                    if any(df['state'].iloc[0].lower() == state.lower() if 'state' in df.columns and len(df) > 0 else False for df in all_dfs if isinstance(df, pd.DataFrame)):
                        self.logger.info(f"Loaded raw processed data for {state}")

        if all_dfs:
            consolidated_df = pd.concat(all_dfs, ignore_index=True)
            self.logger.info(f"Consolidated {len(consolidated_df)} rows from {len(all_dfs)} sources")
            return consolidated_df
        else:
            self.logger.warning("No data to consolidate")
            return pd.DataFrame()
    
    def run(self, states: List[str], dates: Optional[List[str]] = None,
            days_back: Optional[int] = None, skip_scraping: bool = False,
            skip_processing: bool = False, skip_cleaning: bool = False,
            memory_limit: float = 80.0, progress_interval: int = 10) -> pd.DataFrame:
        """
        Run the complete workflow

        Args:
            states: List of state codes or ['all'] for all states
            dates: List of dates in YYYY-MM-DD format
            days_back: Number of days to go back from today
            skip_scraping: Skip the scraping step (use existing files)
            skip_processing: Skip the processing step (use existing CSVs)
            skip_cleaning: Skip the cleaning and merging step (use existing cleaned/merged data)
            memory_limit: Stop processing if memory usage exceeds this percentage (default: 80.0)
            progress_interval: Log progress every N files (default: 10)

        Returns:
            Consolidated DataFrame with all extracted data
        """
        # Handle 'all' states
        if 'all' in states:
            states = list(self.scrapers.keys())

        # Initial memory check
        initial_memory = psutil.virtual_memory().percent
        self.logger.info(f"Starting meta processing for states: {states}")
        self.logger.info(f"Initial memory usage: {initial_memory:.1f}% (limit: {memory_limit}%)")

        if initial_memory > memory_limit:
            self.logger.warning(f"Memory usage ({initial_memory:.1f}%) already exceeds limit ({memory_limit}%)")

        # Store performance settings for use throughout the process
        self.memory_limit = memory_limit
        self.progress_interval = progress_interval
        
        # Step 1: Scrape PDFs
        if not skip_scraping:
            scrape_results = {}
            for state in states:
                self.logger.info(f"\n{'='*50}")
                self.logger.info(f"SCRAPING {state.upper()}")
                self.logger.info(f"{'='*50}")
                scrape_results[state] = self.scrape_state_data(state, dates, days_back)
            
            # Save scraping summary
            summary_file = self.base_dir / "logs" / f"scrape_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(summary_file, 'w') as f:
                json.dump(scrape_results, f, indent=2, default=str)
            self.logger.info(f"Scraping summary saved to {summary_file}")
        
        # Step 2: Process PDFs
        if not skip_processing:
            process_results = {}
            for state in states:
                self.logger.info(f"\n{'='*50}")
                self.logger.info(f"PROCESSING {state.upper()}")
                self.logger.info(f"{'='*50}")

                # Get list of files scraped in this session
                current_session_files = []
                if not skip_scraping and state in scrape_results:
                    current_session_files = [f.get('filename') for f in scrape_results[state].get('files', [])]
                    self.logger.info(f"Processing {len(current_session_files)} files from current scraping session")

                process_results[state] = self.process_state_pdfs(state, session_files=current_session_files)

            # Save processing summary
            summary_file = self.base_dir / "logs" / f"process_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(summary_file, 'w') as f:
                json.dump(process_results, f, indent=2, default=str)
            self.logger.info(f"Processing summary saved to {summary_file}")

        # Step 3: Clean and merge using new workflow
        if not skip_cleaning:
            self.logger.info(f"\n{'='*50}")
            self.logger.info("CLEANING AND MERGING ALL FORMATS")
            self.logger.info(f"{'='*50}")
            merge_results = self.merge_all_formats(states)

            # Save merge summary
            summary_file = self.base_dir / "logs" / f"merge_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(summary_file, 'w') as f:
                json.dump(merge_results, f, indent=2, default=str)
            self.logger.info(f"Merge summary saved to {summary_file}")

        # Step 4: Consolidate data
        self.logger.info(f"\n{'='*50}")
        self.logger.info("CONSOLIDATING DATA")
        self.logger.info(f"{'='*50}")
        consolidated_df = self.consolidate_data(states)
        
        # Save consolidated data
        # if not consolidated_df.empty:
        #     output_file = self.base_dir / f"consolidated_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        #     consolidated_df.to_csv(output_file, index=False)
        #     self.logger.info(f"Consolidated data saved to {output_file}")
        
        return consolidated_df


def main():
    """Main entry point for the meta processor"""
    parser = argparse.ArgumentParser(
        description='Meta processor for harness racing sectional data',
        epilog="""
Examples:
  # Scrape specific dates for NSW and VIC
  python meta_processor.py --states nsw vic --dates 2024-01-15 2024-01-16

  # Scrape last 7 days for all states
  python meta_processor.py --states all --days-back 7

  # Scrape today's data for QLD
  python meta_processor.py --states qld --dates 2024-01-17

  # Process existing PDFs without scraping
  python meta_processor.py --states all --skip-scraping
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('--states', nargs='+', required=True,
                       choices=['nsw', 'vic', 'qld', 'sa', 'tas', 'wa', 'all'],
                       help='States to process (use "all" for all states)')

    parser.add_argument('--dates', nargs='+',
                       help='Specific dates to process (YYYY-MM-DD format)')

    parser.add_argument('--days-back', type=int,
                       help='Number of days to go back from today')

    parser.add_argument('--date-range',
                       help='Date range in format: YYYY-MM-DD:YYYY-MM-DD')

    parser.add_argument('--skip-scraping', action='store_true',
                       help='Skip scraping step (use existing PDFs)')

    parser.add_argument('--skip-processing', action='store_true',
                       help='Skip processing step (use existing CSVs)')

    parser.add_argument('--skip-cleaning', action='store_true',
                       help='Skip cleaning and merging step (use existing cleaned/merged data)')

    parser.add_argument('--output-dir',
                       help='Output directory for results')

    parser.add_argument('--memory-limit', type=float, default=80.0,
                       help='Stop processing if memory usage exceeds this percentage (default: 80)')

    parser.add_argument('--progress-interval', type=int, default=10,
                       help='Log progress every N files (default: 10)')
    
    args = parser.parse_args()

    # Process date arguments
    dates_to_process = args.dates
    if args.date_range:
        # Parse date range
        try:
            start_str, end_str = args.date_range.split(':')
            start_date = datetime.strptime(start_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_str, '%Y-%m-%d')
            dates_to_process = []
            current = start_date
            while current <= end_date:
                dates_to_process.append(current.strftime('%Y-%m-%d'))
                current += timedelta(days=1)
            print(f"Generated {len(dates_to_process)} dates from range {start_str} to {end_str}")
        except Exception as e:
            print(f"Error parsing date range: {e}")
            print("Date range should be in format: YYYY-MM-DD:YYYY-MM-DD")
            return 1

    # Initialize processor
    processor = MetaProcessor(args.output_dir)

    # Run the workflow
    result_df = processor.run(
        states=args.states,
        dates=dates_to_process,
        days_back=args.days_back,
        skip_scraping=args.skip_scraping,
        skip_processing=args.skip_processing,
        skip_cleaning=args.skip_cleaning
    )
    
    # Print summary
    print(f"\n{'='*60}")
    print("PROCESSING COMPLETE")
    print(f"{'='*60}")
    if not result_df.empty:
        print(f"Total rows extracted: {len(result_df)}")
        print(f"States processed: {result_df['state'].unique() if 'state' in result_df.columns else 'N/A'}")
        print(f"Date range: {result_df['date'].min() if 'date' in result_df.columns else 'N/A'} to {result_df['date'].max() if 'date' in result_df.columns else 'N/A'}")
    else:
        print("No data extracted")
    
    return 0


if __name__ == "__main__":
    # sys.exit(main())

    # Test configuration (commented out - uncomment for testing)
    processor = MetaProcessor()
    result_df = processor.run(
        states=['nsw', 'vic', 'qld', 'sa', 'tas', 'wa'],
        dates=None,
        days_back=10,
        skip_scraping=False,
        skip_processing=False,
        skip_cleaning=False,
        memory_limit=80.0,  # Default memory limit
        progress_interval=10  # Default progress logging
    )
    print(result_df.head())
    result_df.to_csv('result_df.csv', index=False)