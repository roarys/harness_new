#!/usr/bin/env python3
"""
Unified State Processor Module
Provides a consistent interface for processing PDFs from all states using the specialized process_all_{state} logic
"""

import os
import sys
import glob
import traceback
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import logging

# Import the PDF extractor
from .pdf_extractor import PDFExtractor

class StateProcessor:
    """Unified processor for all state PDF/XLS files"""

    def __init__(self, base_dir: Optional[str] = None):
        """Initialize the state processor"""
        if base_dir is None:
            base_dir = str(Path(__file__).parent.parent)
        self.base_dir = Path(base_dir)
        self.extractor = PDFExtractor()

        # Track mapping for all states (consolidated from all process_all files)
        self.track_mappings = {
            'vic': {
                'BA': 'Ballarat',
                'BN': 'Bendigo',
                'CR': 'Cranbourne',
                'GE': 'Geelong',
                'KI': 'Kilmore',
                'ML': 'Melton',
                'MX': 'Melton',
                'JV': 'Juneville',
                'WA': 'Warragul',
                'AR': 'Ararat',
                'HA': 'Hamilton',
                'ST': 'Stawell',
                'SH': 'Shepparton',
                'YA': 'Yarra Valley',
                'AL': 'Alexandra',
                'EC': 'Echuca',
                'HO': 'Horsham',
                'SW': 'Swan Hill'
            },
            'nsw': {
                'PE': 'Penrith',
                'TA': 'Tamworth',
                'WA': 'Wagga',
                'BA': 'Bathurst',
                'DU': 'Dubbo',
                'NE': 'Newcastle',
                'ME': 'Menangle',
                'YO': 'Young'
            },
            'qld': {
                'AL': 'Albion Park',
                'RE': 'Redcliffe',
                'CA': 'Capalaba',
                'MA': 'Marburg',
                'RO': 'Rockhampton'
            },
            'sa': {
                'GL': 'Globe Derby',
                'GA': 'Gawler',
                'KP': 'Kapunda',
                'PT': 'Port Pirie',
                'VI': 'Victor Harbor'
            },
            'tas': {
                'HO': 'Hobart',
                'LA': 'Launceston',
                'DE': 'Devonport',
                'CA': 'Carrick',
                'BR': 'Brighton'
            }
        }

    def process_state_files(self, state: str, input_dir: str, output_dir: str,
                           logger: Optional[logging.Logger] = None) -> Dict:
        """
        Process all PDF/XLS files for a specific state using the actual process_all_{state} functions

        Args:
            state: State code (nsw, vic, qld, sa, tas, wa)
            input_dir: Directory containing input files
            output_dir: Directory for output CSVs
            logger: Optional logger instance

        Returns:
            Dictionary with processing statistics
        """
        if logger is None:
            logger = logging.getLogger(__name__)

        logger.info(f"Processing {state.upper()} files using process_all_{state} function")

        # Call the actual process_all_{state} functions with all their specialized logic
        try:
            if state == 'vic':
                from scrapers.process_all_vic import process_all_vic_pdfs
                return process_all_vic_pdfs(input_dir=input_dir, output_dir=output_dir, logger=logger)
            elif state == 'nsw':
                from scrapers.process_all_nsw import process_all_nsw_pdfs
                return process_all_nsw_pdfs(input_dir=input_dir, output_dir=output_dir, logger=logger)
            elif state == 'qld':
                from scrapers.process_all_qld import process_all_qld_pdfs
                return process_all_qld_pdfs(input_dir=input_dir, output_dir=output_dir, logger=logger)
            elif state == 'sa':
                from scrapers.process_all_sa import process_all_sa_pdfs
                return process_all_sa_pdfs(input_dir=input_dir, output_dir=output_dir, logger=logger)
            elif state == 'tas':
                from scrapers.process_all_tas import process_all_tas_pdfs
                return process_all_tas_pdfs(input_dir=input_dir, output_dir=output_dir, logger=logger)
            elif state == 'wa':
                from scrapers.process_all_wa import process_all_wa_files
                return process_all_wa_files(input_dir=input_dir, output_dir=output_dir, logger=logger)
            else:
                logger.error(f"Unknown state: {state}")
                return {'processed': 0, 'successful': 0, 'failed': 0}
        except Exception as e:
            logger.error(f"Error calling process_all_{state}: {e}")
            logger.info(f"Falling back to default processing for {state}")
            return self._fallback_processing(state, input_dir, output_dir, logger)

    def _fallback_processing(self, state: str, input_dir: str, output_dir: str,
                           logger: Optional[logging.Logger] = None) -> Dict:
        """
        Fallback processing method when process_all_{state} functions aren't available or compatible

        This is the original processing logic from StateProcessor
        """
        if logger is None:
            logger = logging.getLogger(__name__)

        logger.info(f"Using fallback processing for {state.upper()} files")
        logger.info(f"Input directory: {input_dir}")
        logger.info(f"Output directory: {output_dir}")

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Create format-specific subdirectories
        pj_dir = os.path.join(output_dir, 'pj')
        triples_dir = os.path.join(output_dir, 'triples')
        unknown_dir = os.path.join(output_dir, 'unknown')
        os.makedirs(pj_dir, exist_ok=True)
        os.makedirs(triples_dir, exist_ok=True)
        os.makedirs(unknown_dir, exist_ok=True)

        # Get all files to process
        if state.lower() == 'wa':
            files_to_process = glob.glob(os.path.join(input_dir, "*.xls*"))
            file_type = "XLS"
        else:
            files_to_process = glob.glob(os.path.join(input_dir, "*.pdf"))
            file_type = "PDF"

        files_to_process.sort()  # Process in alphabetical order

        if not files_to_process:
            logger.warning(f"No {file_type} files found in {input_dir}")
            return {'processed': 0, 'successful': 0, 'failed': 0, 'files': []}

        logger.info(f"Found {len(files_to_process)} {file_type} files to process")

        # Initialize statistics
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
            'files': []
        }

        start_time = datetime.now()

        # Process each file
        for i, file_path in enumerate(files_to_process, 1):
            filename = os.path.basename(file_path)
            base_name = os.path.splitext(filename)[0]

            # Check if file already exists in any format folder
            already_exists = False
            for format_dir in ['pj', 'triples', 'unknown']:
                check_path = os.path.join(output_dir, format_dir, f"{base_name}*.csv")
                existing_files = glob.glob(check_path)
                if existing_files:
                    logger.info(f"[{i:3d}/{len(files_to_process)}] SKIPPED: {filename} (already exists in {format_dir}/)")
                    stats['skipped'] += 1
                    already_exists = True
                    break

            if already_exists:
                continue

            logger.info(f"[{i:3d}/{len(files_to_process)}] Processing: {filename}")
            stats['processed'] += 1

            try:
                # Process the file based on type
                if state.lower() == 'wa':
                    extracted_data = self._process_wa_xls(file_path)
                else:
                    extracted_data = self._process_pdf_with_state_logic(file_path, state)

                if extracted_data and extracted_data.get('success'):
                    runners = extracted_data.get('runners', [])

                    if runners:
                        # Ensure track information is present
                        track_name = self._get_track_name(filename, extracted_data, state)
                        if track_name:
                            for runner in runners:
                                if not runner.get('track'):
                                    runner['track'] = track_name

                        # Determine format type
                        format_type = extracted_data.get('format', 'unknown')
                        if 'triples' in format_type.lower():
                            format_dir = triples_dir
                            format_type = 'triples'
                        elif 'pj' in format_type.lower():
                            format_dir = pj_dir
                            format_type = 'pj'
                        else:
                            format_dir = unknown_dir
                            format_type = 'unknown'

                        # Save to CSV
                        df = pd.DataFrame(runners)
                        csv_filename = f"{base_name}.csv"
                        csv_path = os.path.join(format_dir, csv_filename)
                        df.to_csv(csv_path, index=False)

                        # Update statistics
                        stats['successful'] += 1
                        stats['total_runners'] += len(runners)
                        stats['format_counts'][format_type] = stats['format_counts'].get(format_type, 0) + 1

                        # Track venues
                        unique_venues = set(r.get('track', 'Unknown') for r in runners if r.get('track'))
                        for venue in unique_venues:
                            stats['venue_counts'][venue] = stats['venue_counts'].get(venue, 0) + 1

                        stats['files'].append({
                            'filename': filename,
                            'format': format_type,
                            'runners': len(runners),
                            'track': track_name,
                            'csv_path': csv_path
                        })

                        logger.info(f"  ✓ SUCCESS: {len(runners)} runners ({format_type} format)")
                        logger.info(f"  ✓ Saved to: {csv_path}")
                        if track_name:
                            logger.info(f"  ✓ Track: {track_name}")
                    else:
                        logger.warning(f"  ✗ No runners found in {filename}")
                        stats['failed'] += 1
                else:
                    logger.error(f"  ✗ Failed to extract data from {filename}")
                    stats['failed'] += 1
                    if extracted_data:
                        stats['errors'].append({
                            'filename': filename,
                            'error': extracted_data.get('error', 'Unknown error')
                        })

            except Exception as e:
                logger.error(f"  ✗ Error processing {filename}: {str(e)}")
                logger.debug(traceback.format_exc())
                stats['failed'] += 1
                stats['errors'].append({
                    'filename': filename,
                    'error': str(e),
                    'details': traceback.format_exc()
                })

        # Processing complete
        end_time = datetime.now()
        duration = end_time - start_time

        logger.info("\n" + "=" * 80)
        logger.info(f"{state.upper()} PROCESSING COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Total files found: {len(files_to_process)}")
        logger.info(f"Files skipped (already exist): {stats['skipped']}")
        logger.info(f"Files processed: {stats['processed']}")
        logger.info(f"Successful extractions: {stats['successful']}")
        logger.info(f"Failed extractions: {stats['failed']}")
        if stats['processed'] > 0:
            logger.info(f"Success rate: {stats['successful']/stats['processed']*100:.1f}%")
        logger.info(f"Total runners extracted: {stats['total_runners']}")
        logger.info(f"Processing time: {duration}")

        # Format breakdown
        if stats['format_counts']:
            logger.info("\nFormat breakdown:")
            for format_type, count in stats['format_counts'].items():
                logger.info(f"  {format_type}: {count} files")

        # Venue breakdown
        if stats['venue_counts']:
            logger.info("\nVenue breakdown:")
            for venue, count in sorted(stats['venue_counts'].items()):
                logger.info(f"  {venue}: {count} files")

        return stats

    def _process_pdf_with_state_logic(self, file_path: str, state: str) -> Dict:
        """
        Process PDF using state-specific logic from the enhanced extractors

        This uses all the improvements we made in the individual process_all_{state} files
        """
        # Use the enhanced PDF extractor with all our improvements
        extracted_data = self.extractor.extract_pdf_data(file_path)

        # Add state information
        if extracted_data and extracted_data.get('runners'):
            for runner in extracted_data['runners']:
                if not runner.get('state'):
                    runner['state'] = state.upper()

        return extracted_data

    def _process_wa_xls(self, xls_file: str) -> Dict:
        """Process WA XLS file"""
        try:
            df = pd.read_excel(xls_file)

            if df.empty:
                return {'success': False, 'error': 'Empty XLS file'}

            # Convert DataFrame rows to runner dictionaries
            runners = []
            for _, row in df.iterrows():
                runner_data = {
                    'state': 'WA',
                    'source_file': os.path.basename(xls_file)
                }

                # Add all columns from the XLS file
                for col in df.columns:
                    runner_data[col.lower().replace(' ', '_')] = row[col]

                runners.append(runner_data)

            return {
                'success': True,
                'runners': runners,
                'format': 'wa_xls'
            }

        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'format': 'wa_xls'
            }

    def _get_track_name(self, filename: str, extracted_data: Dict, state: str) -> Optional[str]:
        """
        Get track name from various sources

        Priority:
        1. From extracted data runners
        2. From metadata
        3. From filename using state-specific mappings
        """
        # Check if runners already have track info
        runners = extracted_data.get('runners', [])
        if runners and runners[0].get('track'):
            return runners[0]['track']

        # Check metadata
        metadata = extracted_data.get('metadata', {})
        if metadata.get('venue'):
            return metadata['venue']
        if metadata.get('track'):
            return metadata['track']

        # Extract from filename
        base_name = os.path.splitext(filename)[0]

        # Try state-specific track mappings
        state_mappings = self.track_mappings.get(state.lower(), {})

        # Handle various filename patterns
        if '_' in base_name:
            potential_track = base_name.split('_')[0]
            # Extract just letters if mixed with numbers
            import re
            letters_match = re.match(r'^([A-Za-z]+)', potential_track)
            if letters_match:
                potential_track = letters_match.group(1)
        else:
            # Extract letters from start of filename
            import re
            match = re.match(r'^([A-Za-z]+)', base_name)
            potential_track = match.group(1) if match else base_name

        # Check if it's an abbreviation
        track_name = state_mappings.get(potential_track, potential_track)

        # Validate track name
        if track_name and track_name.replace(' ', '').isalpha() and 3 <= len(track_name) <= 25:
            return track_name

        return None

    def _process_wa_files(self, input_dir: str, output_dir: str, logger: Optional[logging.Logger] = None) -> Dict:
        """Process WA XLS files"""
        if logger is None:
            logger = logging.getLogger(__name__)

        results = {
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'files': [],
            'format_counts': {'wa_xls': 0}
        }

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        wa_dir = os.path.join(output_dir, 'wa_xls')
        os.makedirs(wa_dir, exist_ok=True)

        # Get all XLS files
        xls_files = glob.glob(os.path.join(input_dir, "*.xls*"))

        for xls_file in xls_files:
            try:
                logger.info(f"Processing WA file: {os.path.basename(xls_file)}")
                df = pd.read_excel(xls_file)

                if not df.empty:
                    csv_filename = os.path.splitext(os.path.basename(xls_file))[0] + '.csv'
                    csv_path = os.path.join(wa_dir, csv_filename)
                    df.to_csv(csv_path, index=False)

                    results['processed'] += 1
                    results['successful'] += 1
                    results['format_counts']['wa_xls'] += 1
                    results['files'].append({
                        'filename': os.path.basename(xls_file),
                        'format': 'wa_xls',
                        'csv': csv_filename,
                        'runners': len(df)
                    })
                    logger.info(f"  ✓ Processed {os.path.basename(xls_file)}: {len(df)} rows")
                else:
                    results['failed'] += 1
                    logger.warning(f"  ✗ Empty file: {os.path.basename(xls_file)}")
            except Exception as e:
                results['failed'] += 1
                logger.error(f"  ✗ Error processing {os.path.basename(xls_file)}: {e}")

        return results