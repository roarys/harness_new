#!/usr/bin/env python3
"""
PDF Data Extraction for Harness Racing Sectional Times

Supports multiple PDF formats:
- TripleS format (complex tabular data)
- PJ format (simpler layout with position/margin data)

Author: Claude Code
"""

import re
import csv
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd


try:
    import pdfplumber
except ImportError:
    print("Error: pdfplumber not installed. Please run: pip install pdfplumber")
    sys.exit(1)

class PDFExtractor:
    """Extract sectional times data from harness racing PDF files"""
    
    def __init__(self):
        self.supported_formats = ['triples', 'pj']
        
    def detect_format(self, text: str) -> str:
        """
        Detect PDF format based on content analysis
        
        Returns:
            'triples' for TripleS format
            'triples_detailed' for detailed TripleS format (like Redcliffe sub-type)
            'pj' for PJ format
            'unknown' if format cannot be determined
        """
        # Convert to lowercase for case-insensitive matching
        text_lower = text.lower()
        
        # TripleS format indicators - more comprehensive
        triples_indicators = [
            'triples',
            'triplesdata',
            'data processed by triples',
            'fastest section',
            'top speed',
            'distance travelled',
            'gross time',
            'mile rate',
            'horse/driver',
            'first 50m',
            'first 100m',
            'first 200m',
            'first half',
            'middle half',
            'last half',
            'lead time',
            '1st quarter',
            '2nd quarter', 
            '3rd quarter',
            '4th quarter',
            # VIC-specific TripleS indicators
            'gross time:',
            'milerate:',
            'mile rate:',
            'leadtime:',
            'lead time:',
            'first qtr:',
            'second qtr:',
            'third qtr:',
            'fourth qtr:',
            'position and metres gained from 800m',
            'finish position and metres gained'
        ]
        
        # PJ format indicators - more comprehensive  
        pj_indicators = [
            'p j data',
            'pj data',
            'sectionals powered by',
            'tasracing',
            # SA-specific PJ indicators
            'nohorse plc mar',  # Very specific SA PJ header pattern
            'globe derby park',  # SA venue indicator
            '800 posi 400 posi', # SA column header pattern
            # Other PJ indicators
            'plc margin time',
            '800time(w)',
            '400time(w)',
            'first100m',
            'no horse',
            'data table: 800 / 400'
        ]
        
        triples_count = sum(1 for indicator in triples_indicators if indicator in text_lower)
        pj_count = sum(1 for indicator in pj_indicators if indicator in text_lower)
        
        # Check for detailed TripleS format (like Redcliffe sub-type)
        # Make indicators more specific to avoid false positives with VIC files
        detailed_triples_indicators = [
            'driver section 50m 100m 200m mile travelled',  # Very specific to detailed format
            'fastest section',  # Specific detailed field
            'first 50m',       # Specific detailed field
            'first 100m',      # Specific detailed field
            'first 200m',      # Specific detailed field
            'distance travelled', # Specific detailed field
        ]
        
        detailed_count = sum(1 for indicator in detailed_triples_indicators if indicator in text_lower)
        
        print(f"Format detection - TripleS indicators: {triples_count}, PJ indicators: {pj_count}, Detailed TripleS: {detailed_count}")
        
        # Check for detailed TripleS format first (most specific) - require more indicators
        if detailed_count >= 3:
            return 'triples_detailed'
        
        # More lenient thresholds with fallback patterns, prioritize PJ detection
        if pj_count >= 2:
            return 'pj' 
        elif triples_count >= 3:
            return 'triples'
        elif pj_count >= 1 and triples_count < 2:
            return 'pj'
        elif triples_count >= 2:
            return 'triples'
        else:
            # Fallback pattern matching for specific formats
            
            # TripleS format specific patterns
            triples_patterns = [
                r'horse/\s*driver',
                r'km/h.*fastest.*section',
                r'speed.*section.*first.*50m.*100m.*200m',
                r'r\s*t\s*a\s*a\s*n\s*b\s*k',  # The column headers pattern
                r'lead\s*time.*quarter.*quarter.*quarter.*quarter',
                r'\[(\d+)\].*\[(\d+)\].*\[(\d+)\]',  # Position indicators
                r'first\s*half.*middle\s*half.*last\s*half',
                r'margin.*travelled',
                r'fastest\s*section',
                r'first\s*50m',
                r'first\s*100m',
                r'first\s*200m',
                r'middle\s*half',
                r'last\s*half',
                r'distance\s*travelled',
                r'gross\s*time.*mile\s*rate.*travelled',
                r'\d{2}\.\d{2}\s+0:\d{2}\.\d{2}',  # Speed + time pattern
                r'lead\s*q[1-4]',
                r'q[1-4]\s*q[1-4]'
            ]
            
            # PJ format specific patterns  
            pj_patterns = [
                r'plc.*margin.*time.*800time',
                r'finish position and metres gained',
                r'gross time:.*mile.*rate:',
                r'horse.*plc.*margin.*time.*800time.*400time',
                r'metres gained from 800m',
                r'\(\d+\).*\(\d+\)',  # Win indicators pattern
                r'first\s*qtr:.*second\s*qtr:.*third\s*qtr:.*fourth\s*qtr:',
                r'nohorse.*plc.*mar.*800.*posi.*400.*posi',
                r'data\s*table:\s*800\s*/\s*400',
                r'milerate:\s*\d+:\d+\.\d+',
                r'leadtime:\s*\d+\.\d+s',
                r'race\s+\d+\s+distance\s+\d+m',
                r'sunday.*\d+\s+\w+\s+\d{4}',
                r'saturday.*\d+\s+\w+\s+\d{4}',
                r'monday.*\d+\s+\w+\s+\d{4}',
                r'tuesday.*\d+\s+\w+\s+\d{4}',
                r'wednesday.*\d+\s+\w+\s+\d{4}',
                r'thursday.*\d+\s+\w+\s+\d{4}',
                r'friday.*\d+\s+\w+\s+\d{4}'
            ]
            
            triples_pattern_count = sum(1 for pattern in triples_patterns if re.search(pattern, text_lower))
            pj_pattern_count = sum(1 for pattern in pj_patterns if re.search(pattern, text_lower))
            
            print(f"Fallback patterns - TripleS: {triples_pattern_count}, PJ: {pj_pattern_count}")
            
            # Choose format based on higher pattern count
            if triples_pattern_count > pj_pattern_count and triples_pattern_count >= 1:
                return 'triples'
            elif pj_pattern_count > triples_pattern_count and pj_pattern_count >= 1:
                return 'pj'
            elif pj_pattern_count >= 1:  # Tie-breaker: prefer PJ in case of equal counts
                return 'pj'
            elif triples_pattern_count >= 1:
                return 'triples'
            else:
                return 'unknown'
    
    def extract_metadata(self, text: str, format_type: str) -> Dict[str, Any]:
        """Extract race metadata from PDF text"""
        metadata = {
            'venue': None,
            'date': None,
            'race_number': None,
            'distance': None,
            'race_name': None,
            'format': format_type
        }
        
        # Extract venue and date patterns
        if format_type == 'triples':
            # TripleS format: "Cranbourne VIC - C-CLASS" or "Redcliffe QLD"
            venue_match = re.search(r'(\w+(?:\s+\w+)*)\s+(VIC|QLD|NSW|SA|TAS|WA|NT|ACT)\s*-?\s*[A-Z-]*', text, re.IGNORECASE)
            if venue_match:
                metadata['venue'] = f"{venue_match.group(1)} {venue_match.group(2)}"
            
            # Date: "16 April 2023"
            date_match = re.search(r'(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})', text, re.IGNORECASE)
            if date_match:
                metadata['date'] = date_match.group(1)
                
            # Race info: "Race 1: BLUE HILLS RISE PACE - 2080m"
            race_match = re.search(r'Race\s+(\d+):\s*([^-\n]+?)\s*-\s*(\d+)m', text, re.IGNORECASE)
            if race_match:
                metadata['race_number'] = int(race_match.group(1))
                metadata['race_name'] = race_match.group(2).strip()
                metadata['distance'] = int(race_match.group(3))
                
        elif format_type == 'pj':
            # PJ format: "Carrick Race 1 Distance 1670m Saturday, 13 February 2021"
            pj_header_match = re.search(r'(\w+(?:\s+\w+)*)\s+Race\s+(\d+)\s+Distance\s+(\d+)m\s+(\w+,\s*\d{1,2}\s+\w+\s+\d{4})', text, re.IGNORECASE)
            if pj_header_match:
                metadata['venue'] = pj_header_match.group(1)
                metadata['race_number'] = int(pj_header_match.group(2))
                metadata['distance'] = int(pj_header_match.group(3))
                metadata['date'] = pj_header_match.group(4)
            else:
                # TAS 2020 format: Extract venue and date specifically for this format
                # Try "Sectional information Hobart" pattern first (cleanest)
                sectional_match = re.search(r'Sectional information\s+([A-Za-z]+)', text, re.IGNORECASE)
                if sectional_match:
                    metadata['venue'] = sectional_match.group(1)
                
                # Extract date from "Hobart Sunday, 2 August 2020" or standalone date lines  
                date_match = re.search(r'(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+(\d{1,2}\s+[A-Za-z]+\s+\d{4})', text, re.IGNORECASE)
                if date_match:
                    metadata['date'] = f"{date_match.group(1)}, {date_match.group(2)}"
                
                # Extract race numbers and distance from 2020 format: "Race No. 1 Distance 2090m"
                race_2020_match = re.search(r'Race\s+No\.\s+(\d+)\s+Distance\s+(\d+)m', text, re.IGNORECASE)
                if race_2020_match:
                    metadata['race_number'] = int(race_2020_match.group(1))
                    metadata['distance'] = int(race_2020_match.group(2))
        
        return metadata
    
    def extract_triples_data(self, text: str, state: str = None, pdf_obj = None, file_path: str = None) -> List[Dict[str, Any]]:
        """Extract data from TripleS format PDFs using state-specific methods"""
        if not state:
            # Try to determine state from file path first, fallback to text
            if file_path:
                state = self._detect_state_from_path(file_path)
            if not state or state == 'unknown':
                state = self._detect_state_from_text(text)
        
        # Route to state-specific extraction method
        if state == 'qld':
            return self._extract_triples_qld(text, pdf_obj)
        elif state == 'vic':
            return self._extract_triples_vic(text, pdf_obj)
        elif state == 'nsw':
            return self._extract_triples_nsw(text, pdf_obj)
        elif state == 'sa':
            return self._extract_triples_sa(text)
        elif state == 'tas':
            return self._extract_triples_tas(text)
        else:
            # Fallback to generic method
            return self._extract_triples_generic(text)
    
    def _extract_triples_qld(self, text: str, pdf_obj=None) -> List[Dict[str, Any]]:
        """Extract TripleS data specifically for QLD format using table extraction"""
        runners = []
        
        # Try to extract the table structure directly from PDF
        if pdf_obj:
            try:
                return self._extract_triples_qld_from_table(pdf_obj)
            except Exception as e:
                print(f"Table extraction failed: {e}, falling back to text parsing")
                return self._extract_triples_qld_from_text(text)
        else:
            return self._extract_triples_qld_from_text(text)
    
    def _extract_triples_qld_from_table(self, pdf_obj) -> List[Dict[str, Any]]:
        """Extract QLD TripleS data using table extraction - improved from VIC template"""
        import re
        runners = []
        
        # Extract tables from all pages - QLD TripleS PDFs contain multiple races
        for page_num, page in enumerate(pdf_obj.pages):
            tables = page.extract_tables()
            
            if not tables:
                continue
            
            # Use the first table on each page
            table = tables[0]
            
            if len(table) < 2:  # Need at least header + 1 data row
                continue
            
            # Get race information from this page
            page_text = page.extract_text()
            race_info = self._extract_race_info_from_page(page_text, page_num + 1)
            
            # Check if this is the special Redcliffe format (less columns, different order)
            # Special format has 14 columns vs standard 20+ columns
            is_redcliffe_format = len(table[0]) <= 14 and any('Lead\nTime' in str(cell) for cell in table[0] if cell)
            
            # Skip header row and process each data row
            for row_idx, row in enumerate(table[1:], 1):
                # For Redcliffe format, we need at least 14 columns; for standard, at least 15
                min_columns = 14 if is_redcliffe_format else 15
                if not row or len(row) < min_columns:
                    continue
                    
                # Skip rows that don't have rank data (sub-rows)
                if not row[0] or not row[0].isdigit():
                    continue
                
                try:
                    if is_redcliffe_format:
                        # Special Redcliffe format: columns are in different order
                        # Columns: Rank, Tab, Horse, TopSpeed, FastestSection, First50m, First100m, First200m, 
                        #          LeadTime, 1stQuarter, 2ndQuarter, LastMile, DistanceTravelled, GrossTime/Margin
                        runner_data = {
                            'rank': int(row[0]) if row[0] and row[0].isdigit() else None,
                            'tab_number': int(row[1]) if row[1] and row[1].isdigit() else None,
                            'horse_name': row[2].strip() if row[2] else None,
                            'top_speed': float(row[3]) if row[3] and row[3].replace('.', '').isdigit() else None,
                            'fastest_section': row[4] if len(row) > 4 else None,
                            'first_50m': row[5] if len(row) > 5 else None,
                            'first_100m': row[6] if len(row) > 6 else None,
                            'first_200m': row[7] if len(row) > 7 else None,
                            'lead_time': row[8] if len(row) > 8 else None,
                            'quarter_1': row[9] if len(row) > 9 else None,
                            'quarter_2': row[10] if len(row) > 10 else None,
                            'last_mile': row[11] if len(row) > 11 else None,
                            'distance_travelled': row[12] if len(row) > 12 else None,
                            'gross_time_margin': row[13] if len(row) > 13 else None,
                            # Additional fields not in this format
                            'first_half': None,
                            'middle_half': None,
                            'last_half': None,
                            'quarter_3': None,
                            'quarter_4': None,
                            'mile_rate': None,
                            # Add race information
                            'race_number': race_info.get('race_number') if race_info else None,
                            'race_name': race_info.get('race_name') if race_info else None,
                            'distance': race_info.get('distance') if race_info else None,
                            'date': race_info.get('date') if race_info else None,
                            'track': race_info.get('track') if race_info else None,
                            'page_number': page_num + 1
                        }
                    else:
                        # Standard format
                        runner_data = {
                            'rank': int(row[0]) if row[0] and row[0].isdigit() else None,
                            'tab_number': int(row[1]) if row[1] and row[1].isdigit() else None,
                            'horse_name': row[2].strip() if row[2] else None,
                            'top_speed': float(row[3]) if row[3] and row[3].replace('.', '').isdigit() else None,
                            'fastest_section': row[4] if len(row) > 4 else None,
                            'first_50m': row[5] if len(row) > 5 else None,
                            'first_100m': row[6] if len(row) > 6 else None,
                            'first_200m': row[7] if len(row) > 7 else None,
                            'first_half': row[8] if len(row) > 8 else None,
                            'middle_half': row[9] if len(row) > 9 else None,
                            'last_half': row[10] if len(row) > 10 else None,
                            'lead_time': row[11] if len(row) > 11 else None,
                            'quarter_1': row[12] if len(row) > 12 else None,
                            'quarter_2': row[13] if len(row) > 13 else None,
                            'quarter_3': row[14] if len(row) > 14 else None,
                            'quarter_4': row[15] if len(row) > 15 else None,
                            'last_mile': row[16] if len(row) > 16 else None,
                            'mile_rate': row[17] if len(row) > 17 else None,
                            'distance_travelled': row[18] if len(row) > 18 else None,
                            'gross_time_margin': row[19] if len(row) > 19 else None,
                            # Add race information
                            'race_number': race_info.get('race_number') if race_info else None,
                            'race_name': race_info.get('race_name') if race_info else None,
                            'distance': race_info.get('distance') if race_info else None,
                            'date': race_info.get('date') if race_info else None,
                            'track': race_info.get('track') if race_info else None,
                            'page_number': page_num + 1
                        }
                    
                    # Only add runners with essential data
                    if not (runner_data['tab_number'] and runner_data['horse_name']):
                        continue
                        
                except (ValueError, IndexError) as e:
                    print(f"Warning: Error processing QLD TripleS row {row_idx} on page {page_num + 1}: {e}")
                    continue
                
                # Extract quarter positions from quarter data (format: "time [position]")
                for quarter_num in [1, 2, 3, 4]:
                    quarter_data = runner_data[f'quarter_{quarter_num}']
                    if quarter_data:
                        # Extract time and position
                        time_match = re.search(r'(\d:\d{2}\.\d{2})', quarter_data)
                        pos_match = re.search(r'\[(\d+)\]', quarter_data)
                        
                        runner_data[f'quarter_{quarter_num}_time'] = time_match.group(1) if time_match else None
                        runner_data[f'quarter_{quarter_num}_position'] = int(pos_match.group(1)) if pos_match else None
                
                # Extract margin and final time from gross_time_margin
                if runner_data['gross_time_margin']:
                    # Look for margin pattern like "+28m" or "0m"
                    margin_match = re.search(r'(\+?\d+\.?\d*m|0m)', runner_data['gross_time_margin'])
                    time_match = re.search(r'(\d:\d{2}\.\d{2})', runner_data['gross_time_margin'])
                    
                    runner_data['margin'] = margin_match.group(1) if margin_match else None
                    runner_data['final_time'] = time_match.group(1) if time_match else None
                
                # Extract lead time and position
                if runner_data['lead_time']:
                    lead_match = re.match(r'(\d:\d{2}\.\d{2})\s*\[(\d+)\]', runner_data['lead_time'])
                    if lead_match:
                        runner_data['lead_time_value'] = lead_match.group(1)
                        runner_data['lead_position'] = int(lead_match.group(2))
                
                # Find driver name in subsequent rows (look ahead in table)
                driver_name = None
                if is_redcliffe_format:
                    # In Redcliffe format, driver name is typically in row+2, column 2
                    # Pattern: Main row -> Q2/Q3 row -> Driver row
                    if row_idx + 2 < len(table):
                        driver_row = table[row_idx + 2]
                        if driver_row and len(driver_row) > 2 and driver_row[2]:
                            potential_driver = driver_row[2].strip()
                            # Check if it's a proper driver name
                            if (potential_driver and 
                                not potential_driver.isupper() and 
                                not potential_driver.isdigit() and
                                len(potential_driver.split()) <= 3 and
                                potential_driver != runner_data['horse_name']):
                                driver_name = potential_driver
                else:
                    # Standard format driver detection
                    for next_row_idx in range(row_idx + 1, min(row_idx + 4, len(table))):
                        if next_row_idx < len(table):
                            next_row = table[next_row_idx]
                            # Driver name is typically in column 2 of a subsequent row
                            if next_row and len(next_row) > 2 and next_row[2]:
                                potential_driver = next_row[2].strip()
                                # Driver names are usually proper case (not all caps like horse names)
                                if (potential_driver and 
                                    not potential_driver.isupper() and 
                                    not potential_driver.isdigit() and
                                    len(potential_driver.split()) <= 3):
                                    driver_name = potential_driver
                                    break
                
                runner_data['driver_name'] = driver_name
                runners.append(runner_data)
        
        return runners
    
    def _extract_race_info_from_page(self, page_text: str, page_number: int) -> Dict[str, Any]:
        """Extract race information from a specific page including date and track"""
        race_info = {
            'race_number': None,
            'race_name': None,
            'distance': None,
            'date': None,
            'track': None
        }

        lines = page_text.split('\n')
        for idx, line in enumerate(lines[:20]):  # Check first 20 lines for more info
            # Look for race pattern: "Race 1: WOLF SIGNS 4YO & OLDER 1 WIN PACE - 1780m"
            race_match = re.search(r'Race\s+(\d+):\s*([^-\n]+?)\s*-\s*(\d+)m', line, re.IGNORECASE)
            if race_match:
                race_info['race_number'] = int(race_match.group(1))
                race_info['race_name'] = race_match.group(2).strip()
                race_info['distance'] = int(race_match.group(3))

            # Enhanced track extraction for various formats
            if idx <= 2 and not race_info['track']:  # Check first 3 lines for track info
                # NSW format: "Penrith NSW - C-CLASS"
                if any(state in line for state in ['NSW', 'VIC', 'QLD', 'SA', 'TAS', 'WA']):
                    track_parts = line.split('-')[0].strip()
                    track_parts = track_parts.replace('NSW', '').replace('VIC', '').replace('QLD', '').replace('SA', '').replace('TAS', '').replace('WA', '').strip()
                    if track_parts:
                        race_info['track'] = track_parts
                # VIC format: "Geelong Race 1 Distance 2100m" or "Ballarat Race 1"
                elif 'Race' in line:
                    track_match = re.match(r'^([A-Za-z\s]+?)\s+Race', line)
                    if track_match:
                        potential_track = track_match.group(1).strip()
                        # Filter out common header words
                        excluded_words = ['Data', 'Processed', 'By', 'TripleS', 'Sectional', 'Information']
                        if not any(word.lower() in potential_track.lower() for word in excluded_words):
                            race_info['track'] = potential_track
                # Alternative VIC format: "Cranbourne VIC - C-CLASS" or just track name
                elif re.match(r'^[A-Za-z\s]+(?:\s+VIC)?(?:\s*-|$)', line):
                    # Extract just the track name part
                    track_clean = re.sub(r'\s+VIC\s*-.*$', '', line.strip(), flags=re.IGNORECASE)
                    track_clean = re.sub(r'\s*-.*$', '', track_clean)
                    if track_clean and len(track_clean) > 2 and track_clean.replace(' ', '').isalpha():
                        race_info['track'] = track_clean

            # Extract date patterns
            # NSW format: "23 May 2024 - 6:22PM"
            date_match1 = re.search(r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4})', line, re.IGNORECASE)
            if date_match1:
                race_info['date'] = date_match1.group(1)

            # VIC format: "Thursday, 14 March 2024"
            date_match2 = re.search(r'(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s*(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})', line, re.IGNORECASE)
            if date_match2:
                race_info['date'] = date_match2.group(1)

        return race_info
    
    def _extract_triples_qld_from_text(self, text: str) -> List[Dict[str, Any]]:
        """Extract QLD TripleS data from text with comprehensive sectional data"""
        runners = []
        lines = text.split('\n')
        
        # Look for horse data blocks - each horse spans multiple lines
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if not line:
                i += 1
                continue
            
            # Look for a line that starts with rank and tab number
            rank_match = re.match(r'^(\d+)\s+(\d+)\s+', line)
            if rank_match:
                rank = int(rank_match.group(1))
                tab_number = int(rank_match.group(2))
                
                # Look backwards for the horse name (should be 1-3 lines above)
                horse_name = None
                driver_name = None
                
                for j in range(max(0, i-4), i):
                    prev_line = lines[j].strip()
                    
                    # Look for horse name pattern - all caps name with timing data
                    horse_match = re.match(r'^([A-Z][A-Z\s&\-\'NZ]+?)\s+(\d{1,2}\.\d{2})', prev_line)
                    if horse_match:
                        potential_name = horse_match.group(1).strip()
                        # Filter out headers and non-horse words
                        excluded = {'QLD', 'C-CLASS', 'PACE', 'TROT', 'REDCLIFFE', 'WOLF SIGNS', 'OLDER', 'HORSE', 'DRIVER'}
                        if not any(word in potential_name.upper() for word in excluded):
                            horse_name = potential_name
                            break
                
                # Look forward for driver name (should be 1-3 lines below)
                for j in range(i+1, min(i+4, len(lines))):
                    next_line = lines[j].strip()
                    
                    # Look for driver name - properly capitalized, not all caps
                    driver_match = re.match(r'^([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', next_line)
                    if driver_match:
                        potential_driver = driver_match.group(1).strip()
                        # Make sure it's not a horse name or position indicator
                        if (potential_driver != horse_name and 
                            'Lead' not in potential_driver and 
                            'Q' not in potential_driver and
                            len(potential_driver.split()) <= 3 and
                            not potential_driver.isupper()):
                            driver_name = potential_driver
                            break
                
                # Extract detailed timing data from the current line and surrounding lines
                timing_data = self._extract_qld_timing_data(lines, i, horse_name)
                
                runner_data = {
                    'rank': rank,
                    'tab_number': tab_number,
                    'horse_name': horse_name,
                    'driver_name': driver_name,
                    **timing_data
                }
                
                runners.append(runner_data)
            
            i += 1
        
        return runners
    
    def _extract_qld_timing_data(self, lines: List[str], current_index: int, horse_name: str) -> Dict[str, Any]:
        """Extract comprehensive timing data for QLD TripleS format"""
        timing_data = {
            'top_speed': None,
            'fastest_section': None,
            'first_50m': None,
            'first_100m': None,
            'first_200m': None,
            'first_half': None,
            'middle_half': None,
            'last_half': None,
            'lead_time': None,
            'quarter_1_time': None,
            'quarter_1_position': None,
            'quarter_2_time': None,
            'quarter_2_position': None,
            'quarter_3_time': None,
            'quarter_3_position': None,
            'quarter_4_time': None,
            'quarter_4_position': None,
            'last_mile': None,
            'mile_rate': None,
            'distance_travelled': None,
            'final_time': None,
            'margin': None
        }
        
        # Look in surrounding lines for timing data
        for i in range(max(0, current_index-3), min(len(lines), current_index+3)):
            line = lines[i].strip()
            
            # Look for horse name line with comprehensive timing
            if horse_name and horse_name in line:
                # Pattern: "HORSE NAME speed half1 half2 half3 lead_time [pos] final_time"
                parts = line.split()
                if len(parts) >= 8:
                    try:
                        # Find the horse name and extract timing data after it
                        horse_parts = horse_name.split()
                        horse_end_idx = None
                        
                        for j in range(len(parts) - len(horse_parts) + 1):
                            if ' '.join(parts[j:j+len(horse_parts)]) == horse_name:
                                horse_end_idx = j + len(horse_parts)
                                break
                        
                        if horse_end_idx and horse_end_idx < len(parts):
                            data_parts = parts[horse_end_idx:]
                            if len(data_parts) >= 7:
                                timing_data['top_speed'] = float(data_parts[0]) if data_parts[0].replace('.', '').isdigit() else None
                                timing_data['first_half'] = data_parts[1] if ':' in data_parts[1] else None
                                timing_data['middle_half'] = data_parts[2] if ':' in data_parts[2] else None
                                timing_data['last_half'] = data_parts[3] if ':' in data_parts[3] else None
                                
                                # Look for lead time with position
                                for part in data_parts[4:]:
                                    if '[' in part and ']' in part:
                                        lead_match = re.match(r'(\d:\d{2}\.\d{2})\s*\[(\d+)\]', part)
                                        if lead_match:
                                            timing_data['lead_time'] = lead_match.group(1)
                                            timing_data['lead_position'] = int(lead_match.group(2))
                                
                                # Final time is usually the last time-formatted entry
                                for part in reversed(data_parts):
                                    if re.match(r'\d:\d{2}\.\d{2}', part):
                                        timing_data['final_time'] = part
                                        break
                    except (ValueError, IndexError):
                        pass
            
            # Look for rank/timing line with quarters and margins
            rank_match = re.match(r'^(\d+)\s+(\d+)\s+', line)
            if rank_match:
                parts = line.split()
                
                # Extract margin and final time
                for part in parts:
                    if 'm' in part and ('+' in part or part == '0m'):
                        timing_data['margin'] = part
                    elif re.match(r'\d:\d{2}\.\d{2}', part):
                        if not timing_data['final_time']:  # Don't overwrite if already found
                            timing_data['final_time'] = part
                
                # Look for quarter times in parentheses
                quarter_times = re.findall(r'\((\d:\d{2}\.\d{2})\)', line)
                if len(quarter_times) >= 4:
                    timing_data['quarter_1_time'] = quarter_times[0]
                    timing_data['quarter_2_time'] = quarter_times[1]
                    timing_data['quarter_3_time'] = quarter_times[2]
                    timing_data['quarter_4_time'] = quarter_times[3]
            
            # Look for quarter positions line
            quarter_pos_matches = re.findall(r'(\d:\d{2}\.\d{2})\s*\[(\d+)\]', line)
            if len(quarter_pos_matches) >= 4:
                timing_data['quarter_1_time'] = quarter_pos_matches[0][0]
                timing_data['quarter_1_position'] = int(quarter_pos_matches[0][1])
                timing_data['quarter_2_time'] = quarter_pos_matches[1][0]
                timing_data['quarter_2_position'] = int(quarter_pos_matches[1][1])
                timing_data['quarter_3_time'] = quarter_pos_matches[2][0]
                timing_data['quarter_3_position'] = int(quarter_pos_matches[2][1])
                timing_data['quarter_4_time'] = quarter_pos_matches[3][0]
                timing_data['quarter_4_position'] = int(quarter_pos_matches[3][1])
            
            # Look for distance data (driver line)
            if re.search(r'\d+m.*\d+m.*\d+m', line):
                distances = re.findall(r'(\d+)m', line)
                if len(distances) >= 9:  # Should have 9+ distance measurements
                    timing_data['distance_travelled'] = f"{distances[-1]}m"  # Last one is usually total
        
        return timing_data
    
    def _extract_triples_vic(self, text: str, pdf_obj=None) -> List[Dict[str, Any]]:
        """Extract TripleS data specifically for VIC format using table extraction"""
        runners = []
        
        # Try to extract the table structure directly from PDF
        if pdf_obj:
            try:
                return self._extract_triples_vic_from_table(pdf_obj)
            except Exception as e:
                print(f"VIC table extraction failed: {e}, falling back to text parsing")
                return self._extract_triples_vic_from_text(text)
        else:
            return self._extract_triples_vic_from_text(text)
    
    def _extract_triples_vic_from_table(self, pdf_obj) -> List[Dict[str, Any]]:
        """Extract VIC TripleS data using table extraction with improved patterns from QLD"""
        runners = []
        
        # Extract tables from all pages - VIC TripleS PDFs contain multiple races
        for page_num, page in enumerate(pdf_obj.pages):
            tables = page.extract_tables()
            
            if not tables:
                continue
            
            # Use the first table on each page
            table = tables[0]
            
            if len(table) < 2:  # Need at least header + 1 data row
                continue
            
            # Get race information from this page
            page_text = page.extract_text()
            race_info = self._extract_race_info_from_page(page_text, page_num + 1)
            
            # Skip header row and process each data row
            for row_idx, row in enumerate(table[1:], 1):
                if not row or len(row) < 15:  # Reduced minimum requirement for better compatibility
                    continue
                    
                # Skip rows that don't have rank data (sub-rows)
                if not row[0] or not row[0].isdigit():
                    continue
                
                try:    
                    runner_data = {
                        'rank': int(row[0]) if row[0] and row[0].isdigit() else None,
                        'tab_number': int(row[1]) if row[1] and row[1].isdigit() else None,
                        'horse_name': row[2].strip() if row[2] else None,
                        'top_speed': float(row[3]) if row[3] and row[3].replace('.', '').isdigit() else None,
                        'fastest_section': row[4] if len(row) > 4 else None,
                        'first_50m': row[5] if len(row) > 5 else None,
                        'first_100m': row[6] if len(row) > 6 else None,
                        'first_200m': row[7] if len(row) > 7 else None,
                        'first_half': row[8] if len(row) > 8 else None,
                        'middle_half': row[9] if len(row) > 9 else None,
                        'last_half': row[10] if len(row) > 10 else None,
                        'lead_time': row[11] if len(row) > 11 else None,
                        'quarter_1': row[12] if len(row) > 12 else None,
                        'quarter_2': row[13] if len(row) > 13 else None,
                        'quarter_3': row[14] if len(row) > 14 else None,
                        'quarter_4': row[15] if len(row) > 15 else None,
                        'last_mile': row[16] if len(row) > 16 else None,
                        'mile_rate': row[17] if len(row) > 17 else None,
                        'distance_travelled': row[18] if len(row) > 18 else None,
                        'gross_time_margin': row[19] if len(row) > 19 else None,
                        # Add race information
                        'race_number': race_info.get('race_number') if race_info else None,
                        'race_name': race_info.get('race_name') if race_info else None,
                        'distance': race_info.get('distance') if race_info else None,
                        'date': race_info.get('date') if race_info else None,
                        'track': race_info.get('track') if race_info else None,
                        'page_number': page_num + 1
                    }
                    
                    # Only add runners with essential data
                    if not (runner_data['tab_number'] and runner_data['horse_name']):
                        continue
                        
                except (ValueError, IndexError) as e:
                    print(f"Warning: Error processing VIC TripleS row {row_idx} on page {page_num + 1}: {e}")
                    continue
                
                # Extract quarter positions from quarter data (format: "time [position]")
                for quarter_num in [1, 2, 3, 4]:
                    quarter_data = runner_data[f'quarter_{quarter_num}']
                    if quarter_data:
                        # Extract time and position
                        time_match = re.search(r'(\d:\d{2}\.\d{2})', quarter_data)
                        pos_match = re.search(r'\[(\d+)\]', quarter_data)
                        
                        runner_data[f'quarter_{quarter_num}_time'] = time_match.group(1) if time_match else None
                        runner_data[f'quarter_{quarter_num}_position'] = int(pos_match.group(1)) if pos_match else None
                
                # Extract margin and final time from gross_time_margin
                if runner_data['gross_time_margin']:
                    # Look for margin pattern like "+28m" or "0m"
                    margin_match = re.search(r'(\+?\d+\.?\d*m|0m)', runner_data['gross_time_margin'])
                    time_match = re.search(r'(\d:\d{2}\.\d{2})', runner_data['gross_time_margin'])
                    
                    runner_data['margin'] = margin_match.group(1) if margin_match else None
                    runner_data['final_time'] = time_match.group(1) if time_match else None
                
                # Extract lead time and position
                if runner_data['lead_time']:
                    lead_match = re.match(r'(\d:\d{2}\.\d{2})\s*\[(\d+)\]', runner_data['lead_time'])
                    if lead_match:
                        runner_data['lead_time_value'] = lead_match.group(1)
                        runner_data['lead_position'] = int(lead_match.group(2))
                
                # Find driver name in subsequent rows (look ahead in table) - VIC specific
                driver_name = None
                for next_row_idx in range(row_idx + 1, min(row_idx + 4, len(table))):
                    if next_row_idx < len(table):
                        next_row = table[next_row_idx]
                        # Driver name is typically in column 2 of a subsequent row
                        if next_row and len(next_row) > 2 and next_row[2]:
                            potential_driver = next_row[2].strip()
                            # Check if it looks like a driver name (proper case, not all caps)
                            if (re.match(r'^[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*$', potential_driver) and
                                potential_driver != runner_data['horse_name'] and
                                'Lead' not in potential_driver and
                                'VIC' not in potential_driver):
                                driver_name = potential_driver
                                break
                
                runner_data['driver_name'] = driver_name
                
                # Only add if we have essential data
                if runner_data['horse_name'] and runner_data['rank'] and runner_data['tab_number']:
                    runners.append(runner_data)
        
        return runners
    
    def _extract_triples_vic_from_text(self, text: str) -> List[Dict[str, Any]]:
        """Fallback text-based extraction for VIC TripleS format"""
        runners = []
        lines = text.split('\n')
        
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
                
            # VIC TripleS: Similar to QLD but with VIC-specific variations
            rank_match = re.match(r'^(\d+)\s+(\d+)', line)
            if rank_match:
                rank = int(rank_match.group(1))
                tab_number = int(rank_match.group(2))
                
                runner_data = {
                    'rank': rank,
                    'tab_number': tab_number,
                    'horse_name': None,
                    'driver_name': None,
                    'top_speed': None,
                    'fastest_section': None,
                    'final_time': None,
                    'margin': None,
                    'quarters': [],
                    'sectionals': {}
                }
                
                # VIC specific pattern matching
                for j in range(i, min(i + 10, len(lines))):
                    check_line = lines[j].strip()
                    
                    # VIC horse name pattern
                    if not runner_data['horse_name']:
                        horse_match = re.search(r'\b([A-Z][A-Z\s&\-\']{3,30})\b', check_line)
                        if horse_match:
                            potential_name = horse_match.group(1).strip()
                            # Filter out VIC specific non-horse words
                            excluded = {'VIC', 'C-CLASS', 'PACE', 'TROT', 'MOBILE', 'STANDING', 'BARRIER', 'CRANBOURNE'}
                            if potential_name not in excluded and len(potential_name.split()) <= 5:
                                runner_data['horse_name'] = potential_name
                    
                    # VIC timing patterns (similar to QLD)
                    speed_match = re.search(r'(\d{2}\.\d{2})\s+0:(\d{2}\.\d{2})', check_line)
                    if speed_match:
                        runner_data['top_speed'] = float(speed_match.group(1))
                        runner_data['fastest_section'] = f"0:{speed_match.group(2)}"
                    
                    quarter_matches = re.findall(r'(\d:\d{2}\.\d{2})\s+\[(\d+)\]', check_line)
                    for time_str, pos_str in quarter_matches:
                        runner_data['quarters'].append({
                            'time': time_str,
                            'position': int(pos_str)
                        })
                    
                    time_margin_match = re.search(r'(\d:\d{2}\.\d{2})\s*(\+?\d+\.?\d*m|0m)', check_line)
                    if time_margin_match:
                        runner_data['final_time'] = time_margin_match.group(1)
                        runner_data['margin'] = time_margin_match.group(2)
                
                if runner_data['horse_name'] or runner_data['top_speed'] or runner_data['final_time']:
                    runners.append(runner_data)
        
        return runners
    
    def _extract_triples_generic(self, text: str) -> List[Dict[str, Any]]:
        """Generic TripleS extraction for unknown states"""
        runners = []
        lines = text.split('\n')
        
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
                
            rank_match = re.match(r'^(\d+)\s+(\d+)', line)
            if rank_match:
                rank = int(rank_match.group(1))
                tab_number = int(rank_match.group(2))
                
                runner_data = {
                    'rank': rank,
                    'tab_number': tab_number,
                    'horse_name': None,
                    'driver_name': None,
                    'top_speed': None,
                    'fastest_section': None,
                    'final_time': None,
                    'margin': None,
                    'quarters': [],
                    'sectionals': {}
                }
                
                # Generic pattern matching
                for j in range(i, min(i + 10, len(lines))):
                    check_line = lines[j].strip()
                    
                    if not runner_data['horse_name']:
                        horse_match = re.search(r'\b([A-Z][A-Z\s&\-\']{3,30})\b', check_line)
                        if horse_match:
                            potential_name = horse_match.group(1).strip()
                            excluded = {'THE', 'AND', 'OR', 'OF', 'IN', 'ON', 'AT', 'TO', 'FOR', 'WITH', 'BY', 'C-CLASS', 'PACE', 'TROT'}
                            if potential_name not in excluded and len(potential_name.split()) <= 5:
                                runner_data['horse_name'] = potential_name
                    
                    speed_match = re.search(r'(\d{2}\.\d{2})\s+0:(\d{2}\.\d{2})', check_line)
                    if speed_match:
                        runner_data['top_speed'] = float(speed_match.group(1))
                        runner_data['fastest_section'] = f"0:{speed_match.group(2)}"
                    
                    quarter_matches = re.findall(r'(\d:\d{2}\.\d{2})\s+\[(\d+)\]', check_line)
                    for time_str, pos_str in quarter_matches:
                        runner_data['quarters'].append({
                            'time': time_str,
                            'position': int(pos_str)
                        })
                    
                    time_margin_match = re.search(r'(\d:\d{2}\.\d{2})\s*(\+?\d+\.?\d*m|0m)', check_line)
                    if time_margin_match:
                        runner_data['final_time'] = time_margin_match.group(1)
                        runner_data['margin'] = time_margin_match.group(2)
                
                if runner_data['horse_name'] or runner_data['top_speed'] or runner_data['final_time']:
                    runners.append(runner_data)
        
        return runners
    
    def _extract_triples_nsw(self, text: str, pdf_obj=None) -> List[Dict[str, Any]]:
        """Extract TripleS data specifically for NSW format with enhanced patterns"""
        runners = []
        
        # Try table-based extraction first if PDF object is available
        if pdf_obj:
            try:
                return self._extract_triples_nsw_from_table(pdf_obj)
            except Exception as e:
                print(f"NSW TripleS table extraction failed: {e}, falling back to text parsing")
                return self._extract_triples_nsw_from_text(text)
        else:
            return self._extract_triples_nsw_from_text(text)
    
    def _extract_triples_nsw_from_table(self, pdf_obj) -> List[Dict[str, Any]]:
        """Extract NSW TripleS data using exact VIC table extraction implementation"""
        runners = []
        
        # Extract tables from all pages - NSW TripleS PDFs contain multiple races
        for page_num, page in enumerate(pdf_obj.pages):
            tables = page.extract_tables()
            
            if not tables:
                continue
            
            # Use the first table on each page
            table = tables[0]
            
            if len(table) < 2:  # Need at least header + 1 data row
                continue
            
            # Get race information from this page
            page_text = page.extract_text()
            race_info = self._extract_race_info_from_page(page_text, page_num + 1)
            
            # Skip header row and process each data row
            for row_idx, row in enumerate(table[1:], 1):
                if not row or len(row) < 15:  # Reduced minimum requirement for better compatibility
                    continue
                    
                # Skip rows that don't have rank data (sub-rows)
                if not row[0] or not row[0].isdigit():
                    continue
                
                try:    
                    runner_data = {
                        'rank': int(row[0]) if row[0] and row[0].isdigit() else None,
                        'tab_number': int(row[1]) if row[1] and row[1].isdigit() else None,
                        'horse_name': row[2].strip() if row[2] else None,
                        'top_speed': float(row[3]) if row[3] and row[3].replace('.', '').isdigit() else None,
                        'fastest_section': row[4] if len(row) > 4 else None,
                        'first_50m': row[5] if len(row) > 5 else None,
                        'first_100m': row[6] if len(row) > 6 else None,
                        'first_200m': row[7] if len(row) > 7 else None,
                        'first_half': row[8] if len(row) > 8 else None,
                        'middle_half': row[9] if len(row) > 9 else None,
                        'last_half': row[10] if len(row) > 10 else None,
                        'lead_time': row[11] if len(row) > 11 else None,
                        'quarter_1': row[12] if len(row) > 12 else None,
                        'quarter_2': row[13] if len(row) > 13 else None,
                        'quarter_3': row[14] if len(row) > 14 else None,
                        'quarter_4': row[15] if len(row) > 15 else None,
                        'last_mile': row[16] if len(row) > 16 else None,
                        'mile_rate': row[17] if len(row) > 17 else None,
                        'distance_travelled': row[18] if len(row) > 18 else None,
                        'gross_time_margin': row[19] if len(row) > 19 else None,
                        # Add race information
                        'race_number': race_info.get('race_number') if race_info else None,
                        'race_name': race_info.get('race_name') if race_info else None,
                        'distance': race_info.get('distance') if race_info else None,
                        'date': race_info.get('date') if race_info else None,
                        'track': race_info.get('track') if race_info else None,
                        'page_number': page_num + 1
                    }
                    
                    # Only add runners with essential data
                    if not (runner_data['tab_number'] and runner_data['horse_name']):
                        continue
                        
                except (ValueError, IndexError) as e:
                    print(f"Warning: Error processing NSW TripleS row {row_idx} on page {page_num + 1}: {e}")
                    continue
                
                # Extract quarter positions from quarter data (format: "time [position]")
                for quarter_num in [1, 2, 3, 4]:
                    quarter_data = runner_data[f'quarter_{quarter_num}']
                    if quarter_data:
                        # Extract time and position
                        time_match = re.search(r'(\d:\d{2}\.\d{2})', quarter_data)
                        pos_match = re.search(r'\[(\d+)\]', quarter_data)
                        
                        runner_data[f'quarter_{quarter_num}_time'] = time_match.group(1) if time_match else None
                        runner_data[f'quarter_{quarter_num}_position'] = int(pos_match.group(1)) if pos_match else None
                
                # Extract margin and final time from gross_time_margin
                if runner_data['gross_time_margin']:
                    # Look for margin pattern like "+28m" or "0m"
                    margin_match = re.search(r'(\+?\d+\.?\d*m|0m)', runner_data['gross_time_margin'])
                    time_match = re.search(r'(\d:\d{2}\.\d{2})', runner_data['gross_time_margin'])
                    
                    runner_data['margin'] = margin_match.group(1) if margin_match else None
                    runner_data['final_time'] = time_match.group(1) if time_match else None
                
                # Extract lead time and position
                if runner_data['lead_time']:
                    lead_match = re.match(r'(\d:\d{2}\.\d{2})\s*\[(\d+)\]', runner_data['lead_time'])
                    if lead_match:
                        runner_data['lead_time_value'] = lead_match.group(1)
                        runner_data['lead_position'] = int(lead_match.group(2))
                
                # Find driver name in subsequent rows (look ahead in table) - VIC exact logic
                driver_name = None
                for next_row_idx in range(row_idx + 1, min(row_idx + 4, len(table))):
                    if next_row_idx < len(table):
                        next_row = table[next_row_idx]
                        # Driver name is typically in column 2 of a subsequent row
                        if next_row and len(next_row) > 2 and next_row[2]:
                            potential_driver = next_row[2].strip()
                            # Check if it looks like a driver name (proper case, not all caps) - VIC pattern
                            if (re.match(r'^[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*$', potential_driver) and
                                potential_driver != runner_data['horse_name'] and
                                'Lead' not in potential_driver and
                                'NSW' not in potential_driver and
                                'Menangle' not in potential_driver and
                                'Newcastle' not in potential_driver and
                                'Penrith' not in potential_driver):
                                driver_name = potential_driver
                                break
                
                runner_data['driver_name'] = driver_name
                
                # Only add if we have essential data - VIC validation pattern
                if runner_data['horse_name'] and runner_data['rank'] and runner_data['tab_number']:
                    runners.append(runner_data)
        
        return runners
    
    def _extract_triples_nsw_from_text(self, text: str) -> List[Dict[str, Any]]:
        """Fallback text-based extraction for NSW TripleS format using VIC logic"""
        runners = []
        lines = text.split('\n')
        
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
                
            # NSW TripleS: Using VIC pattern matching logic
            rank_match = re.match(r'^(\d+)\s+(\d+)', line)
            if rank_match:
                rank = int(rank_match.group(1))
                tab_number = int(rank_match.group(2))
                
                runner_data = {
                    'rank': rank,
                    'tab_number': tab_number,
                    'horse_name': None,
                    'driver_name': None,
                    'top_speed': None,
                    'fastest_section': None,
                    'final_time': None,
                    'margin': None,
                    'quarters': [],
                    'sectionals': {}
                }
                
                # NSW specific pattern matching with VIC logic
                for j in range(i, min(i + 10, len(lines))):
                    check_line = lines[j].strip()
                    
                    # NSW horse name pattern
                    if not runner_data['horse_name']:
                        horse_match = re.search(r'\b([A-Z][A-Z\s&\-\']{3,30})\b', check_line)
                        if horse_match:
                            potential_name = horse_match.group(1).strip()
                            # Filter out NSW specific non-horse words
                            excluded = {'NSW', 'PACE', 'TROT', 'MOBILE', 'STANDING', 'BARRIER', 
                                      'MENANGLE', 'NEWCASTLE', 'PENRITH', 'BATHURST', 'TAMWORTH'}
                            if potential_name not in excluded and len(potential_name.split()) <= 5:
                                runner_data['horse_name'] = potential_name
                    
                    # NSW timing patterns (using VIC logic)
                    speed_match = re.search(r'(\d{2}\.\d{2})\s+0:(\d{2}\.\d{2})', check_line)
                    if speed_match:
                        runner_data['top_speed'] = float(speed_match.group(1))
                        runner_data['fastest_section'] = f"0:{speed_match.group(2)}"
                    
                    quarter_matches = re.findall(r'(\d:\d{2}\.\d{2})\s+\[(\d+)\]', check_line)
                    for time_str, pos_str in quarter_matches:
                        runner_data['quarters'].append({
                            'time': time_str,
                            'position': int(pos_str)
                        })
                    
                    time_margin_match = re.search(r'(\d:\d{2}\.\d{2})\s*(\+?\d+\.?\d*m|0m)', check_line)
                    if time_margin_match:
                        runner_data['final_time'] = time_margin_match.group(1)
                        runner_data['margin'] = time_margin_match.group(2)
                
                if runner_data['horse_name'] or runner_data['top_speed'] or runner_data['final_time']:
                    runners.append(runner_data)
        
        return runners
    
    def _extract_triples_sa(self, text: str) -> List[Dict[str, Any]]:
        """Extract TripleS data specifically for SA format - uses generic extraction"""
        return self._extract_triples_generic(text)
    
    def _extract_triples_tas(self, text: str) -> List[Dict[str, Any]]:
        """Extract TripleS data specifically for TAS format - uses generic extraction"""
        return self._extract_triples_generic(text)
    
    def extract_triples_detailed_data(self, pdf_obj, state: str = None, file_path: str = None) -> List[Dict[str, Any]]:
        """Extract data from detailed TripleS format PDFs (like Redcliffe sub-type)"""
        runners = []
        
        try:
            for page_num, page in enumerate(pdf_obj.pages):
                # Extract race info from page text
                page_text = page.extract_text()
                if not page_text:
                    continue
                
                # Extract race metadata from page
                race_info = self._extract_detailed_race_info(page_text)
                
                # Extract table data
                tables = page.extract_tables()
                if not tables:
                    continue
                
                # Process the main table (should be the first/largest table)
                main_table = tables[0]
                if len(main_table) < 2:  # Need header + at least one data row
                    continue
                
                # Extract runners from table
                page_runners = self._extract_runners_from_detailed_table(main_table, race_info)
                runners.extend(page_runners)
                
        except Exception as e:
            print(f"Error extracting detailed triples data: {e}")
            return []
            
        return runners
    
    def _extract_detailed_race_info(self, page_text: str) -> Dict[str, Any]:
        """Extract race information from detailed TripleS format page"""
        race_info = {}
        
        lines = page_text.split('\n')
        for line in lines[:10]:  # Check first 10 lines for race info
            line = line.strip()
            
            # Extract race number and distance: "Race 1: 2023 TROT RODS FINAL NIGHT HEAT 11 - 947m"
            race_match = re.search(r'Race\s+(\d+).*?(\d+)m\s*$', line)
            if race_match:
                race_info['race_number'] = int(race_match.group(1))
                race_info['distance'] = f"{race_match.group(2)}m"
                
            # Extract date: "24 May 2023 - 4:53PM"  
            date_match = re.search(r'(\d{1,2}\s+\w+\s+\d{4})', line)
            if date_match:
                race_info['date'] = date_match.group(1)
                
        return race_info
    
    def _extract_runners_from_detailed_table(self, table: List[List[str]], race_info: Dict) -> List[Dict[str, Any]]:
        """Extract runner data from detailed TripleS table format"""
        runners = []
        
        if not table or len(table) < 2:
            return runners
            
        # Skip header row and process data rows
        for i in range(1, len(table)):
            row = table[i]
            if not row or len(row) < 10:  # Need sufficient columns
                continue
                
            # Skip rows that are clearly not runner data (like sub-headers)
            if not row[0] or not row[0].strip() or not row[0].strip().isdigit():
                continue
                
            try:
                runner = {
                    # Basic info
                    'rank': int(row[0].strip()) if row[0] and row[0].strip().isdigit() else None,
                    'tab_number': int(row[1].strip()) if row[1] and row[1].strip().isdigit() else None,
                    'horse_name': self._clean_horse_name(row[2]) if row[2] else '',
                    
                    # Performance metrics  
                    'top_speed': self._clean_numeric_field(row[3]),
                    'fastest_section': self._clean_time_field(row[4]),
                    'first_50m': self._clean_numeric_field(row[5]),
                    'first_100m': self._clean_numeric_field(row[6]),
                    'first_200m': self._clean_numeric_field(row[7]),
                    'lead_time': self._clean_time_field(row[8]),
                    'first_quarter': self._clean_time_field(row[9]),
                    'second_quarter': self._clean_time_field(row[10]) if len(row) > 10 else None,
                    'last_mile': self._clean_time_field(row[11]) if len(row) > 11 else None,
                    'distance_travelled': row[12] if len(row) > 12 else None,
                    'gross_time_margin': row[13] if len(row) > 13 else None,
                    
                    # Race metadata
                    'race_number': race_info.get('race_number'),
                    'distance': race_info.get('distance'),
                    'date': race_info.get('date')
                }
                
                # Extract actual time and margin from gross_time_margin field
                if runner['gross_time_margin']:
                    time_match = re.search(r'(\d:\d{2}\.\d{2})', runner['gross_time_margin'])
                    if time_match:
                        runner['final_time'] = time_match.group(1)
                        
                runners.append(runner)
                
            except (ValueError, IndexError) as e:
                # Skip problematic rows but continue processing
                continue
                
        return runners
    
    def _clean_horse_name(self, name_field: str) -> str:
        """Clean horse name field, removing driver info"""
        if not name_field:
            return ''
        # Take first line if multi-line (horse name usually comes first)
        return name_field.split('\n')[0].strip()
    
    def _clean_numeric_field(self, field: str) -> str:
        """Clean numeric fields, keeping only numbers and decimal points"""
        if not field:
            return None
        # Extract numeric value, removing extra formatting
        clean = re.sub(r'[^\d\.]', '', field.strip())
        return clean if clean else None
    
    def _clean_time_field(self, field: str) -> str:
        """Clean time fields, extracting time format"""
        if not field:
            return None
        # Look for time patterns like "0:09.52" or "1:06.56"  
        time_match = re.search(r'(\d:\d{2}\.\d{2})', field)
        if time_match:
            return time_match.group(1)
        # Also handle seconds format like "28.70s"
        seconds_match = re.search(r'(\d{2}\.\d{2})s', field) 
        if seconds_match:
            return f"{seconds_match.group(1)}s"
        return field.strip() if field.strip() else None
    
    def _parse_combined_position_data(self, field: str) -> tuple:
        """Parse combined position data like '12.0m (1)' into margin and width components"""
        if not field:
            return None, None
        
        field = field.strip()
        
        # Pattern: "12.0m (1)" -> margin = "12.0m", width = "1"
        match = re.match(r'^([\d\.]+m)\s*\((\d+)\)$', field)
        if match:
            margin = match.group(1)
            width = match.group(2)
            return margin, width
        
        # If no parentheses, assume it's just margin
        if field.endswith('m'):
            return field, None
            
        return field, None
    
    def extract_pj_data(self, pdf_obj_or_text, state: str = None, file_path: str = None) -> List[Dict[str, Any]]:
        """Extract data from PJ format PDFs using state-specific methods"""
        
        # Handle both PDF object and text input
        if hasattr(pdf_obj_or_text, 'pages'):
            # PDF object passed
            pdf_obj = pdf_obj_or_text
            text = pdf_obj.pages[0].extract_text() if pdf_obj.pages else ""
        else:
            # Text passed
            text = pdf_obj_or_text
            pdf_obj = None
        
        if not state:
            # Try to determine state from file path first, fallback to text
            if file_path:
                state = self._detect_state_from_path(file_path)
            if not state or state == 'unknown':
                state = self._detect_state_from_text(text)
        
        # Route to state-specific extraction method
        if state == 'qld':
            if pdf_obj:
                return self._extract_pj_qld(pdf_obj)
            else:
                # Fallback to text-based for QLD if no PDF object
                return self._extract_pj_qld_from_text(text, {})
        elif state == 'vic':
            if pdf_obj:
                return self._extract_pj_vic(pdf_obj)
            else:
                # Fallback to text-based for VIC if no PDF object
                return self._extract_pj_vic_from_text(text, {})
        elif state == 'nsw':
            # Pass PDF object if available for NSW multi-format support
            return self._extract_pj_nsw(pdf_obj if pdf_obj else text)
        elif state == 'sa':
            # Pass PDF object if available, otherwise pass text
            return self._extract_pj_sa(pdf_obj if pdf_obj else text)
        elif state == 'tas':
            # Pass PDF object if available, otherwise pass text
            return self._extract_pj_tas(pdf_obj if pdf_obj else text)
        else:
            # Fallback to generic method
            return self._extract_pj_generic(text)
    
    def _extract_pj_qld(self, pdf_obj) -> List[Dict[str, Any]]:
        """Extract PJ data specifically for QLD format - page by page processing with individual CSVs"""
        import pandas as pd
        from pathlib import Path
        import tempfile
        import os
        
        temp_csvs = []
        all_runners = []
        
        # Process each page separately and create individual CSVs
        for page_num, page in enumerate(pdf_obj.pages):
            page_text = page.extract_text()
            
            # Extract race metadata from this page
            race_info = self._extract_pj_race_info_from_page(page_text, page_num + 1)
            
            # Skip if no race data found
            if not race_info.get('race_number'):
                continue
            
            # Extract table data from this page using table extraction
            tables = page.extract_tables()
            page_runners = []
            
            # Try table-based extraction first, but fallback to text if no valid data
            if tables:
                page_runners = self._extract_pj_qld_from_table(tables[0], race_info)
            
            # If table extraction failed or returned no data, use text-based extraction
            if not page_runners:
                page_runners = self._extract_pj_qld_from_text(page_text, race_info)
            
            # Skip if no runners found
            if not page_runners:
                continue
            
            # Add all race metadata to each runner for this page/race
            for runner in page_runners:
                runner.update({
                    'race_number': race_info.get('race_number'),
                    'distance': race_info.get('distance'),
                    'date': race_info.get('date'),
                    'gross_time': race_info.get('gross_time'),
                    'mile_rate': race_info.get('mile_rate'),
                    'lead_time': race_info.get('lead_time'),
                    'quarter_1': race_info.get('quarter_1'),
                    'quarter_2': race_info.get('quarter_2'),
                    'quarter_3': race_info.get('quarter_3'),
                    'quarter_4': race_info.get('quarter_4')
                })
            
            # Create temporary CSV for this page/race
            if page_runners:
                temp_file = tempfile.NamedTemporaryFile(mode='w', suffix=f'_race_{race_info.get("race_number", page_num+1)}.csv', delete=False)
                df = pd.DataFrame(page_runners)
                df.to_csv(temp_file.name, index=False)
                temp_file.close()
                temp_csvs.append(temp_file.name)
                
                # Add to overall collection
                all_runners.extend(page_runners)
        
        # Concatenate all CSVs at the end
        if temp_csvs:
            combined_df = pd.concat([pd.read_csv(csv_file) for csv_file in temp_csvs], ignore_index=True)
            
            # Clean up temp files
            for temp_csv in temp_csvs:
                try:
                    os.unlink(temp_csv)
                except:
                    pass
        
        return all_runners
    
    def _extract_pj_race_info_from_page(self, page_text: str, page_num: int) -> Dict[str, Any]:
        """Extract Track, Race Number, Distance, Date and race timing data from PJ page header"""
        race_info = {
            'track': None,
            'race_number': None,
            'distance': None,
            'date': None,
            'gross_time': None,
            'mile_rate': None,
            'quarter_1': None,
            'quarter_2': None,
            'quarter_3': None,
            'quarter_4': None,
            'page_number': page_num
        }
        
        lines = page_text.split('\n')[:15]  # Look in first 15 lines for better VIC support

        for idx, line in enumerate(lines):
            line = line.strip()

            # Extract from header line: "Albion Park Race 1 Distance 1660m Friday, 13 December 2019"
            header_match = re.match(r'^(.+?)\s+Race\s+(\d+)\s+Distance\s+(\d+)m\s+(.+)$', line)
            if header_match and not race_info['track']:
                race_info['track'] = header_match.group(1).strip()
                race_info['race_number'] = int(header_match.group(2))
                race_info['distance'] = f"{header_match.group(3)}m"
                race_info['date'] = header_match.group(4).strip()

            # Enhanced VIC format detection in early lines
            if idx <= 3 and not race_info['track']:
                # VIC format: "Geelong Race 1" or "Ballarat Race 2 Distance 2100m"
                vic_race_match = re.match(r'^([A-Za-z\s]+?)\s+Race\s+(\d+)', line)
                if vic_race_match:
                    potential_track = vic_race_match.group(1).strip()
                    # Filter out common non-track words
                    excluded_words = ['Data', 'Processed', 'By', 'Sectional', 'Information', 'PJ']
                    if not any(word.lower() in potential_track.lower() for word in excluded_words):
                        race_info['track'] = potential_track
                        race_info['race_number'] = int(vic_race_match.group(2))

                        # Look for distance in same line or next line
                        distance_match = re.search(r'Distance\s+(\d+)m', line)
                        if distance_match:
                            race_info['distance'] = f"{distance_match.group(1)}m"

                # Alternative VIC patterns: just track name on first line
                elif idx == 0 and re.match(r'^[A-Za-z\s]+$', line) and len(line.strip()) > 2:
                    # Check if it looks like a track name (alphabetic, reasonable length)
                    if line.replace(' ', '').isalpha() and 3 <= len(line.strip()) <= 25:
                        race_info['track'] = line
            
            # Alternative format: "Race No. 1 Distance 2090m 2:40.70 7.10s 29.80s 29.20s" (TAS 2020 format)
            alt_header_match = re.match(r'^Race\s+No\.\s+(\d+)\s+Distance\s+(\d+)m(?:\s+.*)?', line)
            if alt_header_match and not race_info['race_number']:
                race_info['race_number'] = int(alt_header_match.group(1))
                race_info['distance'] = f"{alt_header_match.group(2)}m"
            
            # TAS 2020 format with timing data: "Race No. 1 Distance 1680m 2:05.50 7.20s 29.70s 30.40s"
            # Note: The timing values in the header are race summary data, not individually labeled
            tas_2020_timing_match = re.match(r'^Race\s+No\.\s+(\d+)\s+Distance\s+(\d+)m\s+(\d:\d{2}\.\d{2})\s+([\d\.]+)s\s+([\d\.]+)s\s+([\d\.]+)s', line)
            if tas_2020_timing_match:
                race_info['race_number'] = int(tas_2020_timing_match.group(1))
                race_info['distance'] = f"{tas_2020_timing_match.group(2)}m"
                race_info['gross_time'] = tas_2020_timing_match.group(3)
                race_info['quarter_1'] = f"{tas_2020_timing_match.group(5)}s"  # 800m-400m split
                race_info['quarter_4'] = f"{tas_2020_timing_match.group(6)}s"  # Last 400m
                
                # Calculate mile rate from gross time and distance for TAS 2020 format
                try:
                    distance_m = int(tas_2020_timing_match.group(2))
                    gross_time_str = tas_2020_timing_match.group(3)
                    
                    # Convert gross time to seconds
                    time_parts = gross_time_str.split(':')
                    if len(time_parts) == 2:
                        minutes = int(time_parts[0])
                        seconds = float(time_parts[1])
                        total_seconds = minutes * 60 + seconds
                        
                        # Calculate mile rate (time for 1609m/1 mile)
                        mile_seconds = (total_seconds * 1609) / distance_m
                        mile_minutes = int(mile_seconds // 60)
                        mile_remainder = mile_seconds % 60
                        race_info['mile_rate'] = f"{mile_minutes}:{mile_remainder:05.2f}"
                except (ValueError, ZeroDivisionError):
                    pass  # Keep mile_rate as None if calculation fails
            
            # TAS 2020 format venue extraction: "Sectional information Hobart" or "Hobart Sunday, 2 August 2020"
            venue_match = re.match(r'^Sectional information\s+(.+)$', line)
            if venue_match and not race_info['track']:
                race_info['track'] = venue_match.group(1).strip()
            
            # TAS format with detailed race timing info - process this first to capture all timing data
            # "Gross Time:2:41.60 MileRate:2:04.40 LeadTime: 39.30s First Qtr: 31.90s Second Qtr: 31.50s Third Qtr:29.00s Fourth Qtr: 29.90s"
            tas_detailed_match = re.search(r'Gross\s+Time:([\d:.]+).*?MileRate:([\d:.]+).*?LeadTime:\s*([\d.]+)s.*?First\s+Qtr:\s*([\d.]+)s.*?Second\s+Qtr:\s*([\d.]+)s.*?Third\s+Qtr:([\d.]+)s.*?Fourth\s+Qtr:\s*([\d.]+)s', line)
            if tas_detailed_match:
                race_info['gross_time'] = tas_detailed_match.group(1)
                race_info['mile_rate'] = tas_detailed_match.group(2)
                race_info['lead_time'] = f"{tas_detailed_match.group(3)}s"
                race_info['quarter_1'] = f"{tas_detailed_match.group(4)}s"
                race_info['quarter_2'] = f"{tas_detailed_match.group(5)}s"
                race_info['quarter_3'] = f"{tas_detailed_match.group(6)}s"
                race_info['quarter_4'] = f"{tas_detailed_match.group(7)}s"
                continue  # Skip individual patterns if detailed match found
            
            # Alternative venue pattern: "Hobart Sunday, 2 August 2020"
            venue_date_match = re.match(r'^(.+?)\s+(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+(.+)$', line)
            if venue_date_match and not race_info['track'] and not race_info['date']:
                race_info['track'] = venue_date_match.group(1).strip()
                race_info['date'] = f"{venue_date_match.group(2)}, {venue_date_match.group(3)}".strip()
            
            # TAS 2020 format date extraction: "Sunday, 2 August 2020"
            date_match = re.match(r'^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+(.+)$', line)
            if date_match and not race_info['date']:
                race_info['date'] = f"{date_match.group(1)}, {date_match.group(2)}".strip()
            
            # Fallback individual timing extractions (only if detailed pattern didn't match)
            timing_match = re.search(r'Gross Time:(\d:\d{2}\.\d{2})', line)
            if timing_match and not race_info['gross_time']:
                race_info['gross_time'] = timing_match.group(1)
            
            mile_rate_match = re.search(r'MileRate:(\d:\d{2}\.\d{2})', line)
            if mile_rate_match and not race_info['mile_rate']:
                race_info['mile_rate'] = mile_rate_match.group(1)
            
            quarter_1_match = re.search(r'First Qtr:\s*([\d\.]+)s', line)
            if quarter_1_match and not race_info['quarter_1']:
                race_info['quarter_1'] = f"{quarter_1_match.group(1)}s"
            
            quarter_2_match = re.search(r'Second Qtr:\s*([\d\.]+)s', line)
            if quarter_2_match and not race_info['quarter_2']:
                race_info['quarter_2'] = f"{quarter_2_match.group(1)}s"
            
            quarter_3_match = re.search(r'Third Qtr:\s*([\d\.]+)s', line)
            if quarter_3_match and not race_info['quarter_3']:
                race_info['quarter_3'] = f"{quarter_3_match.group(1)}s"
            
            quarter_4_match = re.search(r'Fourth Qtr:\s*([\d\.]+)s', line)
            if quarter_4_match and not race_info['quarter_4']:
                race_info['quarter_4'] = f"{quarter_4_match.group(1)}s"

        return race_info

    def _extract_track_from_filename(self, file_path: str) -> Optional[str]:
        """Extract track name from filename patterns for multiple states"""
        import os
        filename = os.path.basename(file_path)
        base_name = os.path.splitext(filename)[0]

        # Common filename patterns across states
        # Examples: "Ararat_01032024.pdf", "BA110424_20240411.pdf", "Ballarat_01052024.pdf", "MX130925_20250913.pdf"

        # Handle underscore-separated patterns first
        if '_' in base_name:
            potential_track = base_name.split('_')[0]
            # If the part before underscore contains numbers, extract just the letters
            # e.g., "MX130925" -> "MX", "GE120925" -> "GE"
            letters_match = re.match(r'^([A-Za-z]+)', potential_track)
            if letters_match:
                potential_track = letters_match.group(1)
        else:
            # Handle patterns without underscore like "MX130925", "GE120925"
            # Extract letters from start of filename (before numbers)
            match = re.match(r'^([A-Za-z]+)', base_name)
            potential_track = match.group(1) if match else base_name

        # Map abbreviations to full names for all states
        track_mapping = {
            # VIC tracks
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
            'SW': 'Swan Hill',
            # NSW tracks
            'PE': 'Penrith',
            'TA': 'Tamworth',
            'WA': 'Wagga',
            'BA': 'Bathurst',
            'DU': 'Dubbo',
            # QLD tracks
            'AL': 'Albion Park',
            'RE': 'Redcliffe',
            'CA': 'Capalaba',
            'MA': 'Marburg',
            'RO': 'Rockhampton',
            # SA tracks
            'GL': 'Globe Derby',
            'GA': 'Gawler',
            # TAS tracks
            'HO': 'Hobart',
            'LA': 'Launceston',
            'DE': 'Devonport'
        }

        track_name = track_mapping.get(potential_track, potential_track)

        # Validate track name (should be alphabetic and reasonable length)
        if track_name and track_name.replace(' ', '').isalpha() and 3 <= len(track_name) <= 25:
            return track_name

        # Try other filename patterns (track name at start without underscore)
        # Remove common date patterns and numbers
        clean_name = re.sub(r'\d{4,8}', '', base_name)  # Remove dates like 20240315
        clean_name = re.sub(r'[-_].*$', '', clean_name)  # Remove everything after first dash/underscore

        if clean_name and clean_name.replace(' ', '').isalpha() and 3 <= len(clean_name) <= 25:
            return clean_name

        return None
    
    def _extract_pj_qld_from_table(self, table: List[List[str]], race_info: Dict) -> List[Dict[str, Any]]:
        """Extract QLD PJ data from table structure"""
        runners = []
        
        if not table or len(table) < 2:
            return runners
        
        # Skip header row and process data rows
        for row in table[1:]:
            if not row or len(row) < 7:  # Need at least 7 columns for Marburg format
                continue
            
            # Skip empty rows
            if not any(cell and cell.strip() for cell in row):
                continue
            
            try:
                # Marburg format columns:
                # 0: Tab Number, 1: Horse Name, 2: Place, 3: Margin
                # 4: 800m Position, 5: 400m Position, 6: Time, 7: 3rd Qtr, 8: 4th Qtr
                
                # Clean up horse name - remove newlines and extra spaces
                horse_name = row[1].strip() if row[1] else None
                if horse_name:
                    horse_name = ' '.join(horse_name.split())  # Replace newlines/multiple spaces with single space
                
                # Parse position data to extract distance and width
                # Format: "20.6m (1)" -> distance: 20.6m, width: 1
                pos_800 = row[4].strip() if row[4] else None
                pos_400 = row[5].strip() if row[5] else None
                
                # Extract width from position strings
                width_800 = None
                width_400 = None
                
                if pos_800 and '(' in pos_800:
                    # Extract width value from parentheses
                    import re
                    width_match = re.search(r'\((\d+)\)', pos_800)
                    if width_match:
                        width_800 = int(width_match.group(1))
                
                if pos_400 and '(' in pos_400:
                    width_match = re.search(r'\((\d+)\)', pos_400)
                    if width_match:
                        width_400 = int(width_match.group(1))
                
                runner_data = {
                    'tab_number': int(row[0]) if row[0] and row[0].isdigit() else None,
                    'horse_name': horse_name,
                    'finish_position': int(row[2]) if row[2] and row[2].isdigit() else None,
                    'margin': row[3].strip() if row[3] else None,
                    # For QLD PJ format, use time_800m and time_400m for the sectional times
                    'time_800m': row[7].strip() if len(row) > 7 and row[7] else None,  # 3rd quarter time
                    'time_400m': row[8].strip() if len(row) > 8 and row[8] else None,  # 4th quarter time
                    'width_800m': width_800,  # Width at 800m
                    'width_400m': width_400,  # Width at 400m
                    'final_time': row[6].strip() if row[6] else None,  # Actual finish time
                    # Store position data separately for reference
                    'position_800m': pos_800,
                    'position_400m': pos_400
                }
                
                # Only add if we have essential data
                if runner_data['tab_number'] and runner_data['horse_name']:
                    runners.append(runner_data)
                    
            except (ValueError, IndexError):
                continue
        
        return runners
    
    def _extract_pj_qld_from_text(self, text: str, race_info: Dict) -> List[Dict[str, Any]]:
        """Fallback text-based extraction for QLD PJ format"""
        import re
        runners = []
        lines = text.split('\n')
        
        # Check if this is the Albion Park 2024 format (horse names on separate lines)
        is_albion_2024_format = False
        for line in lines[:20]:  # Check first 20 lines
            if 'NoHorse Plc Mar 800 Posi 400 Posi Time 3rd Qtr 4th Qt' in line:
                is_albion_2024_format = True
                break
        
        if is_albion_2024_format:
            # Special handling for Albion Park 2024 format
            i = 0
            while i < len(lines):
                line = lines[i].strip()
                
                # Look for horse name lines (non-digit start, not headers)
                if line and not line[0].isdigit() and 'Race' not in line and 'Gross' not in line and 'DATA TABLE' not in line and 'NoHorse' not in line:
                    # Extract horse name (remove any trailing descriptive text)
                    horse_name = line.split(' First Arrow')[0].split(' Second Arrow')[0].split(' Blue arrow')[0].strip()
                    
                    # Check next line for data
                    if i + 1 < len(lines):
                        next_line = lines[i + 1].strip()
                        # Pattern: TabNo Plc Margin 800Posi 400Posi Time 3rdQtr 4thQtr [extras]
                        match = re.match(r'^(\d+)\s+(\d+)\s+([\d\.]+m)\s+([\d\.]+m)\s+\((\d+)\)\s+([\d\.]+m)\s+\((\d+)\)\s+(\d:\d{2}\.\d{2})\s+([\d\.]+)s\s+([\d\.]+)s', next_line)
                        if match:
                            runners.append({
                                'tab_number': int(match.group(1)),
                                'horse_name': horse_name,
                                'finish_position': int(match.group(2)),
                                'margin': match.group(3),
                                'position_800m': f"{match.group(4)} ({match.group(5)})",
                                'position_400m': f"{match.group(6)} ({match.group(7)})",
                                'final_time': match.group(8),
                                'time_800m': f"{match.group(9)}s",  # 3rd Quarter
                                'time_400m': f"{match.group(10)}s"   # 4th Quarter
                            })
                            i += 1  # Skip the data line we just processed
                i += 1
            
            return runners
        
        # Original QLD format extraction
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
                
            # QLD Marburg format pattern: "6BEE GEES BANDIT 1 0.0m5.6m (1) 6.8m (1) 2:15.60 29.00s 29.89s"
            # Pattern: Number+Horse, Place, Margin+800_margin (800_width), 400_margin (400_width), Time, 3rd_Qtr, 4th_Qtr
            qld_marburg_match = re.match(r'^(\d+)([A-Z][A-Z\s&\-\'NZ]+?)\s+(\d+)\s+([\d\.]+m)([\d\.]+m)\s*\((\d+)\)\s+([\d\.]+m)\s*\((\d+)\)\s+(\d:\d{2}\.\d{2})\s+(\d{2}\.\d{2})s\s+(\d{2}\.\d{2})s', line)
            
            if qld_marburg_match:
                runners.append({
                    'tab_number': int(qld_marburg_match.group(1)),
                    'horse_name': qld_marburg_match.group(2).strip(),
                    'finish_position': int(qld_marburg_match.group(3)),
                    'margin': qld_marburg_match.group(4),
                    'margin_800m': qld_marburg_match.group(5),
                    'width_800m': int(qld_marburg_match.group(6)),
                    'margin_400m': qld_marburg_match.group(7),
                    'width_400m': int(qld_marburg_match.group(8)),
                    'final_time': qld_marburg_match.group(9),
                    'gross_time': qld_marburg_match.group(9),
                    'third_quarter': f"{qld_marburg_match.group(10)}s",
                    'fourth_quarter': f"{qld_marburg_match.group(11)}s"
                })
                continue
            
            # QLD 2022 format pattern: "3WEWILLSEEHOWWE 1 0.0m13.0m (0) 3.4m (1) 2:04.40 29.00s 28.95s"
            # Pattern: Number+Horse, Place, Margin, 800_margin (800_width), 400_margin (400_width), Time, 3rd_Qtr, 4th_Qtr  
            qld_2022_match = re.match(r'^(\d+)([A-Z][A-Z\s&\-\'NZ]+?)\s+(\d+)\s+([\d\.]+m)\s+([\d\.]+m)\s*\((\d+)\)\s+([\d\.]+m)\s*\((\d+)\)\s+(\d:\d{2}\.\d{2})\s+(\d{2}\.\d{2})s\s+(\d{2}\.\d{2})s', line)
            
            if qld_2022_match:
                runners.append({
                    'tab_number': int(qld_2022_match.group(1)),
                    'horse_name': qld_2022_match.group(2).strip(),
                    'finish_position': int(qld_2022_match.group(3)),
                    'margin': qld_2022_match.group(4),
                    'margin_800m': qld_2022_match.group(5),
                    'width_800m': int(qld_2022_match.group(6)),
                    'margin_400m': qld_2022_match.group(7),
                    'width_400m': int(qld_2022_match.group(8)),
                    'final_time': qld_2022_match.group(9),
                    'gross_time': qld_2022_match.group(9),  # Set gross_time to final_time
                    'third_quarter': f"{qld_2022_match.group(10)}s",
                    'fourth_quarter': f"{qld_2022_match.group(11)}s"
                })
                continue
            
            # QLD PJ older pattern from Albion Park data:
            # "1PARISIAN ROCKSTAR 1 0.0m 1:57.10 54.80s (0) 27.69s (0) +4.4"
            # Pattern: Number+Horse Name, Place, Margin, Time, 800m time (W), 400m time (W), optional position change
            qld_match = re.match(r'^(\d+)([A-Z][A-Z\s&\-\'NZ]+?)\s+(\d+)\s+([\d\.]+m)\s+(\d:\d{2}\.\d{2})\s+(\d{2}\.\d{2})s\s*\((\d+)\)\s+(\d{2}\.\d{2})s\s*\((\d+)\)(?:\s+([\+\-]?\d+\.\d+))?', line)
            
            if qld_match:
                runners.append({
                    'tab_number': int(qld_match.group(1)),
                    'horse_name': qld_match.group(2).strip(),
                    'finish_position': int(qld_match.group(3)),
                    'margin': qld_match.group(4),
                    'final_time': qld_match.group(5),
                    'time_800m': f"{qld_match.group(6)}s",
                    'width_800m': int(qld_match.group(7)),
                    'time_400m': f"{qld_match.group(8)}s", 
                    'width_400m': int(qld_match.group(9))
                })
                continue
                
            # QLD simpler format fallback - just tab, horse, place, margin, time
            simple_match = re.match(r'^(\d+)([A-Z][A-Z\s&\-\'NZ]+?)\s+(\d+)\s+([\d\.]+m)\s+(\d:\d{2}\.\d{2})', line)
            if simple_match:
                runners.append({
                    'tab_number': int(simple_match.group(1)),
                    'horse_name': simple_match.group(2).strip(),
                    'finish_position': int(simple_match.group(3)),
                    'margin': simple_match.group(4),
                    'final_time': simple_match.group(5)
                })
        
        return runners
    
    def _extract_pj_vic(self, pdf_obj) -> List[Dict[str, Any]]:
        """Extract PJ data specifically for VIC format - page by page processing"""
        all_runners = []
        
        # Process each page separately for VIC
        for page_num, page in enumerate(pdf_obj.pages):
            page_text = page.extract_text()
            
            # Extract race metadata from this page
            race_info = self._extract_pj_race_info_from_page(page_text, page_num + 1)
            
            # Skip if no race data found
            if not race_info.get('race_number'):
                continue
            
            # Extract table data from this page using table extraction first
            tables = page.extract_tables()
            page_runners = []
            
            # Try table-based extraction first
            if tables:
                page_runners = self._extract_pj_vic_from_table(tables[0], race_info)
            
            # If table extraction failed or returned no data, use text-based extraction
            if not page_runners:
                page_runners = self._extract_pj_vic_from_text(page_text, race_info)
            
            # Skip if no runners found
            if not page_runners:
                continue
            
            # Add all race metadata to each runner for this page/race
            for runner in page_runners:
                runner.update({
                    'race_number': race_info.get('race_number'),
                    'distance': race_info.get('distance'),
                    'date': race_info.get('date'),
                    'gross_time': race_info.get('gross_time'),
                    'mile_rate': race_info.get('mile_rate'),
                    'lead_time': race_info.get('lead_time'),
                    'quarter_1': race_info.get('quarter_1'),
                    'quarter_2': race_info.get('quarter_2'),
                    'quarter_3': race_info.get('quarter_3'),
                    'quarter_4': race_info.get('quarter_4')
                })
            
            all_runners.extend(page_runners)
        
        return all_runners
    
    def _extract_pj_vic_from_table(self, table: List[List[str]], race_info: Dict) -> List[Dict[str, Any]]:
        """Extract VIC PJ data from table structure"""
        runners = []
        
        if not table or len(table) < 2:
            return runners
        
        # Skip header row and process data rows
        for row in table[1:]:
            if not row or len(row) < 5:
                continue
            
            # Skip empty rows
            if not any(cell and cell.strip() for cell in row):
                continue
            
            try:
                runner_data = {
                    'tab_number': int(row[0]) if row[0] and row[0].isdigit() else None,
                    'horse_name': row[1].strip() if row[1] else None,
                    'finish_position': int(row[2]) if row[2] and row[2].isdigit() else None,
                    'margin': row[3].strip() if row[3] else None,
                }
                
                # Handle different VIC format variations
                if len(row) >= 9:
                    # 2025 format: ['3', 'American Alli', '1', '0.0m', '0.0m (0)', '0.0m (0)', '2:10.10', '29.20s', '28.50s']
                    # Parse 800m data (row[4]): "0.0m (0)" -> margin_800m = "0.0m", width_800m = "0"
                    margin_800m, width_800m = self._parse_combined_position_data(row[4])
                    # Parse 400m data (row[5]): "0.0m (0)" -> margin_400m = "0.0m", width_400m = "0" 
                    margin_400m, width_400m = self._parse_combined_position_data(row[5])
                    
                    runner_data.update({
                        'margin_800m': margin_800m,
                        'width_800m': width_800m,
                        'margin_400m': margin_400m,
                        'width_400m': width_400m,
                        'final_time': row[6].strip() if row[6] else None,
                        'third_quarter_seconds': row[7].strip() if row[7] else None,
                        'fourth_quarter_seconds': row[8].strip() if row[8] else None
                    })
                else:
                    # 2023 format or simpler format
                    runner_data['final_time'] = row[4].strip() if row[4] else None
                    
                    if len(row) > 5:
                        runner_data['width_800m'] = row[5].strip() if row[5] else None
                    if len(row) > 6:
                        runner_data['width_400m'] = row[6].strip() if row[6] else None
                    if len(row) > 7:
                        runner_data['third_quarter'] = row[7].strip() if row[7] else None
                    if len(row) > 8:
                        runner_data['fourth_quarter'] = row[8].strip() if row[8] else None
                
                # Only add if we have essential data
                if runner_data['tab_number'] and runner_data['horse_name']:
                    runners.append(runner_data)
                    
            except (ValueError, IndexError):
                continue
        
        return runners
    
    def _extract_pj_vic_from_text(self, text: str, race_info: Dict) -> List[Dict[str, Any]]:
        """Enhanced text-based extraction for VIC PJ format with improved patterns from TAS/QLD"""
        runners = []
        lines = text.split('\n')
        
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
            
            runner_data = None
                
            # VIC Warragul format - horse name concatenated with tab number
            # "6PERSHING 1 0.0m3.8m (1) 1.5m (1) 2:46.40 30.52s 30.49s"
            warragul_match = re.match(r'^(\d+)([A-Z][A-Z\s&\-\']+?)\s+(\d+)\s+([\d\.]+m)([\d\.]+m)\s*\((\d+)\)\s+([\d\.]+m)\s*\((\d+)\)\s+(\d:\d{2}\.\d{2})\s+(\d{2}\.\d{2})s\s+(\d{2}\.\d{2})s', line)
            
            if warragul_match:
                runner_data = {
                    'tab_number': int(warragul_match.group(1)),
                    'horse_name': warragul_match.group(2).strip(),
                    'finish_position': int(warragul_match.group(3)),
                    'margin': warragul_match.group(4),
                    'margin_800m': warragul_match.group(5),
                    'width_800m': int(warragul_match.group(6)),
                    'margin_400m': warragul_match.group(7),
                    'width_400m': int(warragul_match.group(8)),
                    'final_time': warragul_match.group(9),
                    'third_quarter': f"{warragul_match.group(10)}s",
                    'fourth_quarter': f"{warragul_match.group(11)}s",
                    'third_quarter_seconds': float(warragul_match.group(10)),
                    'fourth_quarter_seconds': float(warragul_match.group(11))
                }
            
            # VIC enhanced pattern similar to QLD Marburg format
            # "7ANOTHER HORSE 2 1.2m4.5m (1) 2.3m (2) 2:47.80 30.10s 31.20s"
            elif not runner_data:
                vic_enhanced_match = re.match(r'^(\d+)([A-Z][A-Z\s&\-\']+?)\s+(\d+)\s+([\d\.]+m)([\d\.]+m)\s*\((\d+)\)\s+([\d\.]+m)\s*\((\d+)\)\s+(\d:\d{2}\.\d{2})\s+([\d\.]+)s\s+([\d\.]+)s', line)
                
                if vic_enhanced_match:
                    runner_data = {
                        'tab_number': int(vic_enhanced_match.group(1)),
                        'horse_name': vic_enhanced_match.group(2).strip(),
                        'finish_position': int(vic_enhanced_match.group(3)),
                        'margin': vic_enhanced_match.group(4),
                        'margin_800m': vic_enhanced_match.group(5),
                        'width_800m': int(vic_enhanced_match.group(6)),
                        'margin_400m': vic_enhanced_match.group(7),
                        'width_400m': int(vic_enhanced_match.group(8)),
                        'final_time': vic_enhanced_match.group(9),
                        'third_quarter': f"{vic_enhanced_match.group(10)}s",
                        'fourth_quarter': f"{vic_enhanced_match.group(11)}s",
                        'third_quarter_seconds': float(vic_enhanced_match.group(10)),
                        'fourth_quarter_seconds': float(vic_enhanced_match.group(11))
                    }
            
            # VIC standard format: "3 1 ANOTHER NAME 1.5m 2.1m (1) 1.8m (2) 2:45.90 29.80s 30.10s"
            elif not runner_data:
                vic_standard_match = re.match(r'^(\d+)\s+(\d+)\s+([A-Z][A-Z\s&\-\']+?)\s+([\d\.]+m)\s+([\d\.]+m)\s*\((\d+)\)\s+([\d\.]+m)\s*\((\d+)\)\s+(\d:\d{2}\.\d{2})\s+([\d\.]+)s\s+([\d\.]+)s', line)
                
                if vic_standard_match:
                    runner_data = {
                        'finish_position': int(vic_standard_match.group(1)),
                        'tab_number': int(vic_standard_match.group(2)),
                        'horse_name': vic_standard_match.group(3).strip(),
                        'margin': vic_standard_match.group(4),
                        'margin_800m': vic_standard_match.group(5),
                        'width_800m': int(vic_standard_match.group(6)),
                        'margin_400m': vic_standard_match.group(7),
                        'width_400m': int(vic_standard_match.group(8)),
                        'final_time': vic_standard_match.group(9),
                        'third_quarter': f"{vic_standard_match.group(10)}s",
                        'fourth_quarter': f"{vic_standard_match.group(11)}s",
                        'third_quarter_seconds': float(vic_standard_match.group(10)),
                        'fourth_quarter_seconds': float(vic_standard_match.group(11))
                    }
                    
            # Add runner if we found valid data
            if runner_data:
                runners.append(runner_data)
                continue
                
            # VIC Ararat 2025 format - horse name in separate line above data
            # Data line: "3 1 0.0m 0.0m (0) 0.0m (0) 2:10.10 29.20s 28.50s 0.0 0.0"
            # Horse name line above: "American Alli"
            ararat_2025_match = re.match(r'^(\d+)\s+(\d+)\s+([\d\.]+m)\s+([\d\.]+m)\s*\((\d+)\)\s+([\d\.]+m)\s*\((\d+)\)\s+(\d:\d{2}\.\d{2})\s+(\d{2}\.\d{2})s\s+(\d{2}\.\d{2})s(?:\s+[\d\.\-\+]+\s+[\d\.\-\+]+)?', line)
            
            if ararat_2025_match:
                tab_number = int(ararat_2025_match.group(1))
                
                # VIC specific horse name extraction - look for tab number pattern
                horse_name = None
                for j in range(max(0, i-10), i):
                    prev_line = lines[j].strip()
                    
                    # Look for the specific VIC pattern where tab number appears on its own line
                    if prev_line == str(tab_number):
                        # Horse name should be on the next line(s)
                        for k in range(j+1, min(j+5, len(lines))):
                            potential_name_line = lines[k].strip()
                            
                            # Check if it looks like a horse name
                            if (re.match(r'^[A-Z][A-Za-z\s&\-\']+$', potential_name_line) and 
                                len(potential_name_line) > 2 and 
                                potential_name_line not in {'DATA TABLE', 'NO HORSE', 'HORSE', 'POSITION', 'METRES', 'FINISH POSITION'} and
                                not re.match(r'^\d+', potential_name_line)):  # Not starting with a number
                                horse_name = potential_name_line
                                break
                        break
                
                # VIC fallback - look directly above the data line
                if not horse_name and i > 0:
                    prev_line = lines[i-1].strip()
                    if (re.match(r'^[A-Z][A-Za-z\s&\-\']+$', prev_line) and len(prev_line) > 2 and
                        prev_line not in {'DATA TABLE', 'NO HORSE', 'HORSE', 'POSITION', 'METRES'}):
                        horse_name = prev_line
                
                runners.append({
                    'tab_number': tab_number,
                    'horse_name': horse_name,
                    'finish_position': int(ararat_2025_match.group(2)),
                    'margin': ararat_2025_match.group(3),
                    'margin_800m': ararat_2025_match.group(4),
                    'width_800m': int(ararat_2025_match.group(5)),
                    'margin_400m': ararat_2025_match.group(6),
                    'width_400m': int(ararat_2025_match.group(7)),
                    'final_time': ararat_2025_match.group(8),
                    'third_quarter': f"{ararat_2025_match.group(9)}s",
                    'fourth_quarter': f"{ararat_2025_match.group(10)}s",
                    'third_quarter_seconds': float(ararat_2025_match.group(9)),
                    'fourth_quarter_seconds': float(ararat_2025_match.group(10))
                })
                continue
            
            # VIC Ararat variant - horse name in separate line above data (older format)
            # Data line: "6 1 0.0m 11.0m (1) 4.6m (2) 2:45.50 29.82s 30.84s"
            # Horse name line above: "JANES GEM"
            ararat_match = re.match(r'^(\d+)\s+(\d+)\s+([\d\.]+m)\s+([\d\.]+m)\s*\((\d+)\)\s+([\d\.]+m)\s*\((\d+)\)\s+(\d:\d{2}\.\d{2})\s+(\d{2}\.\d{2})s\s+(\d{2}\.\d{2})s', line)
            
            if ararat_match:
                tab_number = int(ararat_match.group(1))
                
                # VIC specific horse name extraction - look for tab number pattern
                horse_name = None
                for j in range(max(0, i-10), i):
                    prev_line = lines[j].strip()
                    
                    # Look for the specific VIC pattern where tab number appears on its own line
                    if prev_line == str(tab_number):
                        # Horse name should be on the next line(s)
                        for k in range(j+1, min(j+5, len(lines))):
                            potential_name_line = lines[k].strip()
                            
                            # VIC horse name validation - must be all caps, no numbers at start
                            if re.match(r'^[A-Z][A-Z\s&\-\']+$', potential_name_line) and len(potential_name_line) > 2:
                                # VIC specific exclusions
                                excluded_words = {
                                    'DATA TABLE', 'NO HORSE', 'HORSE', 'PLC', 'MAR', 'TIME', 'QTR', 'POSI',
                                    'FIRST', 'SECOND', 'THIRD', 'FOURTH', 'FINISH', 'POSITION', 'METRES',
                                    'GAINED', 'VISUAL', 'DIGITAL', 'RACE', 'DISTANCE', 'SUNDAY', 'ARARAT'
                                }
                                if (potential_name_line not in excluded_words and 
                                    not any(word in potential_name_line for word in ['TABLE', 'DATA', 'POSITION', 'RACE'])):
                                    horse_name = potential_name_line
                                    break
                        if horse_name:
                            break
                
                # VIC fallback - look directly above the data line
                if not horse_name and i > 0:
                    prev_line = lines[i-1].strip()
                    if (re.match(r'^[A-Z][A-Z\s&\-\']+$', prev_line) and len(prev_line) > 2 and
                        prev_line not in {'DATA TABLE', 'NO HORSE', 'HORSE', 'POSITION', 'METRES'}):
                        horse_name = prev_line
                
                runners.append({
                    'tab_number': tab_number,
                    'horse_name': horse_name,
                    'finish_position': int(ararat_match.group(2)),
                    'margin': ararat_match.group(3),
                    'width_800m': ararat_match.group(4),
                    'width_800m_position': int(ararat_match.group(5)),
                    'width_400m': ararat_match.group(6), 
                    'width_400m_position': int(ararat_match.group(7)),
                    'final_time': ararat_match.group(8),
                    'third_quarter': f"{ararat_match.group(9)}s",
                    'fourth_quarter': f"{ararat_match.group(10)}s"
                })
                continue
                
            # VIC standard PJ format fallback
            vic_standard_match = re.match(r'^(\d+)\s*([A-Z][A-Z\s&\-\']+?)\s+(\d+)\s+([\d\.]+m)\s+(\d:\d{2}\.\d{2})\s+(\d{2}\.\d{2})s\s*\((\d+)\)\s+(\d{2}\.\d{2})s\s*\((\d+)\)', line)
            if vic_standard_match:
                runners.append({
                    'tab_number': int(vic_standard_match.group(1)),
                    'horse_name': vic_standard_match.group(2).strip(),
                    'finish_position': int(vic_standard_match.group(3)),
                    'margin': vic_standard_match.group(4),
                    'final_time': vic_standard_match.group(5),
                    'time_800m': f"{vic_standard_match.group(6)}s",
                    'width_800m': int(vic_standard_match.group(7)),
                    'time_400m': f"{vic_standard_match.group(8)}s", 
                    'width_400m': int(vic_standard_match.group(9))
                })
        
        return runners
    
    def _extract_pj_tas(self, pdf_obj_or_text) -> List[Dict[str, Any]]:
        """Extract PJ data specifically for TAS format - based on QLD approach with First 100m support"""
        
        # Handle both PDF object and text input
        if hasattr(pdf_obj_or_text, 'pages'):
            # PDF object passed - use page by page processing like QLD
            pdf_obj = pdf_obj_or_text
            all_runners = []
            
            # Process each page separately for TAS
            for page_num, page in enumerate(pdf_obj.pages):
                page_text = page.extract_text()
                
                # Extract race metadata from this page
                race_info = self._extract_pj_race_info_from_page(page_text, page_num + 1)
                
                # Skip if no race data found
                if not race_info.get('race_number'):
                    continue
                
                # Extract table data from this page using table extraction
                tables = page.extract_tables()
                page_runners = []
                
                # Try table-based extraction first, fallback to text if no valid data
                if tables:
                    page_runners = self._extract_pj_tas_from_table(tables[0], race_info)
                
                # If table extraction failed or returned no data, use text-based extraction
                if not page_runners:
                    page_runners = self._extract_pj_tas_from_text(page_text, race_info)
                
                # Skip if no runners found
                if not page_runners:
                    continue
                
                # Add all race metadata to each runner for this page/race
                for runner in page_runners:
                    runner.update({
                        'race_number': race_info.get('race_number'),
                        'distance': race_info.get('distance'),
                        'date': race_info.get('date'),
                        'gross_time': race_info.get('gross_time'),
                        'mile_rate': race_info.get('mile_rate'),
                        'lead_time': race_info.get('lead_time'),
                        'quarter_1': race_info.get('quarter_1'),
                        'quarter_2': race_info.get('quarter_2'),
                        'quarter_3': race_info.get('quarter_3'),
                        'quarter_4': race_info.get('quarter_4')
                    })
                
                all_runners.extend(page_runners)
            
            return all_runners
        else:
            # Text passed - use simple text processing
            text = pdf_obj_or_text
            return self._extract_pj_tas_from_text(text, {})
    
    def _extract_pj_tas_from_table(self, table: List[List[str]], race_info: Dict) -> List[Dict[str, Any]]:
        """Extract TAS PJ data from table structure - based on QLD approach with First 100m detection"""
        runners = []
        
        if not table or len(table) < 2:
            return runners
        
        # Skip header row and process data rows
        for row in table[1:]:
            if not row or len(row) < 7:  # Need at least 7 columns for basic TAS format
                continue
            
            # Skip empty rows
            if not any(cell and cell.strip() for cell in row):
                continue
            
            try:
                # Clean up horse name - remove newlines and extra spaces
                horse_name = row[2].strip() if row[2] else None
                if horse_name:
                    horse_name = ' '.join(horse_name.split())  # Replace newlines/multiple spaces with single space
                
                # TAS format detection:
                # Standard: Plc | No | Horse | Margin(m) | 800 margin(m) | 800 width | 400 Width | Overall Time | 800m-400m | Last 400m
                # First 100m: + First 100m column (11 total)
                
                runner_data = {
                    'finish_position': int(row[0]) if row[0] and row[0].isdigit() else None,
                    'tab_number': int(row[1]) if row[1] and row[1].isdigit() else None,
                    'horse_name': horse_name,
                    'margin': row[3].strip() if len(row) > 3 and row[3] else None,
                    'margin_800m': row[4].strip() if len(row) > 4 and row[4] else None,
                    'width_800m': int(row[5]) if len(row) > 5 and row[5] and row[5].isdigit() else None,
                    'width_400m': int(row[6]) if len(row) > 6 and row[6] and row[6].isdigit() else None,
                    'final_time': row[7].strip() if len(row) > 7 and row[7] else None,
                    'time_800m': row[8].strip() if len(row) > 8 and row[8] else None,  # 800m-400m split
                    'time_400m': row[9].strip() if len(row) > 9 and row[9] else None,   # Last 400m
                }
                
                # Check for First 100m column (11+ columns)
                if len(row) >= 11 and row[10] and row[10].strip():
                    runner_data['first_100m'] = row[10].strip()
                
                # Only add if we have essential data
                if runner_data.get('tab_number') and runner_data.get('horse_name'):
                    runners.append(runner_data)
                    
            except (ValueError, IndexError):
                continue
        
        return runners
    
    def _identify_tas_format(self, text: str) -> str:
        """Identify TAS PDF format based on text structure and header patterns"""
        import re
        lines = text.split('\n')
        
        # Look for format-specific indicators
        for line in lines:
            line = line.strip()
            
            # Format 3 (2025): Look for "800 Posi 400 Posi Time 3rd Qtr 4th Qtr"
            if re.search(r'800\s+Posi\s+400\s+Posi\s+Time\s+3rd\s+Qtr\s+4th\s+Qtr', line):
                return 'format_2025'
            
            # Format 2 (2021): Look for "800Time(W) 400Time(W)First100m" pattern
            if re.search(r'800Time\(W\)\s+400Time\(W\)First100m', line):
                return 'format_2021'
            
            # Format 1 (2020): Look for traditional "First 800m- Last 400m" pattern
            if re.search(r'First\s+800m-\s+Last\s+400m', line) or re.search(r'800m-\s+Last\s+400m', line):
                return 'format_2020'
        
        # Fallback: check data line patterns
        for line in lines:
            line = line.strip()
            if not line or not re.match(r'^\d', line):
                continue
                
            # Format 2025 pattern: "1Cincinnati 1 0.0m0.0m (0) 0.0m (0) 2:04.00 29.90s 30.40s"
            if re.match(r'^\d+[A-Za-z\s&\-\']+\s+\d+\s+[\d\.]+m[\d\.]+m\s+\(\d+\)\s+[\d\.]+m\s+\(\d+\)\s+\d:\d{2}\.\d{2}\s+[\d\.]+s\s+[\d\.]+s', line):
                return 'format_2025'
            
            # Format 2021 pattern: "1SPRING QUEEN 1 0.0m2:45.20 60.40s (0) 30.20s (0) 6.65s"  
            if re.match(r'^\d+[A-Za-z\s&\-\']+\s+\d+\s+[\d\.]+m\d:\d{2}\.\d{2}\s+[\d\.]+s\s+\(\d+\)\s+[\d\.]+s\s+\(\d+\)\s+[\d\.]+s', line):
                return 'format_2021'
            
            # Format 2020 pattern: "1 2 MACH CHARM 0 6.2 1 2 2:45.10 6.81s 28.55s 29.90s"
            if re.match(r'^\d+\s+\d+\s+[A-Za-z\s&\-\']+\s+[\d\.]+\s+[\d\.]+\s+\d+\s+\d+\s+\d:\d{2}\.\d{2}\s+[\d\.]+s\s+[\d\.]+s(?:\s+[\d\.]+s)?', line):
                return 'format_2020'
                
            # Only check first few data lines
            break
            
        return 'unknown'
    
    def _extract_tas_format_2025(self, text: str, race_info: Dict) -> List[Dict[str, Any]]:
        """Extract Format 3 (2025): NoHorse Plc Mar 800 Posi 400 Posi Time 3rd Qtr 4th Qtr"""
        import re
        runners = []
        lines = text.split('\n')
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # Pattern: "1Cincinnati 1 0.0m0.0m (0) 0.0m (0) 2:04.00 29.90s 30.40s"
            match = re.match(r'^(\d+)([A-Za-z\s&\-\']+?)\s+(\d+)\s+([\d\.]+m)([\d\.]+m)\s+\((\d+)\)\s+([\d\.]+m)\s+\((\d+)\)\s+(\d:\d{2}\.\d{2})\s+([\d\.]+)s\s+([\d\.]+)s', line)
            if match:
                runner_data = {
                    'tab_number': int(match.group(1)),
                    'horse_name': match.group(2).strip(),
                    'finish_position': int(match.group(3)),
                    'margin': match.group(4),
                    'margin_800m': match.group(5),
                    'width_800m': int(match.group(6)),
                    'margin_400m': match.group(7),
                    'width_400m': int(match.group(8)),
                    'final_time': match.group(9),
                    'time_800m': f"{match.group(10)}s",  # 3rd Quarter
                    'time_400m': f"{match.group(11)}s"   # 4th Quarter
                }
                runners.append(runner_data)
        
        return runners
    
    def _extract_tas_format_2021(self, text: str, race_info: Dict) -> List[Dict[str, Any]]:
        """Extract Format 2 (2021): NoHorse Plc Margin Time 800Time(W) 400Time(W)[First100m]"""
        import re
        runners = []
        lines = text.split('\n')
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # Pattern WITH First100m: "1SPRING QUEEN 1 0.0m2:45.20 60.40s (0) 30.20s (0) 6.65s"
            match = re.match(r'^(\d+)([A-Za-z\s&\-\']+?)\s+(\d+)\s+([\d\.]+m)(\d:\d{2}\.\d{2})\s+([\d\.]+)s\s+\((\d+)\)\s+([\d\.]+)s\s+\((\d+)\)\s+([\d\.]+)s', line)
            if match:
                runner_data = {
                    'tab_number': int(match.group(1)),
                    'horse_name': match.group(2).strip(),
                    'finish_position': int(match.group(3)),
                    'margin': match.group(4),
                    'final_time': match.group(5),
                    'time_800m': f"{match.group(6)}s",  # 800Time
                    'width_800m': int(match.group(7)),
                    'time_400m': f"{match.group(8)}s",  # 400Time
                    'width_400m': int(match.group(9)),
                    'first_100m': f"{match.group(10)}s"
                }
                runners.append(runner_data)
                continue
            
            # Pattern WITHOUT First100m: "6TARIFA GIRL 1 0.0m2:41.10 58.20s (0) 29.50s (0)"
            match = re.match(r'^(\d+)([A-Za-z\s&\-\']+?)\s+(\d+)\s+([\d\.]+m)(\d:\d{2}\.\d{2})\s+([\d\.]+)s\s+\((\d+)\)\s+([\d\.]+)s\s+\((\d+)\)$', line)
            if match:
                runner_data = {
                    'tab_number': int(match.group(1)),
                    'horse_name': match.group(2).strip(),
                    'finish_position': int(match.group(3)),
                    'margin': match.group(4),
                    'final_time': match.group(5),
                    'time_800m': f"{match.group(6)}s",  # 800Time
                    'width_800m': int(match.group(7)),
                    'time_400m': f"{match.group(8)}s",  # 400Time
                    'width_400m': int(match.group(9))
                    # No first_100m in this variant
                }
                runners.append(runner_data)
        
        return runners
    
    def _extract_tas_format_2020(self, text: str, race_info: Dict) -> List[Dict[str, Any]]:
        """Extract Format 1 (2020): Plc No Horse Margin(m) 800margin(m) 800width 400Width OverallTime 800m-400m Last400m [First100m]"""
        import re
        runners = []
        lines = text.split('\n')
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # Pattern with First 100m: "1 2 MACH CHARM 0 6.2 1 2 2:45.10 6.81s 28.55s 29.90s"
            # Column order: Plc No Horse Margin 800margin 800width 400Width OverallTime First100m 800m-400m Last400m
            match = re.match(r'^(\d+)\s+(\d+)\s+([A-Za-z\s&\-\']+?)\s+([\d\.]+)\s+([\d\.]+)\s+(\d+)\s+(\d+)\s+(\d:\d{2}\.\d{2})\s+([\d\.]+)s\s+([\d\.]+)s\s+([\d\.]+)s', line)
            if match:
                runner_data = {
                    'finish_position': int(match.group(1)),
                    'tab_number': int(match.group(2)),
                    'horse_name': match.group(3).strip(),
                    'margin': f"{match.group(4)}m",
                    'margin_800m': f"{match.group(5)}m",
                    'width_800m': int(match.group(6)),
                    'width_400m': int(match.group(7)),
                    'final_time': match.group(8),
                    'first_100m': f"{match.group(9)}s",   # First 100m (comes first in data)
                    'time_800m': f"{match.group(10)}s",   # 800m-400m split (second)
                    'time_400m': f"{match.group(11)}s"    # Last 400m (third)
                }
                runners.append(runner_data)
                continue
                
            # Pattern without First 100m: "1 2 MACH CHARM 0 6.2 1 2 2:45.10 6.81s 28.55s"
            match = re.match(r'^(\d+)\s+(\d+)\s+([A-Za-z\s&\-\']+?)\s+([\d\.]+)\s+([\d\.]+)\s+(\d+)\s+(\d+)\s+(\d:\d{2}\.\d{2})\s+([\d\.]+)s\s+([\d\.]+)s', line)
            if match:
                runner_data = {
                    'finish_position': int(match.group(1)),
                    'tab_number': int(match.group(2)),
                    'horse_name': match.group(3).strip(),
                    'margin': f"{match.group(4)}m",
                    'margin_800m': f"{match.group(5)}m",
                    'width_800m': int(match.group(6)),
                    'width_400m': int(match.group(7)),
                    'final_time': match.group(8),
                    'time_800m': f"{match.group(9)}s",
                    'time_400m': f"{match.group(10)}s"
                }
                runners.append(runner_data)
        
        return runners
    
    def _extract_pj_tas_from_text(self, text: str, race_info: Dict) -> List[Dict[str, Any]]:
        """Enhanced TAS extraction with automatic format detection"""
        import re
        
        # Identify format first
        format_type = self._identify_tas_format(text)
        
        if format_type == 'format_2025':
            return self._extract_tas_format_2025(text, race_info)
        elif format_type == 'format_2021':
            return self._extract_tas_format_2021(text, race_info)
        elif format_type == 'format_2020':
            return self._extract_tas_format_2020(text, race_info)
        else:
            # Fallback to old pattern-based extraction
            return self._extract_tas_fallback_patterns(text, race_info)
    
    def _extract_tas_fallback_patterns(self, text: str, race_info: Dict) -> List[Dict[str, Any]]:
        """Fallback pattern-based extraction for unknown formats"""
        import re
        runners = []
        lines = text.split('\n')
        
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
                
            runner_data = None
            
            # Test patterns in order from most specific to least specific
            
            # TAS 2022 format: "2WESTRAY 1 0.0m2:41.40 61.22s (0) 30.70s (0) 7.18s"
            # Pattern: NoHorse Plc Margin Time 800Time(W) 400Time(W) First100m
            tas_2022_match = re.match(r'^(\d+)([A-Z][A-Za-z\s&\-\']+?)\s+(\d+)\s+([\d\.]+m)(\d:\d{2}\.\d{2})\s+([\d\.]+)s\s+\((\d+)\)\s+([\d\.]+)s\s+\((\d+)\)\s+([\d\.]+)s', line)
            if tas_2022_match:
                runner_data = {
                    'tab_number': int(tas_2022_match.group(1)),
                    'horse_name': tas_2022_match.group(2).strip(),
                    'finish_position': int(tas_2022_match.group(3)),
                    'margin': tas_2022_match.group(4),
                    'final_time': tas_2022_match.group(5),
                    'time_800m': f"{tas_2022_match.group(6)}s",  # 800Time
                    'width_800m': int(tas_2022_match.group(7)),   # Width at 800m
                    'time_400m': f"{tas_2022_match.group(8)}s",  # 400Time 
                    'width_400m': int(tas_2022_match.group(9)),   # Width at 400m
                    'first_100m': f"{tas_2022_match.group(10)}s" # First 100m timing
                }
            
            # TAS 2024 format: "1IDEAL SON 1 0.0m0.0m (0) 0.0m (0) 2:39.30 29.90s 30.10s"
            # Pattern: NoHorse Plc Mar 800 Posi 400 Posi Time 3rd Qtr 4th Qtr
            elif not runner_data:
                tas_2024_match = re.match(r'^(\d+)([A-Z][A-Za-z\s&\-\']+?)\s+(\d+)\s+([\d\.]+m)([\d\.]+m)\s+\((\d+)\)\s+([\d\.]+m)\s+\((\d+)\)\s+(\d:\d{2}\.\d{2})\s+([\d\.]+)s\s+([\d\.]+)s', line)
                if tas_2024_match:
                    runner_data = {
                        'tab_number': int(tas_2024_match.group(1)),
                        'horse_name': tas_2024_match.group(2).strip(),
                        'finish_position': int(tas_2024_match.group(3)),
                        'margin': tas_2024_match.group(4),
                        'margin_800m': tas_2024_match.group(5),      # 800 margin
                        'width_800m': int(tas_2024_match.group(6)),  # 800 Position
                        'margin_400m': tas_2024_match.group(7),      # 400 margin  
                        'width_400m': int(tas_2024_match.group(8)),  # 400 Position
                        'final_time': tas_2024_match.group(9),
                        'time_800m': f"{tas_2024_match.group(10)}s", # 3rd Quarter (800m-400m split)
                        'time_400m': f"{tas_2024_match.group(11)}s"  # 4th Quarter (Last 400m)
                    }
            
            # TAS Launceston/Hobart subtype with First 100m (11 groups): "1 2 BRIDWOOD BELLA 0 0 0 0 2:02.60 6.95s 28.60s 28.90s"
            # Pattern: Plc No Horse Margin(m) 800margin(m) 800width 400Width OverallTime 800m-400m Last400m First100m
            elif not runner_data:
                tas_first100m_match = re.match(r'^(\d+)\s+(\d+)\s+([A-Z][A-Za-z\s&\-\']+?)\s+([\d\.]+)\s+([\d\.]+)\s+(\d+)\s+(\d+)\s+(\d:\d{2}\.\d{2})\s+([\d\.]+)s\s+([\d\.]+)s\s+([\d\.]+)s', line)
                if tas_first100m_match:
                    runner_data = {
                        'finish_position': int(tas_first100m_match.group(1)),
                        'tab_number': int(tas_first100m_match.group(2)),
                        'horse_name': tas_first100m_match.group(3).strip(),
                        'margin': f"{tas_first100m_match.group(4)}m",
                        'margin_800m': f"{tas_first100m_match.group(5)}m",  # Margin at 800m point
                        'width_800m': int(tas_first100m_match.group(6)),
                        'width_400m': int(tas_first100m_match.group(7)),
                        'final_time': tas_first100m_match.group(8),
                        'time_800m': f"{tas_first100m_match.group(9)}s",  # 800m-400m split maps to time_800m
                        'time_400m': f"{tas_first100m_match.group(10)}s",  # Last 400m maps to time_400m  
                        'first_100m': f"{tas_first100m_match.group(11)}s"   # First 100m timing
                    }
            
            # TAS Carrick 2020 format (10 groups): "1 1 HELIKAON 0 0 0 0 2:42.00 29.10s 28.40s" 
            elif not runner_data:
                tas_carrick_2020_match = re.match(r'^(\d+)\s+(\d+)\s+([A-Z][A-Za-z\s&\-\']+?)\s+([\d\.]+)\s+([\d\.]+)\s+(\d+)\s+(\d+)\s+(\d:\d{2}\.\d{2})\s+([\d\.]+)s\s+([\d\.]+)s', line)
                if tas_carrick_2020_match:
                    runner_data = {
                        'finish_position': int(tas_carrick_2020_match.group(1)),
                        'tab_number': int(tas_carrick_2020_match.group(2)),
                        'horse_name': tas_carrick_2020_match.group(3).strip(),
                        'margin': f"{tas_carrick_2020_match.group(4)}m",
                        'margin_800m': f"{tas_carrick_2020_match.group(5)}m",
                        'width_800m': int(tas_carrick_2020_match.group(6)),
                        'width_400m': int(tas_carrick_2020_match.group(7)),
                        'final_time': tas_carrick_2020_match.group(8),
                        'time_800m': f"{tas_carrick_2020_match.group(9)}s",  # Use QLD-style column names
                        'time_400m': f"{tas_carrick_2020_match.group(10)}s"   # Use QLD-style column names
                    }
            
            # TAS Hobart format: "1IDEN BLACK PRINCE 1 0.0m7.0m (0) 4.0m (0) 2:40.90 28.59s 30.59s"
            elif not runner_data:
                tas_hobart_match = re.match(r'^(\d+)([A-Z][A-Za-z\s&\-\']+?)\s+(\d+)\s+([\d\.]+m)([\d\.]+m)\s*\((\d+)\)\s+([\d\.]+m)\s*\((\d+)\)\s+(\d:\d{2}\.\d{2})\s+(\d{2}\.\d{2})s\s+(\d{2}\.\d{2})s', line)
                if tas_hobart_match:
                    runner_data = {
                        'tab_number': int(tas_hobart_match.group(1)),
                        'horse_name': tas_hobart_match.group(2).strip(),
                        'finish_position': int(tas_hobart_match.group(3)),
                        'margin': tas_hobart_match.group(4),
                        'margin_800m': tas_hobart_match.group(5),  # Distance from leader at 800m
                        'width_800m': int(tas_hobart_match.group(6)),  # Position width at 800m
                        'margin_400m': tas_hobart_match.group(7),  # Distance from leader at 400m
                        'width_400m': int(tas_hobart_match.group(8)),  # Position width at 400m
                        'final_time': tas_hobart_match.group(9),
                        'time_800m': f"{tas_hobart_match.group(10)}s",  # Use QLD-style column names
                        'time_400m': f"{tas_hobart_match.group(11)}s"  # Use QLD-style column names
                    }
            
            # TAS standard format: need to define this pattern
            elif not runner_data:
                tas_standard_match = re.match(r'^(\d+)\s+([A-Z][A-Za-z\s&\-\']+?)\s+(\d+)\s+([\d\.]+m)\s+(\d:\d{2}\.\d{2})\s+([\d\.]+)s\s+(\d+)\s+([\d\.]+)s\s+(\d+)(?:\s+([\d\.]+)s)?', line)
                if tas_standard_match:
                    runner_data = {
                        'tab_number': int(tas_standard_match.group(1)),
                        'horse_name': tas_standard_match.group(2).strip(),
                        'finish_position': int(tas_standard_match.group(3)),
                        'margin': tas_standard_match.group(4),
                        'final_time': tas_standard_match.group(5),
                        'time_800m': f"{tas_standard_match.group(6)}s",  # Use QLD-style column names
                        'width_800m': int(tas_standard_match.group(7)),
                        'time_400m': f"{tas_standard_match.group(8)}s",  # Use QLD-style column names
                        'width_400m': int(tas_standard_match.group(9))
                    }
                    
                    # Add First 100m if it exists in the pattern
                    if tas_standard_match.group(10):
                        runner_data['first_100m'] = f"{tas_standard_match.group(10)}s"
            
            # Add runner data if valid
            if runner_data and runner_data.get('tab_number') and runner_data.get('horse_name'):
                runners.append(runner_data)
        
        return runners
    
    def _apply_post_extraction_column_conversions(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """No-op column conversions - TAS now uses standard column names from extraction"""
        
        # TAS now uses QLD-style column names from the start, so no conversion needed
        return extracted_data
    
    def _standardize_sa_runner_data(self, runner_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Standardize SA runner data to ensure consistent fields.
        Ensures all SA runners have both combined and granular width fields.
        """
        import re
        standardized = runner_data.copy()
        
        # Helper function to parse width fields
        def parse_width_field(field):
            if not field or str(field).strip() == '':
                return None, None, None
            
            field_str = str(field).strip()
            
            # Pattern 1: "4.4m (0)" - distance and position
            match = re.match(r'^([\d\.]+m)\s*\((\d+)\)$', field_str)
            if match:
                return match.group(1), int(match.group(2)), field_str
            
            # Pattern 2: Just distance "4.4m"
            match = re.match(r'^([\d\.]+m)$', field_str)
            if match:
                return match.group(1), None, field_str
            
            # Pattern 3: Just position "(0)" or "0"
            match = re.match(r'^\(?(\d+)\)?$', field_str)
            if match:
                return None, int(match.group(1)), field_str
            
            return None, None, field_str
        
        # Process width_800m field
        if 'width_800m' in standardized:
            width_800m = standardized['width_800m']
            
            # Check if we already have granular data
            if 'width_800m_distance' not in standardized or 'width_800m_position' not in standardized:
                distance, position, combined = parse_width_field(width_800m)
                
                # Set granular fields if not already present
                if 'width_800m_distance' not in standardized:
                    standardized['width_800m_distance'] = distance
                if 'width_800m_position' not in standardized:
                    standardized['width_800m_position'] = position
                
                # Ensure combined format is consistent
                if distance and position is not None:
                    standardized['width_800m'] = f"{distance} ({position})"
        
        # Process width_400m field
        if 'width_400m' in standardized:
            width_400m = standardized['width_400m']
            
            # Check if we already have granular data
            if 'width_400m_distance' not in standardized or 'width_400m_position' not in standardized:
                distance, position, combined = parse_width_field(width_400m)
                
                # Set granular fields if not already present
                if 'width_400m_distance' not in standardized:
                    standardized['width_400m_distance'] = distance
                if 'width_400m_position' not in standardized:
                    standardized['width_400m_position'] = position
                
                # Ensure combined format is consistent
                if distance and position is not None:
                    standardized['width_400m'] = f"{distance} ({position})"
        
        # Process time fields to ensure decimal seconds are available
        if 'third_quarter' in standardized and 'third_quarter_seconds' not in standardized:
            third_qtr = standardized['third_quarter']
            if third_qtr:
                match = re.search(r'([\d\.]+)', str(third_qtr))
                if match:
                    standardized['third_quarter_seconds'] = float(match.group(1))
        
        if 'fourth_quarter' in standardized and 'fourth_quarter_seconds' not in standardized:
            fourth_qtr = standardized['fourth_quarter']
            if fourth_qtr:
                match = re.search(r'([\d\.]+)', str(fourth_qtr))
                if match:
                    standardized['fourth_quarter_seconds'] = float(match.group(1))
        
        # Ensure all expected fields exist (set to None if missing)
        expected_fields = [
            'width_800m_distance', 'width_800m_position',
            'width_400m_distance', 'width_400m_position',
            'third_quarter_seconds', 'fourth_quarter_seconds',
            'distance_gained_800_to_400'
        ]
        
        for field in expected_fields:
            if field not in standardized:
                standardized[field] = None
        
        return standardized

    def _extract_sa_multi_race_format(self, page_text: str, page_num: int) -> List[Dict[str, Any]]:
        """
        Extract SA multi-race format where multiple races appear on one page.
        Format: Race No. X Distance XXXXm
        """
        all_runners = []
        lines = page_text.split('\n')
        current_race_info = None
        
        # Track where each race starts and ends
        race_sections = []
        for i, line in enumerate(lines):
            # Look for race headers: "Race No. 1 Distance 1609m"
            race_match = re.match(r'^Race\s+No\.\s+(\d+)\s+Distance\s+(\d+)m', line.strip())
            if race_match:
                race_sections.append({
                    'line_index': i,
                    'race_number': int(race_match.group(1)),
                    'distance': f"{race_match.group(2)}m"
                })
        
        # Extract venue and date from header
        venue = None
        date = None
        for line in lines[:5]:  # Check first few lines
            if 'Sectional information' in line:
                # Pattern: "Sectional information Gawler Sunday, 17 October 2021"
                match = re.match(r'Sectional information\s+(.+?)\s+(.+)$', line.strip())
                if match:
                    venue = match.group(1)
                    date = match.group(2)
                    break
        
        # Process each race section
        for i, race_section in enumerate(race_sections):
            race_number = race_section['race_number']
            distance = race_section['distance']
            start_line = race_section['line_index']
            
            # Determine end line (start of next race or end of text)
            if i + 1 < len(race_sections):
                end_line = race_sections[i + 1]['line_index']
            else:
                end_line = len(lines)
            
            # Extract text for this race
            race_text = '\n'.join(lines[start_line:end_line])
            
            # Extract runners from this race section
            race_runners = self._extract_pj_sa_from_text(race_text, {})
            
            # Add race metadata to each runner
            for runner in race_runners:
                runner.update({
                    'race_number': race_number,
                    'distance': distance,
                    'track': venue,
                    'date': date
                })
                
            all_runners.extend(race_runners)
        
        return all_runners

    def _extract_pj_sa(self, pdf_obj_or_text) -> List[Dict[str, Any]]:
        """Extract PJ data specifically for SA format - handle both PDF objects and text"""
        
        # Handle both PDF object and text input
        if hasattr(pdf_obj_or_text, 'pages'):
            # PDF object passed - use page-by-page processing
            pdf_obj = pdf_obj_or_text
            all_runners = []
            
            # Process each page separately for SA
            for page_num, page in enumerate(pdf_obj.pages):
                page_text = page.extract_text()
                
                # Check if this is the multi-race format (Gawler style)
                race_count = len(re.findall(r'Race\s+No\.\s+\d+\s+Distance\s+\d+m', page_text))
                
                if race_count > 1:
                    # This is multi-race format - use special handler
                    page_runners = self._extract_sa_multi_race_format(page_text, page_num + 1)
                    
                    # Standardize each runner's data
                    standardized_runners = []
                    for runner in page_runners:
                        standardized_runner = self._standardize_sa_runner_data(runner)
                        standardized_runners.append(standardized_runner)
                    
                    all_runners.extend(standardized_runners)
                
                else:
                    # Standard single-race format
                    race_info = self._extract_pj_race_info_from_page(page_text, page_num + 1)
                    
                    # Skip if no race data found
                    if not race_info.get('race_number'):
                        continue
                    
                    # Extract table data from this page using table extraction first
                    tables = page.extract_tables()
                    page_runners = []
                    
                    # Try table-based extraction first
                    if tables:
                        page_runners = self._extract_pj_sa_from_table(tables[0], race_info)
                    
                    # If table extraction failed or returned no data, use text-based extraction
                    if not page_runners:
                        page_runners = self._extract_pj_sa_from_text(page_text, race_info)
                    
                    # Skip if no runners found
                    if not page_runners:
                        continue
                    
                    # Standardize each runner's data
                    standardized_runners = []
                    for runner in page_runners:
                        # Standardize the runner data
                        standardized_runner = self._standardize_sa_runner_data(runner)
                        
                        # Add all race metadata to each runner
                        standardized_runner.update({
                            'race_number': race_info.get('race_number'),
                            'distance': race_info.get('distance'),
                            'date': race_info.get('date'),
                            'track': race_info.get('track'),
                            'gross_time': race_info.get('gross_time'),
                            'mile_rate': race_info.get('mile_rate'),
                            'lead_time': race_info.get('lead_time'),
                            'quarter_1': race_info.get('quarter_1'),
                            'quarter_2': race_info.get('quarter_2'),
                            'quarter_3': race_info.get('quarter_3'),
                            'quarter_4': race_info.get('quarter_4')
                        })
                        
                        standardized_runners.append(standardized_runner)
                    
                    all_runners.extend(standardized_runners)
            
            return all_runners
        else:
            # Text passed - use simple text processing
            text = pdf_obj_or_text
            runners = self._extract_pj_sa_from_text(text, {})
            # Standardize each runner before returning
            return [self._standardize_sa_runner_data(runner) for runner in runners]
    
    def _extract_pj_sa_from_table(self, table: List[List[str]], race_info: Dict) -> List[Dict[str, Any]]:
        """Extract SA PJ data from table structure"""
        runners = []
        
        if not table or len(table) < 2:
            return runners
        
        # Skip header row and process data rows
        for row in table[1:]:
            if not row or len(row) < 5:
                continue
            
            # Skip empty rows
            if not any(cell and cell.strip() for cell in row):
                continue
            
            try:
                # SA specific parsing - horse name might be in separate cell
                horse_field = row[0].strip() if row[0] else ""
                tab_number = None
                horse_name = None
                
                # Check if first column is just a tab number
                if horse_field.isdigit():
                    tab_number = int(horse_field)
                    # Look for horse name in next cell or subsequent cells
                    for col_idx in range(1, min(len(row), 3)):
                        if row[col_idx] and row[col_idx].strip():
                            potential_name = row[col_idx].strip()
                            if re.match(r'^[A-Z][A-Za-z\s&\-\']+$', potential_name):
                                horse_name = potential_name
                                break
                
                # Adjust column mapping based on SA format
                finish_pos_col = 2 if horse_name else 1
                margin_col = 3 if horse_name else 2
                
                runner_data = {
                    'tab_number': tab_number,
                    'horse_name': horse_name,
                    'finish_position': int(row[finish_pos_col]) if len(row) > finish_pos_col and row[finish_pos_col] and row[finish_pos_col].isdigit() else None,
                    'margin': row[margin_col].strip() if len(row) > margin_col and row[margin_col] else None,
                    'width_800m': row[margin_col + 1].strip() if len(row) > margin_col + 1 and row[margin_col + 1] else None,
                    'width_400m': row[margin_col + 2].strip() if len(row) > margin_col + 2 and row[margin_col + 2] else None,
                    'final_time': row[margin_col + 3].strip() if len(row) > margin_col + 3 and row[margin_col + 3] else None,
                    'third_quarter': row[margin_col + 4].strip() if len(row) > margin_col + 4 and row[margin_col + 4] else None,
                    'fourth_quarter': row[margin_col + 5].strip() if len(row) > margin_col + 5 and row[margin_col + 5] else None
                }
                
                # Only add if we have essential data
                if runner_data['tab_number'] and runner_data['horse_name']:
                    runners.append(runner_data)
                    
            except (ValueError, IndexError):
                continue
        
        return runners
    
    def _extract_pj_sa_from_text(self, text: str, race_info: Dict) -> List[Dict[str, Any]]:
        """Fallback text-based extraction for SA PJ format"""
        runners = []
        lines = text.split('\n')
        
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
            
            # SA TAS-2020-style format: "1 5 YANKEE CLIPPER 0 0.4 1 1 2:13.40 29.77s 29.60s"
            sa_tas_match = re.match(r'^(\d+)\s+(\d+)\s+([A-Z][A-Za-z\s&\-\']+?)\s+([\d\.]+)\s+([\d\.]+)\s+(\d+)\s+(\d+)\s+(\d:\d{2}\.\d{2})\s+([\d\.]+)s\s+([\d\.]+)s', line)
            if sa_tas_match:
                runners.append({
                    'finish_position': int(sa_tas_match.group(1)),
                    'tab_number': int(sa_tas_match.group(2)), 
                    'horse_name': sa_tas_match.group(3).strip(),
                    'margin': f"{sa_tas_match.group(4)}m",
                    'margin_800m': f"{sa_tas_match.group(5)}m", 
                    'width_800m': int(sa_tas_match.group(6)),
                    'width_400m': int(sa_tas_match.group(7)),
                    'final_time': sa_tas_match.group(8),
                    'split_800m_400m': f"{sa_tas_match.group(9)}s",
                    'last_400m': f"{sa_tas_match.group(10)}s"
                })
                continue
            
            # SA Gawler format: "1 1 MIXED MESSAGES 0 0 0 0 1:58.10 30.30s 29.10s"
            # Format: plc tab_num horse margin 800margin 800width 400width final_time 3rd_qtr 4th_qtr
            gawler_match = re.match(r'^(\d+)\s+(\d+)\s+([A-Z][A-Z\s&\-\']+?)\s+([\d\.]+)\s+([\d\.]+)\s+(\d+)\s+(\d+)\s+(\d:\d{2}\.\d{2})\s+([\d\.]+)s\s+([\d\.]+)s', line)
            if gawler_match:
                runners.append({
                    'finish_position': int(gawler_match.group(1)),
                    'tab_number': int(gawler_match.group(2)),
                    'horse_name': gawler_match.group(3).strip(),
                    'margin': f"{gawler_match.group(4)}m",
                    'margin_800m': f"{gawler_match.group(5)}m", 
                    'width_800m': int(gawler_match.group(6)),
                    'width_400m': int(gawler_match.group(7)),
                    'final_time': gawler_match.group(8),
                    'third_quarter': f"{gawler_match.group(9)}s",
                    'fourth_quarter': f"{gawler_match.group(10)}s",
                    'third_quarter_seconds': float(gawler_match.group(9)),
                    'fourth_quarter_seconds': float(gawler_match.group(10))
                })
                continue
            
            # Enhanced SA concatenated format with improved parsing
            # Pattern: "2HURRICANE ED 1 0.0m4.4m (0) 4.6m (1) 2:59.20 30.71s 28.87s"
            sa_concat_match = re.match(
                r'^(\d+)([A-Z][A-Za-z\s&\-\']+?)\s+(\d+)\s+([\d\.]+)m([\d\.]+)m\s*\((\d+)\)\s+([\d\.]+)m\s*\((\d+)\)\s+(\d:\d{2}\.\d{2})\s+([\d\.]+)s\s+([\d\.]+)s',
                line
            )
            
            if sa_concat_match:
                tab_number = int(sa_concat_match.group(1))
                horse_name = sa_concat_match.group(2).strip()
                finish_position = int(sa_concat_match.group(3))
                margin = f"{sa_concat_match.group(4)}m"
                
                # Parse 800m position data
                width_800m_distance = f"{sa_concat_match.group(5)}m"
                width_800m_position = int(sa_concat_match.group(6))
                width_800m_combined = f"{width_800m_distance} ({width_800m_position})"
                
                # Parse 400m position data
                width_400m_distance = f"{sa_concat_match.group(7)}m"
                width_400m_position = int(sa_concat_match.group(8))
                width_400m_combined = f"{width_400m_distance} ({width_400m_position})"
                
                final_time = sa_concat_match.group(9)
                third_quarter = f"{sa_concat_match.group(10)}s"
                fourth_quarter = f"{sa_concat_match.group(11)}s"
                third_quarter_seconds = float(sa_concat_match.group(10))
                fourth_quarter_seconds = float(sa_concat_match.group(11))
                
                # Look for distance gained/lost indicators on the next line
                distance_gained_800_to_400 = None
                
                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    
                    # Pattern: "+4.4" or "–4.4" (note: uses special dash character)
                    gain_loss_match = re.search(r'([+–-])([\d\.]+)', next_line)
                    if gain_loss_match:
                        sign = gain_loss_match.group(1)
                        value = float(gain_loss_match.group(2))
                        # Convert special dash to standard minus
                        if sign in ['–', '-']:
                            distance_gained_800_to_400 = -value
                        else:
                            distance_gained_800_to_400 = value
                
                runner_data = {
                    'tab_number': tab_number,
                    'horse_name': horse_name,
                    'finish_position': finish_position,
                    'margin': margin,
                    # Combined format (for compatibility)
                    'width_800m': width_800m_combined,
                    'width_400m': width_400m_combined,
                    # Granular format (for analysis)
                    'width_800m_distance': width_800m_distance,
                    'width_800m_position': width_800m_position,
                    'width_400m_distance': width_400m_distance,
                    'width_400m_position': width_400m_position,
                    'final_time': final_time,
                    'third_quarter': third_quarter,
                    'fourth_quarter': fourth_quarter,
                    # Clean time formats (for numerical analysis)
                    'third_quarter_seconds': third_quarter_seconds,
                    'fourth_quarter_seconds': fourth_quarter_seconds,
                    # Distance gained/lost
                    'distance_gained_800_to_400': distance_gained_800_to_400,
                }
                
                runners.append(runner_data)
                continue
                
        # Legacy SA format - tab number on separate lines    
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if not line:
                i += 1
                continue
            
            # SA specific pattern - look for tab number on its own line
            if line.isdigit() and int(line) <= 20:  # Reasonable tab number range
                tab_number = int(line)
                horse_name = None
                
                # Look for horse name in the next few lines
                for j in range(i + 1, min(i + 5, len(lines))):
                    potential_name_line = lines[j].strip()
                    
                    # SA horse name validation - must be all caps, no numbers at start
                    if (re.match(r'^[A-Z][A-Za-z\s&\-\']+$', potential_name_line) and 
                        len(potential_name_line) > 2 and
                        not potential_name_line.isdigit()):
                        
                        # SA specific exclusions
                        excluded_words = {
                            'DATA TABLE', 'NO HORSE', 'HORSE', 'PLC', 'MAR', 'TIME', 'QTR', 'POSI',
                            'FIRST', 'SECOND', 'THIRD', 'FOURTH', 'FINISH', 'POSITION', 'METRES',
                            'GAINED', 'VISUAL', 'DIGITAL', 'RACE', 'DISTANCE', 'FRIDAY', 'GAWLER'
                        }
                        
                        if (potential_name_line not in excluded_words and 
                            not any(word in potential_name_line for word in ['TABLE', 'DATA', 'POSITION', 'RACE'])):
                            horse_name = potential_name_line
                            break
                
                # Look for data line after the horse name
                if horse_name:
                    for k in range(i + 1, min(i + 10, len(lines))):
                        data_line = lines[k].strip()
                        
                        # SA data pattern: "1 0.0m 11.0m (1) 4.6m (2) 2:45.50 29.82s 30.84s"
                        sa_data_match = re.match(r'^(\d+)\s+([\d\.]+m)\s+([\d\.]+m)\s*\((\d+)\)\s+([\d\.]+m)\s*\((\d+)\)\s+(\d:\d{2}\.\d{2})\s+(\d{2}\.\d{2})s\s+(\d{2}\.\d{2})s', data_line)
                        
                        if sa_data_match:
                            runners.append({
                                'tab_number': tab_number,
                                'horse_name': horse_name,
                                'finish_position': int(sa_data_match.group(1)),
                                'margin': sa_data_match.group(2),
                                'width_800m': sa_data_match.group(3),
                                'width_800m_position': int(sa_data_match.group(4)),
                                'width_400m': sa_data_match.group(5),
                                'width_400m_position': int(sa_data_match.group(6)),
                                'final_time': sa_data_match.group(7),
                                'third_quarter': f"{sa_data_match.group(8)}s",
                                'fourth_quarter': f"{sa_data_match.group(9)}s"
                            })
                            break
                
            i += 1
        
        return runners
    
    def _extract_pj_nsw(self, pdf_obj_or_text) -> List[Dict[str, Any]]:
        """Extract PJ data specifically for NSW format with multi-format support"""
        
        # Handle both PDF object and text input
        if hasattr(pdf_obj_or_text, 'pages'):
            # PDF object passed - use page-by-page processing for NSW multi-format support
            pdf_obj = pdf_obj_or_text
            all_runners = []
            
            # Process each page separately for NSW
            for page_num, page in enumerate(pdf_obj.pages):
                page_text = page.extract_text()
                
                # Check for multi-race format (Tamworth style)
                race_count = len(re.findall(r'Race\s+No\.\s+\d+\s+Distance\s+\d+m', page_text))
                
                if race_count > 1:
                    # This is Tamworth-style multi-race format
                    page_runners = self._extract_nsw_tamworth_format(page_text, page_num + 1)
                    all_runners.extend(page_runners)
                else:
                    # Standard NSW format (Penrith style or simple)
                    race_info = self._extract_pj_race_info_from_page(page_text, page_num + 1)
                    
                    # Skip if no race data found
                    if not race_info.get('race_number'):
                        continue
                    
                    # Extract table data from this page using table extraction first
                    tables = page.extract_tables()
                    page_runners = []
                    
                    # Try table-based extraction first
                    if tables:
                        page_runners = self._extract_pj_nsw_from_table(tables, race_info)
                    
                    # If table extraction failed or returned no data, use text-based extraction
                    if not page_runners:
                        page_runners = self._extract_pj_nsw_from_text(page_text, race_info)
                    
                    # Skip if no runners found
                    if not page_runners:
                        continue
                    
                    # Add race metadata to each runner
                    for runner in page_runners:
                        runner.update({
                            'race_number': race_info.get('race_number'),
                            'distance': race_info.get('distance'),
                            'date': race_info.get('date'),
                            'track': race_info.get('track'),
                            'gross_time': race_info.get('gross_time'),
                            'mile_rate': race_info.get('mile_rate'),
                            'lead_time': race_info.get('lead_time'),
                            'quarter_1': race_info.get('quarter_1'),
                            'quarter_2': race_info.get('quarter_2'),
                            'quarter_3': race_info.get('quarter_3'),
                            'quarter_4': race_info.get('quarter_4')
                        })
                    
                    all_runners.extend(page_runners)
            
            return all_runners
        else:
            # Text passed - use simple text processing
            text = pdf_obj_or_text
            return self._extract_pj_nsw_from_text(text, {})

    def _extract_nsw_tamworth_format(self, page_text: str, page_num: int) -> List[Dict[str, Any]]:
        """
        Extract NSW Tamworth multi-race format where multiple races appear on one page.
        Based on successful SA multi-race format handler.
        Format: Race No. X Distance XXXXm
        """
        all_runners = []
        lines = page_text.split('\n')
        
        # Track where each race starts and ends
        race_sections = []
        for i, line in enumerate(lines):
            # Look for race headers: "Race No. 1 Distance 1980m"
            race_match = re.match(r'^Race\s+No\.\s+(\d+)\s+Distance\s+(\d+)m', line.strip())
            if race_match:
                race_sections.append({
                    'line_index': i,
                    'race_number': int(race_match.group(1)),
                    'distance': f"{race_match.group(2)}m"
                })
        
        # Extract venue and date from header
        venue = None
        date = None
        for line in lines[:5]:  # Check first few lines
            if 'Sectional information' in line:
                # Pattern: "Sectional information TAMWORTH Sunday, 8 January 2017"
                match = re.match(r'Sectional information\s+(.+?)\s+(.+)$', line.strip())
                if match:
                    venue = match.group(1)
                    date = match.group(2)
                    break
        
        # Process each race section
        for i, race_section in enumerate(race_sections):
            race_number = race_section['race_number']
            distance = race_section['distance']
            start_line = race_section['line_index']
            
            # Determine end line (start of next race or end of text)
            if i + 1 < len(race_sections):
                end_line = race_sections[i + 1]['line_index']
            else:
                end_line = len(lines)
            
            # Extract text for this race
            race_text = '\n'.join(lines[start_line:end_line])
            
            # Extract runners from this race section using Tamworth-specific patterns
            race_runners = self._extract_tamworth_race_data(race_text)
            
            # Add race metadata to each runner
            for runner in race_runners:
                runner.update({
                    'race_number': race_number,
                    'distance': distance,
                    'track': venue,
                    'date': date
                })
                
            all_runners.extend(race_runners)
        
        return all_runners

    def _extract_tamworth_race_data(self, race_text: str) -> List[Dict[str, Any]]:
        """Extract runner data from Tamworth-style race section"""
        runners = []
        lines = race_text.split('\n')
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Try NSW 3-column sectional format first: "1 9 IDEAL SITUATION 0 10.5 1 2 1:52.50 55.76s 41.26s 27.15s"
            # Format: plc tab_num horse margin 800margin 800width 400width final_time last_800m last_600m last_400m
            nsw_3col_sectional_match = re.match(r'^(\d+)\s+(\d+)\s+([A-Z][A-Za-z\s&\-\']+?)\s+([\d\.]+)\s+([\d\.]+)\s+(\d+)\s+(\d+)\s+(\d:\d{2}\.\d{2})\s+([\d\.]+)s\s+([\d\.]+)s\s+([\d\.]+)s', line)
            
            if nsw_3col_sectional_match:
                runners.append({
                    'finish_position': int(nsw_3col_sectional_match.group(1)),
                    'tab_number': int(nsw_3col_sectional_match.group(2)),
                    'horse_name': nsw_3col_sectional_match.group(3).strip(),
                    'margin': f"{nsw_3col_sectional_match.group(4)}m",
                    'margin_800m': f"{nsw_3col_sectional_match.group(5)}m", 
                    'width_800m': int(nsw_3col_sectional_match.group(6)),
                    'width_400m': int(nsw_3col_sectional_match.group(7)),
                    'final_time': nsw_3col_sectional_match.group(8),
                    'last_800m': f"{nsw_3col_sectional_match.group(9)}s",
                    'last_600m': f"{nsw_3col_sectional_match.group(10)}s",
                    'last_400m': f"{nsw_3col_sectional_match.group(11)}s",
                    'last_800m_seconds': float(nsw_3col_sectional_match.group(9)),
                    'last_600m_seconds': float(nsw_3col_sectional_match.group(10)),
                    'last_400m_seconds': float(nsw_3col_sectional_match.group(11))
                })
                continue
            
            # Try NSW Penrith multi-race format: "1 8 BRACKEN KNIGHT 0 17.8 1 3 2:06.10 57.49s 28.36s"
            # Format: plc tab_num horse margin 800margin 800width 400width final_time last_800m last_400m
            nsw_penrith_match = re.match(r'^(\d+)\s+(\d+)\s+([A-Z][A-Za-z\s&\-\']+?)\s+([\d\.]+)\s+([\d\.]+)\s+(\d+)\s+(\d+)\s+(\d:\d{2}\.\d{2})\s+([\d\.]+)s\s+([\d\.]+)s', line)
            
            if nsw_penrith_match:
                runners.append({
                    'finish_position': int(nsw_penrith_match.group(1)),
                    'tab_number': int(nsw_penrith_match.group(2)),
                    'horse_name': nsw_penrith_match.group(3).strip(),
                    'margin': f"{nsw_penrith_match.group(4)}m",
                    'margin_800m': f"{nsw_penrith_match.group(5)}m", 
                    'width_800m': int(nsw_penrith_match.group(6)),
                    'width_400m': int(nsw_penrith_match.group(7)),
                    'final_time': nsw_penrith_match.group(8),
                    'last_800m': f"{nsw_penrith_match.group(9)}s",
                    'last_400m': f"{nsw_penrith_match.group(10)}s",
                    'last_800m_seconds': float(nsw_penrith_match.group(9)),
                    'last_400m_seconds': float(nsw_penrith_match.group(10))
                })
                continue
            
            # Original Tamworth format: "1 4 AUSSIE VISTA 0 0 0 0 2:27.40 60.80s 30.70s" (with margin column)
            # Format: plc tab_num horse margin 800margin 800width 400width final_time last_800m last_400m
            original_tamworth_match = re.match(r'^(\d+)\s+(\d+)\s+([A-Z][A-Za-z\s&\-\']+?)\s+[\d\.]+\s+([\d\.]+)\s+([\d\.]+)\s+(\d+)\s+(\d+)\s+(\d:\d{2}\.\d{2})\s+([\d\.]+)s\s+([\d\.]+)s', line)
            
            if original_tamworth_match:
                runners.append({
                    'finish_position': int(original_tamworth_match.group(1)),
                    'tab_number': int(original_tamworth_match.group(2)),
                    'horse_name': original_tamworth_match.group(3).strip(),
                    'margin': f"{original_tamworth_match.group(4)}m",
                    'margin_800m': f"{original_tamworth_match.group(5)}m", 
                    'width_800m': int(original_tamworth_match.group(6)),
                    'width_400m': int(original_tamworth_match.group(7)),
                    'final_time': original_tamworth_match.group(8),
                    'last_800m': f"{original_tamworth_match.group(9)}s",
                    'last_400m': f"{original_tamworth_match.group(10)}s",
                    'last_800m_seconds': float(original_tamworth_match.group(9)),
                    'last_400m_seconds': float(original_tamworth_match.group(10))
                })
        
        return runners

    def _extract_pj_nsw_from_table(self, tables, race_info: Dict) -> List[Dict[str, Any]]:
        """Extract NSW PJ data from table structure - handles Penrith format with 3-column sectional data"""
        runners = []
        
        # First, try to reconstruct complete table from multiple table segments
        # NSW PDFs often split into multiple tables by pdfplumber
        full_table_data = []
        headers = []
        
        for table_idx, table in enumerate(tables):
            if not table:
                continue
            
            # Look for headers in each table segment
            for row in table:
                if not row:
                    continue
                    
                # Check for header indicators
                row_text = ' '.join([str(cell) for cell in row if cell]).upper()
                if any(header in row_text for header in ['HORSE', 'MARGIN', 'SECTIONAL', 'OVERALL TIME', 'LAST 800M', 'LAST 600M', 'LAST 400M']):
                    headers.extend(row)
                else:
                    # This is likely data - add to full table
                    full_table_data.append(row)
        
        # Try to extract from reconstructed table
        if full_table_data:
            for row in full_table_data:
                if not row or len(row) < 5:
                    continue
                
                # Skip rows that look like headers
                if any(header in str(cell).upper() for cell in row for header in ['HORSE', 'PLC', 'MAR', 'TIME']):
                    continue
                
                try:
                    # Check if this looks like NSW Penrith format
                    # Look for: position, tab_number, horse_name pattern
                    if (row[0] and str(row[0]).isdigit() and 
                        row[1] and str(row[1]).isdigit() and 
                        row[2] and len(str(row[2])) > 2):
                        
                        runner_data = {
                            'finish_position': int(row[0]),
                            'tab_number': int(row[1]),
                            'horse_name': str(row[2]).strip(),
                            'margin': f"{row[3]}m" if len(row) > 3 and row[3] else None,
                            'margin_800m': f"{row[4]}m" if len(row) > 4 and row[4] else None,
                            'width_800m': int(row[5]) if len(row) > 5 and str(row[5]).isdigit() else None,
                            'width_400m': int(row[6]) if len(row) > 6 and str(row[6]).isdigit() else None,
                            'final_time': str(row[7]) if len(row) > 7 and row[7] else None,
                            'last_800m': str(row[8]) if len(row) > 8 and row[8] else None,
                            'last_600m': str(row[9]) if len(row) > 9 and row[9] else None,
                            'last_400m': str(row[10]) if len(row) > 10 and row[10] else None
                        }
                        
                        # Convert sectional times to seconds for compatibility
                        for field in ['last_800m', 'last_600m', 'last_400m']:
                            if runner_data.get(field):
                                time_str = str(runner_data[field]).replace('s', '')
                                try:
                                    runner_data[f"{field}_seconds"] = float(time_str)
                                except ValueError:
                                    pass
                        
                        # Only add if we have essential data
                        if runner_data['tab_number'] and runner_data['horse_name'] and runner_data['finish_position']:
                            runners.append(runner_data)
                            
                except (ValueError, IndexError):
                    continue
        
        # If no runners found with new logic, fall back to original logic for compatibility
        if not runners:
            for table in tables:
                if not table or len(table) < 2:
                    continue
                
                # Process each row looking for runner data
                for row_idx, row in enumerate(table):
                    if not row or len(row) < 5:
                        continue
                    
                    # Skip header-like rows
                    if any(header in str(cell).upper() for cell in row for header in ['HORSE', 'PLC', 'MAR', 'TIME']):
                        continue
                    
                    try:
                        # Check if this looks like NSW Penrith format
                        # Row format: [tab_num, horse_name, plc, margin, positions, time, quarters]
                        if (row[0] and str(row[0]).isdigit() and 
                            row[1] and len(str(row[1])) > 2 and
                            row[2] and str(row[2]).isdigit()):
                            
                            runner_data = {
                                'tab_number': int(row[0]),
                                'horse_name': str(row[1]).strip(),
                                'finish_position': int(row[2]),
                                'margin': str(row[3]) if len(row) > 3 else None,
                                'width_800m': str(row[4]) if len(row) > 4 else None,
                                'width_400m': str(row[5]) if len(row) > 5 else None,
                                'final_time': str(row[6]) if len(row) > 6 else None,
                                'third_quarter': str(row[7]) if len(row) > 7 else None,
                                'fourth_quarter': str(row[8]) if len(row) > 8 else None
                            }
                            
                            # Only add if we have essential data
                            if runner_data['tab_number'] and runner_data['horse_name']:
                                runners.append(runner_data)
                                
                    except (ValueError, IndexError):
                        continue
        
        return runners

    def _extract_pj_nsw_from_text(self, text: str, race_info: Dict) -> List[Dict[str, Any]]:
        """Enhanced text-based extraction for NSW PJ format"""
        runners = []
        lines = text.split('\n')
        
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
            
            runner_data = None
            
            # NSW 3-column sectional format - "1 9 IDEAL SITUATION 0 10.5 1 2 1:52.50 55.76s 41.26s 27.15s"
            # Format: Plc No Horse Margin 800_margin 800_width 400_width Overall_Time Last_800m Last_600m Last_400m
            nsw_3col_sectional_match = re.match(r'^(\d+)\s+(\d+)\s+([A-Z][A-Z\s&\-\']+?)\s+([\d\.]+)\s+([\d\.]+)\s+(\d+)\s+(\d+)\s+(\d:\d{2}\.\d{2})\s+([\d\.]+)s\s+([\d\.]+)s\s+([\d\.]+)s', line)
            
            if nsw_3col_sectional_match:
                runner_data = {
                    'finish_position': int(nsw_3col_sectional_match.group(1)),
                    'tab_number': int(nsw_3col_sectional_match.group(2)),
                    'horse_name': nsw_3col_sectional_match.group(3).strip(),
                    'margin': f"{nsw_3col_sectional_match.group(4)}m",
                    'margin_800m': f"{nsw_3col_sectional_match.group(5)}m",
                    'width_800m': int(nsw_3col_sectional_match.group(6)),
                    'width_400m': int(nsw_3col_sectional_match.group(7)),
                    'final_time': nsw_3col_sectional_match.group(8),
                    'last_800m': f"{nsw_3col_sectional_match.group(9)}s",
                    'last_600m': f"{nsw_3col_sectional_match.group(10)}s",
                    'last_400m': f"{nsw_3col_sectional_match.group(11)}s",
                    'last_800m_seconds': float(nsw_3col_sectional_match.group(9)),
                    'last_600m_seconds': float(nsw_3col_sectional_match.group(10)),
                    'last_400m_seconds': float(nsw_3col_sectional_match.group(11))
                }
            
            # NSW Newcastle tabular format - "1 6 STRATHLACHLANLUCKY 0 4.5 0 0 2:37.20 61.55s 30.58s"
            # Format: Plc No Horse Margin 800_margin 800_width 400_width Overall_Time Last_800m Last_400m
            # Note: Newcastle format only has width data, no separate 400m margin - we'll set margin_400m to match the main margin
            newcastle_match = re.match(r'^(\d+)\s+(\d+)\s+([A-Z][A-Z\s&\-\']+?)\s+([\d\.]+)\s+([\d\.]+)\s+(\d+)\s+(\d+)\s+(\d:\d{2}\.\d{2})\s+([\d\.]+)s\s+([\d\.]+)s', line)
            
            if newcastle_match:
                runner_data = {
                    'finish_position': int(newcastle_match.group(1)),
                    'tab_number': int(newcastle_match.group(2)),
                    'horse_name': newcastle_match.group(3).strip(),
                    'margin': f"{newcastle_match.group(4)}m",
                    'margin_800m': f"{newcastle_match.group(5)}m",
                    'width_800m': int(newcastle_match.group(6)),
                    'margin_400m': f"{newcastle_match.group(4)}m",  # Use main margin as 400m margin fallback
                    'width_400m': int(newcastle_match.group(7)),
                    'final_time': newcastle_match.group(8),
                    'last_800m_sectional': f"{newcastle_match.group(9)}s",
                    'last_400m_sectional': f"{newcastle_match.group(10)}s"
                }
            
            # NSW Dubbo format (similar to TAS PJ) - "8BROOKLYN BANDIT 1 0.0m7.2m (1) 1.4m (2) 2:40.40 29.48s 28.60s"
            elif not runner_data:
                dubbo_match = re.match(r'^(\d+)([A-Z][A-Z\s&\-\']+?)\s+(\d+)\s+([\d\.]+m)([\d\.]+m)\s*\((\d+)\)\s+([\d\.]+m)\s*\((\d+)\)\s+(\d:\d{2}\.\d{2})\s+([\d\.]+)s\s+([\d\.]+)s', line)
                
                if dubbo_match:
                    runner_data = {
                        'tab_number': int(dubbo_match.group(1)),
                        'horse_name': dubbo_match.group(2).strip(),
                        'finish_position': int(dubbo_match.group(3)),
                        'margin': dubbo_match.group(4),
                        'margin_800m': dubbo_match.group(5),
                        'width_800m': int(dubbo_match.group(6)),
                        'margin_400m': dubbo_match.group(7),
                        'width_400m': int(dubbo_match.group(8)),
                        'final_time': dubbo_match.group(9),
                        'third_quarter_seconds': f"{dubbo_match.group(10)}s",
                        'fourth_quarter_seconds': f"{dubbo_match.group(11)}s"
                    }
            
            # NSW Penrith format with separate horse name line
            # Data line: "1 1 0.0m 4.0m (0) 4.4m (0) 2:06.40 29.13s 28.29s"
            elif not runner_data:
                penrith_match = re.match(r'^(\d+)\s+(\d+)\s+([\d\.]+m)\s+([\d\.]+m)\s*\((\d+)\)\s+([\d\.]+m)\s*\((\d+)\)\s+(\d:\d{2}\.\d{2})\s+([\d\.]+)s\s+([\d\.]+)s', line)
                
                if penrith_match:
                    # Look for horse name in surrounding lines
                    horse_name = None
                    for j in range(max(0, i-3), min(i+3, len(lines))):
                        if j == i:
                            continue
                        potential_name = lines[j].strip()
                        # NSW horse name validation
                        if (re.match(r'^[A-Z][A-Za-z\s&\-\']+$', potential_name) and 
                            len(potential_name) > 2 and len(potential_name) < 30):
                            # Exclude common headers
                            excluded_words = {'HORSE', 'DATA TABLE', 'POSITION', 'TIME', 'QUARTER'}
                            if not any(word in potential_name.upper() for word in excluded_words):
                                horse_name = potential_name
                                break
                    
                    if horse_name:
                        runner_data = {
                            'tab_number': int(penrith_match.group(1)),
                            'finish_position': int(penrith_match.group(2)),
                            'horse_name': horse_name,
                            'margin': penrith_match.group(3),
                            'margin_800m': penrith_match.group(4),
                            'width_800m': int(penrith_match.group(5)),
                            'margin_400m': penrith_match.group(6),
                            'width_400m': int(penrith_match.group(7)),
                            'final_time': penrith_match.group(8),
                            'third_quarter': f"{penrith_match.group(9)}s",
                            'fourth_quarter': f"{penrith_match.group(10)}s",
                            'third_quarter_seconds': float(penrith_match.group(9)),
                            'fourth_quarter_seconds': float(penrith_match.group(10))
                        }
            
            # Add runner if we found valid data
            if runner_data:
                # Add race metadata if available
                if race_info:
                    runner_data.update(race_info)
                runners.append(runner_data)
        
        return runners
    
    def _extract_pj_generic(self, text: str) -> List[Dict[str, Any]]:
        """Generic PJ extraction for unknown states"""
        runners = []
        lines = text.split('\n')
        
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
                
            # Standard PJ with full timing data
            pj_match = re.match(r'^(\d+)\s*([A-Z][A-Z\s&\-\']+?)\s+(\d+)\s+([\d\.]+m)\s+(\d:\d{2}\.\d{2})\s+(\d{2}\.\d{2})s\s*\((\d+)\)\s+(\d{2}\.\d{2})s\s*\((\d+)\)', line)
            
            if pj_match:
                runners.append({
                    'tab_number': int(pj_match.group(1)),
                    'horse_name': pj_match.group(2).strip(),
                    'finish_position': int(pj_match.group(3)),
                    'margin': pj_match.group(4),
                    'final_time': pj_match.group(5),
                    'time_800m': f"{pj_match.group(6)}s",
                    'win_indicator_800m': int(pj_match.group(7)),
                    'time_400m': f"{pj_match.group(8)}s", 
                    'win_indicator_400m': int(pj_match.group(9))
                })
                continue
                
            # Simpler format without timing details
            simple_match = re.match(r'^(\d+)\s+([A-Z][A-Z\s&\-\']+?)\s+(\d+)\s+([\d\.]+m)\s+(\d:\d{2}\.\d{2})', line)
            if simple_match:
                runners.append({
                    'tab_number': int(simple_match.group(1)),
                    'horse_name': simple_match.group(2).strip(),
                    'finish_position': int(simple_match.group(3)),
                    'margin': simple_match.group(4),
                    'final_time': simple_match.group(5)
                })
        
        return runners
    
    def _detect_state_from_path(self, file_path: str) -> str:
        """Detect state from file path - more reliable than text parsing"""
        path_lower = file_path.lower()
        
        # Check for state folders in the path
        state_patterns = {
            'qld': ['/qld/', '\\qld\\', 'queensland'],
            'vic': ['/vic/', '\\vic\\', 'victoria'],
            'nsw': ['/nsw/', '\\nsw\\', 'new_south_wales'],
            'sa': ['/sa/', '\\sa\\', 'south_australia'],
            'tas': ['/tas/', '\\tas\\', 'tasmania'],
            'wa': ['/wa/', '\\wa\\', 'western_australia'],
            'nt': ['/nt/', '\\nt\\', 'northern_territory'],
            'act': ['/act/', '\\act\\', 'australian_capital_territory']
        }
        
        for state, patterns in state_patterns.items():
            if any(pattern in path_lower for pattern in patterns):
                return state
        
        return 'unknown'
    
    def _detect_state_from_text(self, text: str) -> str:
        """Detect state from PDF text content - fallback method"""
        text_lower = text.lower()
        
        # State indicators - order by specificity, most specific first
        state_indicators = {
            'tas': ['tasmania', 'carrick', 'hobart', 'launceston', 'devonport', 'tas racing'],
            'sa': ['south australia', 'gawler', 'globe derby', 'port pirie', 'adelaide'],
            'qld': ['queensland', 'qld', 'redcliffe', 'albion park', 'ipswich', 'gold coast', 'sunshine coast', 'marburg'],
            'vic': ['victoria', 'vic', 'cranbourne', 'ballarat', 'ararat', 'warrnambool', 'geelong', 'bendigo'],
            'nsw': ['new south wales', 'nsw', 'menangle', 'bathurst', 'goulburn', 'newcastle', 'wagga'],
            'wa': ['western australia', 'wa', 'gloucester', 'fremantle', 'narrogin', 'perth'],
            'nt': ['northern territory', 'nt', 'darwin'],
            'act': ['australian capital territory', 'act', 'canberra']
        }
        
        for state, indicators in state_indicators.items():
            if any(indicator in text_lower for indicator in indicators):
                return state
        
        return 'unknown'
    
    def extract_race_summary(self, text: str, format_type: str) -> Dict[str, Any]:
        """Extract race summary information"""
        summary = {}
        
        if format_type == 'triples':
            # Extract lead time, quarters for TripleS
            lead_time_match = re.search(r'Lead\s+Time\s+([\d:\.]+)', text, re.IGNORECASE)
            if lead_time_match:
                summary['lead_time'] = lead_time_match.group(1)
                
        elif format_type == 'pj':
            # Extract gross time, mile rate, quarters for PJ
            gross_time_match = re.search(r'Gross Time:\s*([\d:\.]+)', text, re.IGNORECASE)
            if gross_time_match:
                summary['gross_time'] = gross_time_match.group(1)
            
            mile_rate_match = re.search(r'MileRate:\s*([\d:\.]+)', text, re.IGNORECASE)
            if mile_rate_match:
                summary['mile_rate'] = mile_rate_match.group(1)
            
            lead_time_match = re.search(r'LeadTime:\s*([-\d\.]+s)', text, re.IGNORECASE)
            if lead_time_match:
                summary['lead_time'] = lead_time_match.group(1)
                
            # Extract quarters
            quarters = {}
            for i in range(1, 5):
                quarter_match = re.search(rf'{["First", "Second", "Third", "Fourth"][i-1]}\s+Qtr:\s*([\d\.]+s)', text, re.IGNORECASE)
                if quarter_match:
                    quarters[f'quarter_{i}'] = quarter_match.group(1)
            summary['quarters'] = quarters
        
        return summary
    
    def extract_pdf_data(self, file_path: str) -> Dict[str, Any]:
        """
        Main extraction method for PDF files
        
        Args:
            file_path: Path to PDF file
            
        Returns:
            Dictionary containing extracted race data
        """
        try:
            with pdfplumber.open(file_path) as pdf:
                # Extract text from all pages
                full_text = ""
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        full_text += page_text + "\n"
                
                if not full_text.strip():
                    return {'error': 'No text could be extracted from PDF'}
                
                # Detect format
                pdf_format = self.detect_format(full_text)
                
                if pdf_format == 'unknown':
                    error_msg = 'Unknown PDF format - cannot process'
                    return {'error': error_msg}
                
                print(f"Detected format: {pdf_format}")
                
                # Extract metadata
                metadata = self.extract_metadata(full_text, pdf_format)
                
                # For triples_detailed, also try to extract venue from first line
                if pdf_format == 'triples_detailed' and not metadata.get('venue'):
                    first_line = full_text.split('\n')[0] if full_text else ''
                    venue_match = re.search(r'^(\w+)\s+\w+\s*-', first_line.strip())
                    if venue_match:
                        metadata['venue'] = venue_match.group(1)
                
                # Detect state for state-specific extraction - use path first, fallback to text
                detected_state = self._detect_state_from_path(file_path)
                if detected_state == 'unknown':
                    detected_state = self._detect_state_from_text(full_text)
                    print(f"Detected state: {detected_state} (from text)")
                else:
                    print(f"Detected state: {detected_state} (from path)")
                
                # Extract race data based on format and state
                if pdf_format == 'triples':
                    # VIC files: check if they have PJ-style layout or TripleS detailed layout
                    if detected_state == 'vic':
                        # Check for VIC PJ-style indicators in the text
                        vic_pj_indicators = ['nohorse plc mar', '800 posi 400 posi', 'position and metres gained from 800m']
                        text_lower = full_text.lower()
                        has_pj_indicators = any(indicator in text_lower for indicator in vic_pj_indicators)

                        if has_pj_indicators:
                            # VIC PJ-style files (like Warragul) use PJ extraction
                            runners_data = self.extract_pj_data(pdf, detected_state, file_path)
                        else:
                            # VIC TripleS detailed files (like Geelong) use TripleS extraction
                            runners_data = self.extract_triples_data(full_text, detected_state, pdf, file_path)
                    else:
                        # For other TripleS, pass the PDF object for table extraction
                        runners_data = self.extract_triples_data(full_text, detected_state, pdf, file_path)
                elif pdf_format == 'triples_detailed':
                    # For detailed TripleS format (like Redcliffe sub-type)
                    runners_data = self.extract_triples_detailed_data(pdf, detected_state, file_path)
                elif pdf_format == 'pj':
                    # For PJ QLD, VIC, TAS, SA, and NSW, pass the PDF object for page-by-page processing
                    if detected_state in ['qld', 'vic', 'tas', 'sa', 'nsw']:
                        runners_data = self.extract_pj_data(pdf, detected_state, file_path)
                    else:
                        runners_data = self.extract_pj_data(full_text, detected_state, file_path)
                else:
                    runners_data = []

                # Add filename-based track extraction as fallback for all files (especially VIC)
                if runners_data:
                    filename_track = self._extract_track_from_filename(file_path)
                    if filename_track:
                        # Add track to runners that don't have it or have empty track
                        for runner in runners_data:
                            if not runner.get('track'):
                                runner['track'] = filename_track
                        # Also add to metadata if missing
                        if not metadata.get('venue') and not metadata.get('track'):
                            metadata['venue'] = filename_track

                # Additional fallback: ensure track is populated from venue/metadata
                if runners_data and metadata:
                    fallback_track = metadata.get('venue') or metadata.get('track')
                    if fallback_track:
                        for runner in runners_data:
                            if not runner.get('track'):
                                runner['track'] = fallback_track

                # Extract race summary
                race_summary = self.extract_race_summary(full_text, pdf_format)
                
                return {
                    'success': True,
                    'format': pdf_format,
                    'metadata': metadata,
                    'race_summary': race_summary,
                    'runners': runners_data,
                    'runner_count': len(runners_data)
                }
                
        except Exception as e:
            return {'error': f'Failed to process PDF: {str(e)}'}
    
    def export_to_csv(self, extracted_data: Dict[str, Any], output_path: str = None, base_dir: str = None):
        """Export extracted data to CSV format with state-based directory structure"""
        if 'error' in extracted_data:
            print(f"Cannot export - error in data: {extracted_data['error']}")
            return False
        
        # Apply post-extraction column conversions for TAS data
        extracted_data = self._apply_post_extraction_column_conversions(extracted_data)
        
        try:
            # Prepare CSV data
            csv_rows = []
            metadata = extracted_data.get('metadata', {})
            race_summary = extracted_data.get('race_summary', {})
            
            for runner in extracted_data.get('runners', []):
                row = {
                    # Metadata columns
                    'venue': metadata.get('venue'),
                    'date': metadata.get('date'),
                    'race_number': metadata.get('race_number'),
                    'distance': metadata.get('distance'),
                    'race_name': metadata.get('race_name'),
                    'format': extracted_data.get('format'),
                    
                    # Race summary columns
                    'gross_time': race_summary.get('gross_time'),
                    'mile_rate': race_summary.get('mile_rate'),
                    'lead_time': race_summary.get('lead_time'),
                    
                    # Runner-specific columns (will vary by format)
                    **runner
                }
                
                # Add quarter times if available, but don't overwrite runner-specific quarters
                if 'quarters' in race_summary:
                    # Only add quarters from race_summary if runner doesn't already have them
                    for quarter_key, quarter_value in race_summary['quarters'].items():
                        if quarter_key not in runner or runner[quarter_key] is None:
                            row[quarter_key] = quarter_value
                
                # Filter out specified columns for QLD and TAS data (do this AFTER all columns are added)
                state_code = self._extract_state_code(metadata.get('venue', ''))
                if state_code in ['qld', 'tas']:
                    # Remove race-level columns that should not be in individual runner records
                    excluded_columns = ['quarter_1', 'quarter_2', 'quarter_3', 'quarter_4', 'margin', 'final_time', 'lead_time']
                    
                    # Add format-specific exclusions
                    format_type = extracted_data.get('format', '')
                    if format_type == 'triples':
                        excluded_columns.extend(['quarter_4_position', 'page_number'])
                    elif format_type == 'pj':
                        # For PJ format, also exclude the position data columns (keep only time and width)
                        excluded_columns.extend(['position_800m', 'position_400m', 'time_3rd_qtr', 'time_4th_qtr'])
                        
                        # TAS-specific exclusions to match QLD structure (but preserve first_100m for specific subtypes)
                        if state_code == 'tas':
                            excluded_columns.extend(['margin_800m', 'margin_400m'])
                            # Only exclude first_100m if it's not meaningful data (all None/empty)
                            if 'first_100m' in row and (not row['first_100m'] or row['first_100m'] in ['None', '', 'nan']):
                                excluded_columns.append('first_100m')
                    
                    for col in excluded_columns:
                        row.pop(col, None)  # Remove column if it exists
                
                csv_rows.append(row)
            
            if not csv_rows:
                print("No runner data to export")
                return False
            
            # Determine output path if not provided
            if not output_path:
                # Try to determine state from venue or use 'unknown'
                state_code = self._extract_state_code(metadata.get('venue', ''))
                
                # Set base directory
                if not base_dir:
                    base_dir = Path(__file__).parent.parent
                
                # Create state-specific processed directory
                processed_dir = Path(base_dir) / 'data' / 'processed' / state_code
                processed_dir.mkdir(parents=True, exist_ok=True)
                
                # Generate filename from metadata
                venue = metadata.get('venue') or 'unknown'
                venue_clean = venue.replace(' ', '_').replace('-', '_')
                date_str = self._format_date_for_filename(metadata.get('date', ''))
                race_num = metadata.get('race_number', 1)
                
                filename = f"{venue_clean}_{date_str}_race{race_num}_extracted.csv"
                output_path = processed_dir / filename
            
            # Write to CSV
            df = pd.DataFrame(csv_rows)
            df.to_csv(output_path, index=False)
            print(f"✓ Data exported to {output_path}")
            return True
                
        except Exception as e:
            print(f"✗ Failed to export CSV: {e}")
            return False
    
    def _extract_state_code(self, venue: str) -> str:
        """Extract state code from venue string"""
        if not venue:
            return 'unknown'
        
        venue_lower = venue.lower()
        
        # State mappings
        state_codes = {
            'qld': ['qld', 'queensland', 'redcliffe', 'albion', 'ipswich', 'gold coast', 'marburg'],
            'vic': ['vic', 'victoria', 'cranbourne', 'ballarat', 'ararat', 'warrnambool'],
            'nsw': ['nsw', 'new south wales', 'menangle', 'bathurst', 'goulburn'],
            'sa': ['sa', 'south australia', 'globe derby', 'gawler', 'port pirie'],
            'tas': ['tas', 'tasmania', 'carrick', 'hobart', 'launceston'],
            'wa': ['wa', 'western australia', 'gloucester', 'fremantle', 'narrogin'],
            'nt': ['nt', 'northern territory', 'darwin'],
            'act': ['act', 'australian capital territory', 'canberra']
        }
        
        for state, indicators in state_codes.items():
            if any(indicator in venue_lower for indicator in indicators):
                return state
        
        return 'unknown'
    
    def _format_date_for_filename(self, date_str: str) -> str:
        """Format date string for use in filename"""
        if not date_str:
            return 'unknown_date'
        
        # Try to parse various date formats and convert to YYYYMMDD
        import datetime
        
        # Common date patterns
        patterns = [
            '%d %B %Y',      # "13 July 2023"
            '%d %b %Y',      # "13 Jul 2023" 
            '%Y-%m-%d',      # "2023-07-13"
            '%d/%m/%Y',      # "13/07/2023"
            '%d-%m-%Y',      # "13-07-2023"
            '%B %d, %Y',     # "July 13, 2023"
            '%A, %d %B %Y',  # "Saturday, 13 February 2021"
        ]
        
        for pattern in patterns:
            try:
                parsed_date = datetime.datetime.strptime(date_str, pattern)
                return parsed_date.strftime('%Y%m%d')
            except ValueError:
                continue
        
        # If parsing fails, create a safe filename version
        safe_date = re.sub(r'[^\w\d]', '_', date_str)
        return safe_date[:20]  # Limit length

def main():
    """Main function for command line usage"""
    if len(sys.argv) < 2:
        print("Usage: python pdf_extractor.py <pdf_file> [output_csv]")
        print("\nExample:")
        print("  python pdf_extractor.py /path/to/race.pdf")
        print("  python pdf_extractor.py /path/to/race.pdf /path/to/output.csv")
        sys.exit(1)
    
    pdf_file = sys.argv[1]
    output_csv = sys.argv[2] if len(sys.argv) > 2 else None
    
    if not Path(pdf_file).exists():
        print(f"Error: PDF file not found: {pdf_file}")
        sys.exit(1)
    
    # Initialize extractor
    extractor = PDFExtractor()
    
    print(f"Processing PDF: {pdf_file}")
    print("=" * 60)
    
    # Extract data
    result = extractor.extract_pdf_data(pdf_file)
    
    if 'error' in result:
        print(f"✗ Error: {result['error']}")
        sys.exit(1)
    
    # Display results
    print(f"✓ Successfully processed PDF")
    print(f"Format: {result['format']}")
    print(f"Runners found: {result['runner_count']}")
    
    metadata = result.get('metadata', {})
    if metadata.get('venue'):
        print(f"Venue: {metadata['venue']}")
    if metadata.get('date'):
        print(f"Date: {metadata['date']}")
    if metadata.get('race_number'):
        print(f"Race: {metadata['race_number']}")
    if metadata.get('distance'):
        print(f"Distance: {metadata['distance']}m")
    
    # Export to CSV
    if output_csv:
        success = extractor.export_to_csv(result, output_csv)
        if not success:
            sys.exit(1)
    else:
        # Use automatic state-based directory structure
        print(f"\nExporting to data/processed/<state>/ directory...")
        success = extractor.export_to_csv(result)
        if not success:
            sys.exit(1)
    
    print("=" * 60)
    print("✓ PDF extraction complete!")

if __name__ == "__main__":
    main()