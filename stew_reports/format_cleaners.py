#!/usr/bin/env python3
"""
Format-specific cleaning and merging functions for harness racing data.

Process Flow:
1. Clean each state/format combination
2. Merge all states together for each format (PJ and TripleS)
3. Merge the final PJ and TripleS datasets together

Usage:
    Import functions from this module and register them in meta_processor.py
"""

import pandas as pd
from typing import List, Optional
import logging
import re
from dateutil import parser
import pandas as pd
from typing import Optional


logger = logging.getLogger(__name__)

# ==================== STATE/FORMAT CLEANERS ====================
# Step 1: Clean each state/format combination
MASTER_COLS_PJ = ['state_pj', 'date', 'track', 
               'race_number', 'tab_number', 'horse_name', 
               'time_800m', 'width_800m', 
               'time_400m', 'width_400m',
               'first_100m']

MASTER_COLS_TRIPLES = ['state', 'date', 'track', 
               'race_number', 'tab_number', 'horse_name', 
               'lead_time_value', 'quarter_1_time', 'quarter_2_time',
			   'quarter_3_time', 'quarter_4_time', 
			   'distance_travelled', 'top_speed',
               'first_50m', 'first_100m', 'first_200m']


def clean_nsw_pj(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean NSW PJ format data.

    TODO: Add your NSW PJ cleaning logic here
    - Standardize column names
    - Handle missing values
    - Convert data types
    - Add calculated fields
    """
    logger.info(f"Cleaning NSW PJ data: {len(df)} rows")
    if len(df) == 0:
        return None

    # Create proper column mapping - handle different possible column names but avoid duplicates
    df_nsw_pj = df.copy()
    df_nsw_pj.to_csv('df_nsw_pj.csv', index=False)

    # Handle time_800m - prioritize in order of preference
    if 'time_800m' not in df_nsw_pj.columns:
        if 'last_800m' in df_nsw_pj.columns:
            df_nsw_pj['time_800m'] = df_nsw_pj['last_800m']
        elif 'third_quarter' in df_nsw_pj.columns:
            df_nsw_pj['time_800m'] = df_nsw_pj['third_quarter']
        else:
            df_nsw_pj['time_800m'] = None

    # Handle time_400m - prioritize in order of preference
    if 'time_400m' not in df_nsw_pj.columns:
        if 'last_400m' in df_nsw_pj.columns:
            df_nsw_pj['time_400m'] = df_nsw_pj['last_400m']
        elif 'fourth_quarter' in df_nsw_pj.columns:
            df_nsw_pj['time_400m'] = df_nsw_pj['fourth_quarter']
        else:
            df_nsw_pj['time_400m'] = None

    df_nsw_pj['state_pj'] = 'NSW'
    def extract_width(width_str):
        # Handle both old format (simple numbers like "0", "1", "2")
        # and new format (complex strings like "0.0m (0)", "3.0m (1)")
        if pd.isna(width_str):
            return width_str
        width_str = str(width_str)
        if 'm' in width_str and '(' in width_str and ')' in width_str:
            # New format: "0.0m (0)" -> "0"
            return width_str.split('(')[1].split(')')[0]
        else:
            # Old format: "0" -> "0"
            return width_str

    df_nsw_pj['width_800m'] = df_nsw_pj['width_800m'].apply(extract_width)
    df_nsw_pj['width_400m'] = df_nsw_pj['width_400m'].apply(extract_width)
    df_nsw_pj['width_400m'] = df_nsw_pj['width_400m'].apply(lambda x: '5' if x == '55' else x).fillna(0)

    # Handle time_800m with safe column references
    if 'last_800m_sectional' in df_nsw_pj.columns:
        df_nsw_pj['time_800m'] = df_nsw_pj['time_800m'].fillna(df_nsw_pj['last_800m_sectional'])
    if 'third_quarter_seconds' in df_nsw_pj.columns:
        df_nsw_pj['time_800m'] = df_nsw_pj['time_800m'].fillna(df_nsw_pj['third_quarter_seconds'])
    if 'third_quarter' in df_nsw_pj.columns:
        df_nsw_pj['time_800m'] = df_nsw_pj['time_800m'].fillna(df_nsw_pj['third_quarter'])
    df_nsw_pj['time_800m'] = df_nsw_pj['time_800m'].apply(lambda x: str(x).replace('s', ''))

    # Handle time_400m with safe column references
    if 'last_400m_sectional' in df_nsw_pj.columns:
        df_nsw_pj['time_400m'] = df_nsw_pj['time_400m'].fillna(df_nsw_pj['last_400m_sectional'])
    if 'fourth_quarter_seconds' in df_nsw_pj.columns:
        df_nsw_pj['time_400m'] = df_nsw_pj['time_400m'].fillna(df_nsw_pj['fourth_quarter_seconds'])
    if 'fourth_quarter' in df_nsw_pj.columns:
        df_nsw_pj['time_400m'] = df_nsw_pj['time_400m'].fillna(df_nsw_pj['fourth_quarter'])
    df_nsw_pj['time_400m'] = df_nsw_pj['time_400m'].apply(lambda x: str(x).replace('s', ''))

    if 'first_100m' not in df_nsw_pj.columns:
        df_nsw_pj['first_100m'] = None

    df_nsw_pj_clean = df_nsw_pj[MASTER_COLS_PJ].copy()
    df_nsw_pj_clean.to_csv('df_nsw_pj_clean.csv', index=False)
    return df_nsw_pj_clean


def clean_nsw_triples(df: pd.DataFrame) -> pd.DataFrame:
    """Clean NSW TripleS format data."""
    logger.info(f"Cleaning NSW TripleS data: {len(df)} rows")
    df_nsw_triples = df.copy()
    df_nsw_triples.to_csv('df_nsw_triples.csv', index=False)
    df_nsw_triples['state'] = 'NSW'

    # Fix corrupted track data by extracting from filename
    def extract_track_from_nsw_filename(row):
        if 'source_file' in row and pd.notna(row['source_file']):
            filename = str(row['source_file'])
            # NSW TripleS filenames like: PC130925_20250913_unknown_H_triples.csv
            # Extract track code from start of filename
            parts = filename.split('_')
            if len(parts) > 0:
                code = parts[0][:2]
                track_mapping = {
                    'PC': 'Penrith',
                    'TB': 'Tabcorp Park Menangle',
                    'DU': 'Dubbo',
                    'BA': 'Bathurst',
                    'BH': 'Bathurst',  # Alternative Bathurst code
                    'WA': 'Wagga',
                    'PA': 'Parkes',
                    'YO': 'Young',
                    'CO': 'Cootamundra',
                    'NR': 'Newcastle',  # Common NSW track
                    'PE': 'Penrith'     # Alternative Penrith code
                }

                track_name = track_mapping.get(code, None)
                if track_name is None:
                    # Log warning for unmapped track codes
                    logger.warning(f"NSW TripleS: Unknown track code '{code}' in filename '{filename}'. Please add mapping.")
                    return 'Unknown'
                return track_name
            else:
                logger.warning(f"NSW TripleS: Could not parse filename '{filename}' for track extraction")
                return 'Unknown'

        logger.warning("NSW TripleS: No source_file available for track extraction")
        return 'Unknown'

    # Apply track extraction to fix corrupted track column
    df_nsw_triples['track'] = df_nsw_triples.apply(extract_track_from_nsw_filename, axis=1)

    df_nsw_triples_clean = df_nsw_triples[MASTER_COLS_TRIPLES].copy()
    df_nsw_triples_clean.to_csv('df_nsw_triples_clean.csv', index=False)
    return df_nsw_triples_clean


def clean_vic_pj(df: pd.DataFrame) -> pd.DataFrame:
    """Clean VIC PJ format data."""
    logger.info(f"Cleaning VIC PJ data: {len(df)} rows")
    if len(df) == 0:
        return None

    df_vic_pj = df.copy()
    df_vic_pj.to_csv('df_vic_pj.csv', index=False)
    vic_rename_cols = {
        'third_quarter_seconds': 'third_quarter',
        'fourth_quarter_seconds': 'fourth_quarter',
    }

    df_vic_pj.rename(columns=vic_rename_cols, inplace=True)

    df_vic_pj['third_quarter'] = df_vic_pj['third_quarter'].apply(lambda x: str(x).replace('s', ''))
    df_vic_pj['fourth_quarter'] = df_vic_pj['fourth_quarter'].apply(lambda x: str(x).replace('s', ''))

    col_upates = {
        'third_quarter': 'time_800m',
        'fourth_quarter': 'time_400m',
    }

    df_vic_pj.rename(columns=col_upates, inplace=True)

    df_vic_pj['state_pj'] = 'VIC'
    if 'first_100m' not in df_vic_pj.columns:
        df_vic_pj['first_100m'] = None

    df_vic_pj_clean = df_vic_pj[MASTER_COLS_PJ].copy()
    df_vic_pj_clean.to_csv('df_vic_pj_clean.csv', index=False)
    return df_vic_pj_clean


def clean_vic_triples(df: pd.DataFrame) -> pd.DataFrame:
    """Clean VIC TripleS format data."""
    logger.info(f"Cleaning VIC TripleS data: {len(df)} rows")
    df_vic_triples = df.copy()
    df_vic_triples.to_csv('df_vic_triples.csv', index=False)
    df_vic_triples['state'] = 'VIC'
    df_vic_triples_clean = df_vic_triples[MASTER_COLS_TRIPLES].copy()
    df_vic_triples_clean.to_csv('df_vic_triples_clean.csv', index=False)
    return df_vic_triples_clean


def clean_qld_pj(df: pd.DataFrame) -> pd.DataFrame:
    """Clean QLD PJ format data."""
    logger.info(f"Cleaning QLD PJ data: {len(df)} rows")
    if len(df) == 0:
        return None

    df_qld_pj = df.copy()
    df_qld_pj.to_csv('df_qld_pj.csv', index=False)
    # df_qld_pj['time_800m'].fillna(df_qld_pj['third_quarter'], inplace=True)
    # df_qld_pj['time_400m'].fillna(df_qld_pj['fourth_quarter'], inplace=True)

    df_qld_pj['time_800m'] = df_qld_pj['time_800m'].apply(lambda x: str(x).replace('s', ''))
    df_qld_pj['time_400m'] = df_qld_pj['time_400m'].apply(lambda x: str(x).replace('s', ''))

    # qld_col_updates = {
    #     'venue': 'track',
    # }

    # df_qld_pj.rename(columns=qld_col_updates, inplace=True)

    df_qld_pj['state_pj'] = 'QLD'
    if 'first_100m' not in df_qld_pj.columns:
        df_qld_pj['first_100m'] = None
        
    df_qld_pj_clean = df_qld_pj[MASTER_COLS_PJ].copy()
    df_qld_pj_clean.to_csv('df_qld_pj_clean.csv', index=False)
    return df_qld_pj_clean


def clean_qld_triples(df: pd.DataFrame) -> pd.DataFrame:
    """Clean QLD TripleS format data."""
    logger.info(f"Cleaning QLD TripleS data: {len(df)} rows")
    if len(df) == 0:
        return None

    df_qld_triples = df.copy()
    df_qld_triples.to_csv('df_qld_triples.csv', index=False)
    df_qld_triples['state'] = 'QLD'

    # Handle duplicate track columns properly
    # QLD has both 'venue' (corrupted) and 'track' (correct) columns
    if 'venue' in df_qld_triples.columns and 'track' in df_qld_triples.columns:
        # Drop the corrupted 'venue' column and keep the correct 'track' column
        df_qld_triples = df_qld_triples.drop(columns=['venue'])
    elif 'venue' in df_qld_triples.columns and 'track' not in df_qld_triples.columns:
        # If only venue exists, rename it (though this shouldn't happen based on current data)
        update_cols_qld = {'venue': 'track'}
        df_qld_triples.rename(columns=update_cols_qld, inplace=True)

    df_qld_triples_clean = df_qld_triples[MASTER_COLS_TRIPLES].copy()
    df_qld_triples_clean.to_csv('df_qld_triples_clean.csv', index=False)
    return df_qld_triples_clean


def clean_sa_pj(df: pd.DataFrame) -> pd.DataFrame:
    """Clean SA PJ format data."""
    logger.info(f"Cleaning SA PJ data: {len(df)} rows")
    if len(df) == 0:
        return None

    df_sa_pj = df.copy()
    df_sa_pj.to_csv('df_sa_pj.csv', index=False)
    df_sa_pj['width_800m'] = df_sa_pj['width_800m_position']
    df_sa_pj['width_400m'] = df_sa_pj['width_400m_position']

    df_sa_pj['time_800m'] = df_sa_pj['third_quarter']
    df_sa_pj['time_400m'] = df_sa_pj['fourth_quarter']

    df_sa_pj['time_800m'] = df_sa_pj['time_800m'].apply(lambda x: str(x).replace('s', ''))
    df_sa_pj['time_400m'] = df_sa_pj['time_400m'].apply(lambda x: str(x).replace('s', ''))

    df_sa_pj['state_pj'] = 'SA'

    if 'first_100m' not in df_sa_pj.columns:
        df_sa_pj['first_100m'] = None
        
    df_sa_pj_clean = df_sa_pj[MASTER_COLS_PJ].copy()
    df_sa_pj_clean.to_csv('df_sa_pj_clean.csv', index=False)
    return df_sa_pj_clean


def clean_tas_pj(df: pd.DataFrame) -> pd.DataFrame:
    """Clean TAS PJ format data."""
    logger.info(f"Cleaning TAS PJ data: {len(df)} rows")
    if len(df) == 0:
        return None

    df_tas_pj = df.copy()
    df_tas_pj.to_csv('df_tas_pj.csv', index=False)

    # Handle potential duplicate track columns in TAS data
    if 'venue' in df_tas_pj.columns and 'track' in df_tas_pj.columns:
        # If both exist, drop venue and keep track
        df_tas_pj = df_tas_pj.drop(columns=['venue'])
    elif 'venue' in df_tas_pj.columns:
        # If only venue exists, rename it to track
        tassy_col_mapping = {'venue': 'track'}
        df_tas_pj.rename(columns=tassy_col_mapping, inplace=True)

    df_tas_pj['time_800m'] = df_tas_pj['time_800m'].apply(lambda x: str(x).replace('s', ''))
    df_tas_pj['time_400m'] = df_tas_pj['time_400m'].apply(lambda x: str(x).replace('s', ''))
    

    df_tas_pj['state_pj'] = 'TAS'
    if 'first_100m' not in df_tas_pj.columns:
        df_tas_pj['first_100m'] = None
    df_tas_pj['first_100m'] = df_tas_pj['first_100m'].apply(lambda x: str(x).replace('s', ''))

    df_tas_pj_clean = df_tas_pj[MASTER_COLS_PJ].copy()
    df_tas_pj_clean.to_csv('df_tas_pj_clean.csv', index=False)
    return df_tas_pj_clean


def clean_wa_pj(df: pd.DataFrame) -> pd.DataFrame:
    """Clean WA PJ format data."""
    logger.info(f"Cleaning WA PJ data: {len(df)} rows")

    if len(df) == 0:
        return None

    rename_cols = {
        '800T': 'time_800m',
        '400T': 'time_400m',
        '800Width': 'width_800m',
        '400Width': 'width_400m',
        'Date': 'date',
        'Location': 'track',
        'Race No': 'race_number',
        'TAB No': 'tab_number',
        'Horse': 'horse_name',
    }

    df_wa_pj = df.copy()
    df_wa_pj.to_csv('df_wa_pj.csv', index=False)
    df_wa_pj.rename(columns=rename_cols, inplace=True)

    df_wa_pj['state_pj'] = 'WA'
    if 'first_100m' not in df_wa_pj.columns:
        df_wa_pj['first_100m'] = None

    df_wa_pj_clean = df_wa_pj[MASTER_COLS_PJ].copy()
    df_wa_pj_clean.to_csv('df_wa_pj_clean.csv', index=False)
    return df_wa_pj_clean


def clean_sa_triples(df: pd.DataFrame) -> pd.DataFrame:
    """Clean SA TripleS format data."""
    logger.info(f"Cleaning SA TripleS data: {len(df)} rows")
    if len(df) == 0:
        return None

    df_sa_triples = df.copy()
    df_sa_triples.to_csv('df_sa_triples.csv', index=False)
    df_sa_triples['state'] = 'SA'
    df_sa_triples_clean = df_sa_triples[MASTER_COLS_TRIPLES].copy()
    df_sa_triples_clean.to_csv('df_sa_triples_clean.csv', index=False)
    return df_sa_triples_clean


def clean_tas_triples(df: pd.DataFrame) -> pd.DataFrame:
    """Clean TAS TripleS format data."""
    logger.info(f"Cleaning TAS TripleS data: {len(df)} rows")
    if len(df) == 0:
        return None

    df_tas_triples = df.copy()
    df_tas_triples.to_csv('df_tas_triples.csv', index=False)
    df_tas_triples['state'] = 'TAS'
    tas_col_mapping = {
        'venue': 'track'
    }
    df_tas_triples.rename(columns=tas_col_mapping, inplace=True)
    df_tas_triples_clean = df_tas_triples[MASTER_COLS_TRIPLES].copy()
    df_tas_triples_clean.to_csv('df_tas_triples_clean.csv', index=False)
    return df_tas_triples_clean


def clean_wa_triples(df: pd.DataFrame) -> pd.DataFrame:
    """Clean WA TripleS format data."""
    logger.info(f"Cleaning WA TripleS data: {len(df)} rows")
    if len(df) == 0:
        return None

    df_wa_triples = df.copy()
    df_wa_triples.to_csv('df_wa_triples.csv', index=False)
    df_wa_triples['state'] = 'WA'
    df_wa_triples_clean = df_wa_triples[MASTER_COLS_TRIPLES].copy()
    df_wa_triples_clean.to_csv('df_wa_triples_clean.csv', index=False)
    return df_wa_triples_clean


# ==================== FORMAT-LEVEL MERGERS ====================
# Step 2: Merge all states together for each format

def merge_all_pj_states(df_vic_pj, df_qld_pj, df_sa_pj, df_tas_pj, df_nsw_pj, df_wa_pj) -> pd.DataFrame:
    list_clean_pjs = [clean_vic_pj(df_vic_pj), clean_qld_pj(df_qld_pj), clean_sa_pj(df_sa_pj), clean_tas_pj(df_tas_pj), clean_nsw_pj(df_nsw_pj), clean_wa_pj(df_wa_pj)]
    list_clean_pjs = [x for x in list_clean_pjs if x is not None]

    df_pj_master = pd.concat(list_clean_pjs)
    df_pj_master.to_csv('df_pj_master_pre_cleaning.csv', index=False)

    df_pj_master['time_800m'] = df_pj_master['time_800m'].apply(lambda x: float(x))
    df_pj_master['time_400m'] = df_pj_master['time_400m'].apply(lambda x: float(x))

    df_pj_master.loc[(df_pj_master['date'] == 'Saturday, 16 November 2024') & 
                    (df_pj_master['track'] == 'Melton') & 
                    (df_pj_master['race_number'] == 8) & 
                    (df_pj_master['tab_number'] == 5), 'time_400m'] = 29.79

    df_pj_master.loc[(df_pj_master['date'] == 'Saturday, 16 November 2024') & 
                    (df_pj_master['track'] == 'Melton') & 
                    (df_pj_master['race_number'] == 8) & 
                    (df_pj_master['tab_number'] == 5), 'time_800m'] = 29.79

    df_pj_master.loc[(df_pj_master['date'] == 'Tuesday, 24 February 2015') & 
                    (df_pj_master['track'] == 'WAGGA') & 
                    (df_pj_master['race_number'] == 9) & 
                    (df_pj_master['tab_number'] == 1), 'time_400m'] = 36.78

    df_pj_master.loc[(df_pj_master['date'] == 'Tuesday, 23 November 2021') & 
                    (df_pj_master['track'] == 'Albion Park') & 
                    (df_pj_master['race_number'] == 1) & 
                    (df_pj_master['tab_number'] == 8), 'time_400m'] = 33.8

    # Fixing 800m times where its the addition of 400m and 800m times
    mask = (df_pj_master['time_400m'] < 33) & (df_pj_master['time_800m'] > 48)
    print(f'Updating {mask.sum()} rows with bad 800m times')
    df_pj_master.loc[mask, 'time_800m'] = (
        df_pj_master.loc[mask, 'time_800m'] - df_pj_master.loc[mask, 'time_400m']
    )

    mask2 = (df_pj_master['time_400m'] < 35) & (df_pj_master['time_800m'] > 60)

    print(f'Updating {mask2.sum()} rows with bad 800m times')
    df_pj_master.loc[mask2, 'time_800m'] = (
        df_pj_master.loc[mask2, 'time_800m'] - df_pj_master.loc[mask2, 'time_400m']
    )

    mask3 = (df_pj_master['time_400m'] < 40) & (df_pj_master['time_800m'] > 65)

    print(f'Updating {mask3.sum()} rows with bad 800m times')
    df_pj_master.loc[mask3, 'time_800m'] = (
        df_pj_master.loc[mask3, 'time_800m'] - df_pj_master.loc[mask3, 'time_400m']
    )

    mask4 = (
        (df_pj_master['time_400m'] < 50) &
        (df_pj_master['time_800m'] > 50) &
        ((df_pj_master['time_800m'] - df_pj_master['time_400m']) > 22)
    )

    print(f'Updating {mask4.sum()} rows with bad 800m times')
    df_pj_master.loc[mask4, 'time_800m'] = (
        df_pj_master.loc[mask4, 'time_800m'] - df_pj_master.loc[mask4, 'time_400m']
    )

    def extract_date_track(row):
        if ',' in row['track']:
            track = row['track'].split(', ')[0]
            track = track.replace('Monday', '').replace('Tuesday', '').replace('Wednesday', '').replace('Thursday', '').replace('Friday', '').replace('Saturday', '').replace('Sunday', '')
            date = row['track'].split(', ')[1]
            return date, track.lower().strip()
        else:
            return row['date'], row['track'].lower().strip()

    df_pj_master['date'], df_pj_master['track'] = zip(*df_pj_master.apply(extract_date_track, axis=1))
    print(f'Found {df_pj_master.shape[0]} rows with {df_pj_master["track"].nunique()} unique tracks')

    df_pj_master['horse_name'] = df_pj_master['horse_name'].str.replace('\n', ' ')
    df_pj_master['horse_name'] = df_pj_master['horse_name'].apply(lambda x: str(x).replace(' NZ', '') if str(x).endswith(' NZ') else str(x))
    df_pj_master['horse_name'] = df_pj_master['horse_name'].apply(lambda x: str(x).replace(' IRL', '') if str(x).endswith(' IRL') else str(x))
    df_pj_master['horse_name'] = df_pj_master['horse_name'].apply(lambda x: str(x).replace(' USA', '') if str(x).endswith(' USA') else str(x))

    # df_pj_master.to_csv('df_pj_master.csv', index=False)
    print(df_pj_master['time_800m'].max(), df_pj_master['time_800m'].min(), 'Was 21.1')
    print(df_pj_master['time_400m'].max(), df_pj_master['time_400m'].min(), 'Was 22.24')
    df_pj_master.to_csv('df_pj_master_post_cleaning.csv', index=False)
    df_pj_master['date'] = df_pj_master['date'].apply(extract_date)
    df_pj_master['horse_name'] = df_pj_master['horse_name'].apply(lambda x: x.lower().strip())
    return df_pj_master

def extract_date(text: str) -> Optional[str]:
    """
    Extracts a date from a text string and returns it
    in 'YYYY-MM-DD' format. Returns None if no date is found.
    """
    try:
        # Try to capture a clear 'day month year' pattern first
        match = re.search(r'\d{1,2}\s+\w+\s+\d{4}', text)
        if match:
            dt = parser.parse(match.group())
        else:
            # Fallback: let parser handle full string, ignoring extra words
            dt = parser.parse(text, fuzzy=True)
        
        # Normalize with pandas to ensure consistent format
        return pd.to_datetime(dt).strftime('%Y-%m-%d')
    
    except Exception:
        return None

def merge_all_triples_states(df_nsw_triples, df_vic_triples, df_qld_triples, df_sa_triples, df_tas_triples, df_wa_triples) -> pd.DataFrame:
    list_clean_triples = [clean_nsw_triples(df_nsw_triples), clean_vic_triples(df_vic_triples), clean_qld_triples(df_qld_triples), clean_sa_triples(df_sa_triples), clean_tas_triples(df_tas_triples), clean_wa_triples(df_wa_triples)]
    list_clean_triples = [x for x in list_clean_triples if x is not None]

    df_triples_master = pd.concat(list_clean_triples)

    # replace /n's with a space, remove and ' NZ' if the string ends with ' NZ' and ' IRL' if the string ends with ' IRL'
    df_triples_master['horse_name'] = df_triples_master['horse_name'].str.replace('\n', ' ')
    df_triples_master['horse_name'] = df_triples_master['horse_name'].apply(lambda x: str(x).replace(' NZ', '') if str(x).endswith(' NZ') else str(x))
    df_triples_master['horse_name'] = df_triples_master['horse_name'].apply(lambda x: str(x).replace(' IRL', '') if str(x).endswith(' IRL') else str(x))

    df_triples_master["lead_time_value"] = (
        pd.to_timedelta("00:" + df_triples_master["lead_time_value"])  # prepend "00:" so it parses as mm:ss.SS
        .dt.total_seconds()
    )
    for col in ['quarter_1_time','quarter_2_time',	'quarter_3_time',	'quarter_4_time']:
        df_triples_master[col] = (
            pd.to_timedelta("00:" + df_triples_master[col])  # prepend "00:" so it parses as mm:ss.SS
            .dt.total_seconds()
        )

    df_triples_master.loc[(df_triples_master['date'] == '16 May 2024') & 
                    (df_triples_master['track'] == 'Melton VIC') & 
                    (df_triples_master['race_number'] == 9) & 
                    (df_triples_master['horse_name'] == 'LAIR OF THE EAGLE'), 'quarter_4_time'] = 246.48

    df_triples_master.loc[(df_triples_master['date'] == '04 December 2024') & 
                    (df_triples_master['track'] == 'Bathurst') & 
                    (df_triples_master['horse_name'] == 'MINSTREL'), 'quarter_4_time'] = 144.92

    # Clean some top speeds to be somewhat logical... 
    df_triples_master.loc[(df_triples_master['date'] == '18 August 2023') & 
                    (df_triples_master['track'] == 'Ballarat VIC') & 
                    (df_triples_master['horse_name'] == 'OPRAH DOUBLE YOU'), 'top_speed'] = 51

    df_triples_master.loc[(df_triples_master['date'] == '29 April 2023') & 
                    (df_triples_master['track'] == 'Melton VIC') & 
                    (df_triples_master['horse_name'] == 'SER PATRICK'), 'top_speed'] = 54


    # 12 July 2025	race 1. had TS90, update to 53.
    horsenames = ['CRAFTMANS CHARLIE', 'BOOM', 'DANCE AND DELIVER', 
                'FRANCO TYSON', 'PROMISING', 'DIAMOND ECLIPSE', 
                'ROMANEE', 'SURFERS DELIGHT', 'PITCH PERFECT']

    df_triples_master.loc[(df_triples_master['date'] == '12 July 2025') & 
                    (df_triples_master['race_number'] == 1) & 
                    (df_triples_master['horse_name'].isin(horsenames)), 'top_speed'] = 53

    df_triples_master.loc[(df_triples_master['date'] == '20 June 2025') & 
                    (df_triples_master['horse_name'] == 'CRUSADER MISS'), 'top_speed'] = 55

    df_triples_master.loc[(df_triples_master['date'] == '11 July 2025') & 
                    (df_triples_master['horse_name'] == 'LACEYS LAD'), 'top_speed'] = 53


    # In PJ dataset and here too but have a fair bit of shit data so removing them from triples... 
    bad_track_date_combos = {
        'Tabcorp Pk Menangle': ['14 February 2023', 
                                '18 February 2023', 
                                '21 February 2023', 
                                '25 February 2023',
                                '28 February 2023',
                                '19 August 2023',
                                '25 July 2023',
                                '09 December 2023'],
        'Tabcorp Park Menangle': ['03 August 2024'],
        'Geelong VIC': ['22 January 2023'],
        'Bendigo VIC': ['04 April 2023'],
        'Melton VIC': ['29 November 2023'],
    }

    for track, dates in bad_track_date_combos.items():
        for date in dates:
            df_triples_master = df_triples_master[~((df_triples_master['track'] == track) & (df_triples_master['date'] == date))]


    df_triples_master['time_400m'] = df_triples_master['quarter_4_time'] - df_triples_master['quarter_3_time']
    df_triples_master['time_800m'] = df_triples_master['quarter_3_time'] - df_triples_master['quarter_2_time']
    df_triples_master['time_1200m'] = df_triples_master['quarter_2_time'] - df_triples_master['quarter_1_time']
    df_triples_master['time_1600m'] = df_triples_master['quarter_1_time'] - df_triples_master['lead_time_value']
    df_triples_master['date'] = df_triples_master['date'].apply(extract_date)
    df_triples_master['track'] = df_triples_master['track'].apply(lambda x: x.lower().strip())
    df_triples_master['horse_name'] = df_triples_master['horse_name'].apply(lambda x: x.lower().strip())
    return df_triples_master


def master_merge_pjs_and_triples(df_pj_master: pd.DataFrame, df_triples_master: pd.DataFrame) -> pd.DataFrame:
    df_pj_master.to_csv('df_pj_master_post_cleaning.csv', index=False)
    df_triples_master.to_csv('df_triples_master_post_cleaning.csv', index=False)
    df_pj_to_merge = df_pj_master.rename(columns={'time_800m': 'time_800m_pj', 'time_400m': 'time_400m_pj', 'width_800m': 'width_800m_pj', 'width_400m': 'width_400m_pj', 'first_100m': 'first_100m_pj'})

    df = df_triples_master.merge(df_pj_to_merge[['date', 'track', 'horse_name', 'time_800m_pj', 'time_400m_pj', 'width_800m_pj', 'width_400m_pj', 'first_100m_pj', 'state_pj']], on=['date', 'horse_name'], how='outer')

    fill_na_mapping = {
        'time_800m': 'time_800m_pj',
        'time_400m': 'time_400m_pj',
        'first_100m': 'first_100m_pj',
        'state': 'state_pj',
        'track_x': 'track_y',
    }
    df.replace(to_replace='None', value=None, inplace=True)
    for col, fill_val in fill_na_mapping.items():
        df[col].fillna(df[fill_val], inplace=True)
        df.drop(columns=[fill_val], inplace=True)

    rename_cols = {
        'track_x': 'track',
    }

    for col, new_col in rename_cols.items():
        if col in df.columns:
            df.rename(columns={col: new_col}, inplace=True)


    df.sort_values(by='date', inplace=True)

    cols_to_keep = ['state', 'date', 'track', 'race_number', 'tab_number', 'horse_name',
        'lead_time_value', 'distance_travelled', 'top_speed', 'first_50m',
        'first_100m', 'first_200m', 'time_400m', 'time_800m', 'time_1200m',
        'time_1600m', 'width_800m_pj', 'width_400m_pj']

    df['distance_travelled'] = df['distance_travelled'].apply(lambda x: str(x).replace('m', '').replace('+', '').replace('-', '') if not pd.isna(x) else None)
    
    df = df[cols_to_keep].copy()
    return df