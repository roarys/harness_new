#!/usr/bin/env python3
"""
Configuration file for Harness Meta Scraper

Contains URLs, venues, and settings for each state's harness racing data sources.
This file can be customized to add new venues or update scraping endpoints.
"""

from typing import Dict, List

# State configurations with venues and potential data sources
STATE_CONFIG = {
    'nsw': {
        'name': 'New South Wales',
        'venues': {
            'menangle': {
                'name': 'Menangle Park',
                'codes': ['ME', 'Menangle'],
                'api_endpoint': None  # Add if available
            },
            'newcastle': {
                'name': 'Newcastle',
                'codes': ['NR', 'Newcastle'],
                'api_endpoint': None
            },
            'penrith': {
                'name': 'Penrith',
                'codes': ['PE', 'PC', 'Penrith'],
                'api_endpoint': None
            },
            'bathurst': {
                'name': 'Bathurst',
                'codes': ['BH', 'Bathurst'],
                'api_endpoint': None
            }
        },
        'base_url': None,  # Add state racing authority URL
        'sectionals_available': True
    },
    
    'vic': {
        'name': 'Victoria',
        'venues': {
            'melton': {
                'name': 'Melton',
                'codes': ['ME', 'Melton'],
                'api_endpoint': None
            },
            'cranbourne': {
                'name': 'Cranbourne',
                'codes': ['CR', 'Cranbourne'],
                'api_endpoint': None
            },
            'ballarat': {
                'name': 'Ballarat',
                'codes': ['BA', 'Ballarat'],
                'api_endpoint': None
            }
        },
        'base_url': None,
        'sectionals_available': True
    },
    
    'qld': {
        'name': 'Queensland',
        'venues': {
            'albion_park': {
                'name': 'Albion Park',
                'codes': ['AP', 'Albion_Park', 'AlbionPark'],
                'api_endpoint': None
            },
            'redcliffe': {
                'name': 'Redcliffe',
                'codes': ['RE', 'Redcliffe'],
                'api_endpoint': None
            }
        },
        'base_url': None,
        'sectionals_available': True
    },
    
    'sa': {
        'name': 'South Australia', 
        'venues': {
            'globe_derby': {
                'name': 'Globe Derby Park',
                'codes': ['GD', 'Globe_Derby'],
                'api_endpoint': None
            }
        },
        'base_url': None,
        'sectionals_available': True
    },
    
    'wa': {
        'name': 'Western Australia',
        'venues': {
            'gloucester_park': {
                'name': 'Gloucester Park',
                'codes': ['GP', 'Gloucester'],
                'api_endpoint': None
            },
            'pinjarra': {
                'name': 'Pinjarra',
                'codes': ['PN', 'Pinjarra'],
                'api_endpoint': None
            }
        },
        'base_url': None,
        'sectionals_available': False
    },
    
    'tas': {
        'name': 'Tasmania',
        'venues': {
            'hobart': {
                'name': 'Hobart',
                'codes': ['HO', 'Hobart'],
                'api_endpoint': None
            },
            'launceston': {
                'name': 'Launceston', 
                'codes': ['LA', 'Launceston'],
                'api_endpoint': None
            }
        },
        'base_url': None,
        'sectionals_available': True
    },
    
    'nt': {
        'name': 'Northern Territory',
        'venues': {
            'darwin': {
                'name': 'Darwin',
                'codes': ['DA', 'Darwin'],
                'api_endpoint': None
            }
        },
        'base_url': None,
        'sectionals_available': False
    }
}

# File naming patterns for different states
FILENAME_PATTERNS = {
    'nsw': [
        r'^(PE|PC|ME|NR|BH)\d{6}_\d{8}_.*\.pdf$',  # PE020425_20250402_unknown_H.pdf
        r'^\d{6}\s+.*_(unknown|H)\.pdf$'            # 170211_Newcastle_unknown_H.pdf
    ],
    'vic': [
        r'^\d{8}_.*_H\.pdf$',                       # 20221206_Melton_H.pdf
        r'^VIC_.*\.pdf$'
    ],
    'qld': [
        r'^\d{8}_.*_H\.pdf$',                       # 20221206_Albion_Park_H.pdf
        r'^QLD_.*\.pdf$'
    ],
    'sa': [
        r'^\d{8}_.*_H\.pdf$',
        r'^SA_.*\.pdf$'
    ],
    'tas': [
        r'^\d{8}_.*_H\.pdf$', 
        r'^TAS_.*\.pdf$'
    ],
    'wa': [
        r'^\d{8}_.*_H\.pdf$',
        r'^WA_.*\.pdf$'
    ],
    'nt': [
        r'^\d{8}_.*_H\.pdf$',
        r'^NT_.*\.pdf$'
    ]
}

# Processing settings
PROCESSING_CONFIG = {
    'max_retries': 3,
    'retry_delay': 1,  # seconds
    'timeout': 30,     # seconds per file
    'batch_size': 50,  # files per batch
    'parallel_workers': 4,
    'cleanup_staging': True,  # Clean up staging folders after processing
    'backup_failed': True,    # Keep backups of failed files
}

# Logging configuration
LOGGING_CONFIG = {
    'level': 'INFO',
    'max_log_files': 30,  # Keep last 30 log files
    'log_file_size_mb': 50
}

# Export formats
EXPORT_CONFIG = {
    'csv_format': 'standard',  # or 'detailed'
    'include_metadata': True,
    'timestamp_format': '%Y-%m-%d %H:%M:%S',
    'decimal_places': 2
}

def get_state_venues(state: str) -> List[str]:
    """Get list of venues for a state"""
    return list(STATE_CONFIG.get(state, {}).get('venues', {}).keys())

def get_venue_codes(state: str, venue: str) -> List[str]:
    """Get codes for a specific venue"""
    return STATE_CONFIG.get(state, {}).get('venues', {}).get(venue, {}).get('codes', [])

def is_sectionals_available(state: str) -> bool:
    """Check if sectionals are available for a state"""
    return STATE_CONFIG.get(state, {}).get('sectionals_available', False)

def get_filename_patterns(state: str) -> List[str]:
    """Get filename patterns for a state"""
    return FILENAME_PATTERNS.get(state, [])