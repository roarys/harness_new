from dataclasses import dataclass
from typing import Dict, List

@dataclass
class StateConfig:
    name: str
    code: str
    base_url: str
    harness_url: str
    file_patterns: Dict[str, str]
    venues: List[str]

STATES_CONFIG = {
    'qld': StateConfig(
        name='Queensland',
        code='qld',
        base_url='https://www.racingqueensland.com.au',
        harness_url='https://www.racingqueensland.com.au/industry/harness/harness-sectionals',
        file_patterns={
            'pdf': r'/RacingFile\.ashx\?path=/(?:Harness)?Sectional/(\d{8})_(.+?)(?:_H)?\.pdf',
            'csv': r'/RacingFile\.ashx\?path=/(?:Harness)?Sectional/(\d{8})_(.+?)(?:_H)?\.csv',
            'zip': r'/RacingFile\.ashx\?path=/(?:Harness)?Sectional/(\d{8})_(.+?)(?:_H)?\.zip'
        },
        venues=['Albion_Park', 'Redcliffe', 'Marburg']
    ),
    'nsw': StateConfig(
        name='New South Wales',
        code='nsw',
        base_url='https://www.hrnsw.com.au',
        harness_url='https://www.hrnsw.com.au/racing/stewards/sectionaltimes',
        file_patterns={
            'pdf': r'/Uploads/files/Sectional(?:%20Times|%20Data)/(.+?)\.pdf'
        },
        venues=['ALBURY', 'ARMIDALE', 'BANKSTOWN', 'BATHURST', 'NEWCASTLE', 'YOUNG', 'MENANGLE']
    ),
    'vic': StateConfig(
        name='Victoria',
        code='vic',
        base_url='https://www.thetrots.com.au',
        harness_url='https://www.thetrots.com.au/racing/sectionals/',
        file_patterns={
            'pdf': r'sectionals\.s3\.us-west-1\.wasabisys\.com/.*\.pdf$'  # AWS S3 hosted sectionals
        },
        venues=['Melton', 'Ballarat', 'Geelong', 'Bendigo', 'Mildura', 'Warragul', 'Shepparton', 'Ararat', 'Cranbourne']
    ),
    'sa': StateConfig(
        name='South Australia',
        code='sa',
        base_url='https://satrots.com.au',
        harness_url='https://satrots.com.au/racing/horse-sectional-times/',
        file_patterns={
            'pdf': r'wp-content/uploads/docs/.*\.pdf'
        },
        venues=['Globe Derby Park', 'Gawler', 'Port Pirie']
    ),
    'wa': StateConfig(
        name='Western Australia',
        code='wa',
        base_url='',
        harness_url='',
        file_patterns={},
        venues=[]
    ),
    'tas': StateConfig(
        name='Tasmania',
        code='tas',
        base_url='https://test.tasracing.com.au',
        harness_url='https://test.tasracing.com.au/wp-content/uploads',
        file_patterns={
            'pdf': r'wp-content/uploads/\d{4}/\d{2}/[A-Za-z_-]+\d{8}\.pdf'
        },
        venues=['Hobart', 'Launceston', 'Devonport', 'Carrick']
    ),
    'nt': StateConfig(
        name='Northern Territory',
        code='nt',
        base_url='',
        harness_url='',
        file_patterns={},
        venues=[]
    )
}