import pandas as pd
from tqdm import tqdm
import time
from datetime import datetime, timedelta
from topaz import TopazAPI
import numpy as np
import requests
import os
import itertools
from dotenv import load_dotenv
from static.telegram import send_telegram_message

load_dotenv()


class GetHistoricalData:
    def __init__(self):
        self.racing_type = 'RISE Harness'
        self.today = datetime.now()

        self.API_KEY = os.getenv('RISE_API_KEY')
        self.BASE_URL = 'https://api.rise-digital.com.au/v2/races/{}/raceAndResults' # 'https://api.rise-digital.com.au/v2/races/{}/results'
        self.BASE_MEETING_URL = "https://api.rise-digital.com.au/v2/meetings"
        self.BASE_RACE_URL = 'https://api.rise-digital.com.au/v2/meetings/{}/races' 
        self.today_date = datetime.now().strftime("%Y-%m-%d")
        self.headers = {
            "x-api-key": self.API_KEY
        }
        self.params = {
            "earliest": self.today_date,
            "latest": self.today_date,
            "includeTrials": "true"
        }
        # self.track_name_updates = StaticData().track_name_updates
  
    def get_historical_data(self, days_ago=14):
        
        # return pd.read_csv('historical_data_TEMP.csv', low_memory=False)
    
        self.params['earliest'] = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        response = requests.get(self.BASE_MEETING_URL, headers=self.headers, params=self.params)
        upcoming_meetings = response.json()

        upcoming_meetings_df = pd.DataFrame(upcoming_meetings)
        meeting_codes = upcoming_meetings_df['meetingCode'].unique()

        races_df = pd.DataFrame()
        for meeting_code in tqdm(meeting_codes, desc='Pulling historical races'):
            meeting_url = self.BASE_RACE_URL.format(meeting_code)
            response = requests.get(meeting_url, headers=self.headers)
            races = response.json()
            if 'message' in races and 'not found' in races['message'].lower():
                track_df = upcoming_meetings_df[upcoming_meetings_df["meetingCode"] == meeting_code]
                msg = f'HARNESS: {track_df["track"].values[0]} not found in historical data'
                if track_df['trials'].values[0] == True:
                    msg += ' (trial)'
                send_telegram_message(msg)
                continue
            new_races_df = pd.DataFrame(races)
            if not new_races_df.empty:
                races_df = pd.concat([races_df, new_races_df])

        race_codes = races_df['raceCode'].unique() 
        race_and_results_df = pd.DataFrame() 
        for race_code in tqdm(race_codes, desc='Pulling historical race and results data'): 
            race_and_results_url = self.BASE_URL.format(race_code) 
            response = requests.get(race_and_results_url, headers=self.headers) 
            race_and_results = response.json() 
            if 'message' in race_and_results and 'not found' in race_and_results['message'].lower():
                send_telegram_message(f'Race {race_code} not found in historical data')
                continue
            new_races_df = pd.DataFrame([race_and_results])
            if not new_races_df.empty:
                race_and_results_df = pd.concat([race_and_results_df, new_races_df])
        print(f'Pulled {len(race_and_results_df)} historical {self.racing_type} race and results data')
        

        exploded_df = race_and_results_df.explode('raceResults').reset_index(drop=True)
        results_normalized = pd.json_normalize(exploded_df['raceResults'])
        result_df = pd.concat([exploded_df.drop(columns=['raceResults']), results_normalized], axis=1)
        print(f'Pulled {len(result_df)} historical {self.racing_type} runners')
        df_merged = result_df.merge(upcoming_meetings_df[['meetingCode', 'meetingStage', 'meetingStatus', 
                                                          'numberOfRaces', 'club', 'clubId', 
                                                          'dayNightTwilight', 'driversAvailableTime',
                                                          'featureRaceText',  'lateScratchingTime', 
                                                          'meetingClass', 'state', 'tab', 
                                                          'track', 'trackId', 'trials']], 
                                    left_on='meetingCode', 
                                    right_on='meetingCode', 
                                    how='left')
        
        df_merged.loc[df_merged['trials'] == True, 'tab'] = False
        df_merged['trainerTitle'] = df_merged['trainerTitle'].apply(lambda x: str(x).replace('19-08', '').replace('(None)', 'No Title').replace('nan', 'No Title'))
        valid_race_classes = ['C-CLASS', 'M-CLASS', 'B-CLASS', 'C-CLASS (MV)', 'AD-TRIAL', 'QU-TRIAL', 'OF-TRIAL', 'SHOW', 'RA-TRIAL', 'AWASKY']

        strange_classes = df_merged[~df_merged['raceClass'].isin(valid_race_classes)]
        if len(strange_classes) > 0:
            send_telegram_message(f'Found {len(strange_classes)} strange race classes {strange_classes["raceClass"].unique()}')
            strange_classes.to_csv('strange_classes.csv', index=False)

        cols_to_drop = ['lastUpdatedTime', 'lateScratchingTime', 'ageSexTrackRecord', 
                'betTypes', 'blackType', 'scheduledDateAsISO8601', 'distanceInLaps', 'numberAcrossFront',
                'plannedStartTimeLocal', 'prizemoney13', 'prizemoney14', 'prizemoneyAll', 
                'stateBred', 'driverInitials', 'driverLastName', 'driverNameShort',
                'driverPreferredName', 'driverTitle', 'horseFoalDateTime', 
                'odStatus', 'stewardsComments', 'toteFavourite', 
                'trainerBirthDateTime', 'trainerInitials', 'trainerLastName', 
                'trainerNameShort', 'trainerPreferredName', 'driverBirthDateTime', 'startTypeWord']
        
        for col in cols_to_drop:
            if col in df_merged.columns:
                df_merged = df_merged.drop(columns=[col])
            else:
                print(f'{col} not in df')

        # race_and_results_df.to_csv('race_and_results_df.csv', index=False)
        # upcoming_meetings_df.to_csv('upcoming_meetings_df.csv', index=False)
        # df_merged.to_csv('historical_data_TEMP.csv', index=False)

        # drop planned and non-tab
        df_merged = df_merged[(df_merged['tab'] == True) & (df_merged['meetingStage'] != 'PLANNED')]
        df_merged = df_merged[(~df_merged['meetingStage'].isin(['FIELDS']))]
        return df_merged 



if __name__ == "__main__":

    # download_topaz_data(define_topaz_api(TOPAZ_API_KEY),
    #                     generate_date_range(start_date, end_date),
    #                     JURISDICTION_CODES,
    #                     'HISTORICAL',
    #                     3,
    #                     3)

    get_data = GetHistoricalData()
    historical_data = get_data.get_historical_data(days_ago=14)
    historical_data.to_csv('historical_data.csv', index=False)
    print(historical_data.head())
