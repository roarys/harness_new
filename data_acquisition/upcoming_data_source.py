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
load_dotenv()

from static.telegram import send_telegram_message

class GetUpcomingData:
    def __init__(self):
        self.racing_type = 'RISE Harness'
        self.today = datetime.now()

        self.API_KEY = os.getenv('RISE_API_KEY')
        self.BASE_URL = "https://api.rise-digital.com.au/v2/meetings"
        self.BASE_RACE_URL = "https://api.rise-digital.com.au/v2/meetings/{}/races"
        self.BASE_RACE_AND_FIELDS_URL = "https://api.rise-digital.com.au/v2/races/{}/raceAndFields"
        self.today_date = datetime.now().strftime("%Y-%m-%d")
        self.headers = {
            "x-api-key": self.API_KEY
        }
        self.params = {
            "earliest": self.today_date,
            "latest": self.today_date,
            "includeTrials": "true"
        }

    def get_upcoming_meetings(self):
        print(f'Pulling upcoming {self.racing_type} meetings...')

        response = requests.get(self.BASE_URL, headers=self.headers, params=self.params)
        upcoming_meetings = response.json()

        upcoming_meetings_df = pd.DataFrame(upcoming_meetings)
        upcoming_meetings_df = upcoming_meetings_df[upcoming_meetings_df['meetingStage'] == 'FIELDS']

        if len(upcoming_meetings_df) < 1:
            # if its christmas or new years day then dont send a mesage
            if self.today.month == 12 and self.today.day == 25 or self.today.month == 1 and self.today.day == 1:
                pass    
            else:
                send_telegram_message(f'Issue pulling {self.racing_type} upcoming meetings, nothing available')
            return None

        meeting_codes = upcoming_meetings_df['meetingCode'].unique()
        races_df = pd.DataFrame()
        for meeting_code in tqdm(meeting_codes, desc='Pulling upcoming races'):
            meeting_url = self.BASE_RACE_URL.format(meeting_code)
            response = requests.get(meeting_url, headers=self.headers)
            races = response.json()
            if 'message' in races and 'not found' in races['message'].lower():
                track_df = upcoming_meetings_df[upcoming_meetings_df["meetingCode"] == meeting_code]
                msg = f'HARNESS: {track_df["track"].values[0]} not found in upcoming data'
                if track_df['trials'].values[0] == True:
                    msg += ' (trial)'
                send_telegram_message(msg)
                continue

            races_df = pd.concat([races_df, pd.DataFrame(races)])

        race_codes = races_df['raceCode'].unique()
        race_and_fields_df = pd.DataFrame()
        for race_code in tqdm(race_codes, desc='Pulling race and fields data'):
            race_and_fields_url = self.BASE_RACE_AND_FIELDS_URL.format(race_code)
            response = requests.get(race_and_fields_url, headers=self.headers)
            race_and_fields = response.json()
            race_and_fields_df = pd.concat([race_and_fields_df, pd.DataFrame([race_and_fields])])
        print(f'Pulled {len(race_and_fields_df)} upcoming {self.racing_type} race and fields data')
        
        exploded_df = race_and_fields_df.explode('raceFields').reset_index(drop=True)
        race_fields_normalized = pd.json_normalize(exploded_df['raceFields'])
        result_df = pd.concat([exploded_df.drop(columns=['raceFields']), race_fields_normalized], axis=1)
        print(f'Pulled {len(result_df)} upcoming {self.racing_type} runners')

        # supposed to merge this data with the upcoming_meetings_df to get track data accross
        df_merged = result_df.merge(upcoming_meetings_df[['meetingCode', 'club', 'clubId',
                                                          'dayNightTwilight', 'driversAvailableTime',
                                                          'featureRaceText',  'lateScratchingTime', 
                                                          'meetingClass', 'state', 'tab', 
                                                          'track', 'trackId', 'trials']], 
                                    left_on='meetingCode', 
                                    right_on='meetingCode', 
                                    how='left')
        return df_merged


if __name__ == '__main__':
    get_data = GetUpcomingData()

    # Historical data
    df = get_data.get_upcoming_meetings()
    # df.to_csv('upcoming_meetings.csv', index=False)
    print(df.groupby(['date', 'track']).track.count())

    # upcoming races
    # upcoming_data = get_data.upcoming_topaz_data()
    # upcoming_data.to_csv('upcoming_data.csv', index=False)
    # print(upcoming_data.groupby(['date', 'track']).track.count())