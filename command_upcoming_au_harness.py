import schedule
import datetime
import pandas as pd
import asyncio
import atexit

from data_acquisition.upcoming_data_source import GetUpcomingData
from database_management.mongodb import clean_upcoming_greys_from_mongodb, save_to_mongodb
from static.telegram import send_telegram_message
from sp_data.betfair_data import pull_betfair_data
from sp_data.betwatch_data import BetwatchData
from retrying import retry    
from static.static_data import StaticData
from static.static_functions import DataFormatting

def run():
    @retry(wait_fixed=10000, stop_max_attempt_number=3)
    def update_data():
        print(f'Resetting upcoming database at {datetime.datetime.now()}')
        get_data = GetUpcomingData()
        master_upcoming_races = get_data.get_upcoming_meetings()
        if master_upcoming_races is None:
            print('No upcoming races found')
            return
        master_upcoming_races.rename(columns={'plannedStartTimestamp': 'date'}, inplace=True)
        
        bw = BetwatchData()
        static_data = StaticData()
        static_functions = DataFormatting()
        track_update_dict = static_data.track_name_updates

        df = asyncio.run(bw.pull_sequential_dates(num_search_days=2, 
                                                  meeting_types='H', 
                                                  look_forwards=True))
        
        
        df['date'] = df['date'].apply(lambda x: pd.to_datetime(x, errors='coerce').strftime('%Y-%m-%d'))
        
        master_upcoming_races['date'] = master_upcoming_races['date'].apply(lambda x: pd.to_datetime(x, errors='coerce'))
        master_upcoming_races['date'] = master_upcoming_races['date'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)
        
        df['track'] = df['track'].apply(lambda x: x.lower().strip().replace(' extra', ''))
        master_upcoming_races['track'] = master_upcoming_races['track'].apply(lambda x: x.lower().strip().replace(' extra', ''))

        # trackname updates
        master_upcoming_races['track'] = master_upcoming_races['track'].replace(track_update_dict)
        
        seen = {}
        new_cols = []
        
        for col in master_upcoming_races.columns:
            if col in seen:
                seen[col] += 1
                new_cols.append(f"{col}_{seen[col]}")
            else:
                seen[col] = 0
                new_cols.append(col)

        master_upcoming_races.columns = new_cols
        master_upcoming_races = master_upcoming_races.rename(columns={'gait': 'gaitRace', 'gait_1': 'gaitHorse'})
        master_upcoming_races = master_upcoming_races.rename(columns={'name': 'raceName', 'name_1': 'horseName'})
        if 'raceCode_1' in master_upcoming_races.columns:
            master_upcoming_races.drop(columns=['raceCode_1'], inplace=True)

        df.rename(columns={'dogname': 'horseName',
                           'dog_number': 'tab_horseNumber'}, inplace=True)
        master_upcoming_races.drop(columns=['horseName'], inplace=True)
        master_upcoming_races.rename(columns={'nameNoCountry': 'horseName'}, inplace=True)

        master_upcoming_races['horseName'] = master_upcoming_races['horseName'].apply(lambda x: x.lower().strip())
        df['horseName'] = df['horseName'].apply(lambda x: x.lower().strip())

        try:
            df_merged = master_upcoming_races.merge(df.drop(columns=['state']), 
                                                left_on=['date', 'track', 'horseName'],
                                                right_on=['date', 'track', 'horseName'],  
                                                how='left')
        except Exception as e:
            print(f'Error merging master_upcoming_races and df: {e}')
            return

        betfair_data = pull_betfair_data(race_code='harness') #horses, greyhounds, harness or all

        if not betfair_data.empty:
            betfair_data['date'] = betfair_data['market_start_time'].apply(lambda x: pd.to_datetime(str(x.date())).strftime('%Y-%m-%d'))
            
            df_merged['raceNumber'] = pd.to_numeric(df_merged['raceNumber'], errors='coerce')
            betfair_data['race_number'] = pd.to_numeric(betfair_data['race_number'], errors='coerce')

            df_merged['track'] = df_merged['track'].apply(lambda x: x.lower().strip())
            betfair_data['track'] = betfair_data['track'].apply(lambda x: x.lower().strip())
            
            # Merge these two so we have the betfair market idx
            df_merged = df_merged.drop(columns=['race_number']).merge(betfair_data[['date', 'track', 'betfair_market_id', 'race_number']], 
                        left_on=['date', 'track', 'raceNumber'], 
                        right_on=['date', 'track', 'race_number'],
                        how='left')
        else:
            df_merged['betfair_market_id'] = None

        
        df_merged = df_merged[~df_merged['state'].isin(['GBR'])]
        no_betwatch_match = df_merged[(df_merged['betfair_market_id'].isna()) & (df_merged['trials'] == False)]
        # no_betwatch_match.to_csv('no_betwatch_match.csv', index=False)
        if no_betwatch_match.shape[0] > 0:
            no_betwatch_match.to_csv('no_betfair_id.csv', index=False)
            if datetime.datetime.now().hour > 10:
                send_telegram_message(f'Found {no_betwatch_match.shape[0]} no betfair matches')

        # drop duplicates based on conditions
        df_merged.drop_duplicates(subset=['horseId', 'track', 'raceNumber'], inplace=True)

        df_merged['date_added'] = datetime.datetime.now()

        print(f'Found {len(df_merged["betfair_market_id"].unique())} unique betfair markets')

        # check to make sure ive seen the track and distances before
        df_merged = df_merged[~df_merged['distance'].isna()]
        unique_tracks = df_merged['track'].unique()

        for track in unique_tracks:
            if track.lower().strip() not in static_data.aus_track_distances.keys():
                send_telegram_message(f'Found a new track {track}??')

            for distance in list(df_merged[df_merged['track'] == track]['distance'].unique()):
                if distance not in static_data.aus_track_distances[track]:
                    send_telegram_message(f'Found a new distance {distance} for {track}??')
                
        # FIX THE COLUMN NAMES I WANT TO ADD so it errors if theres any changes 
        df_merged.rename(columns={'betfair_dog_id': 'betfair_horse_id'}, inplace=True)
        
        # removed cols: 'distanceInLaps','leadTime', 'plannedStartTimeLocal',
        # 'marginFirstToSecond', 'marginSecondToThird', 'mileRate',
        # 'overallTime', 'quarter1', 'quarter2', 'quarter3', 'quarter4', 
        # 'prizemoney13', 'prizemoney14', 'prizemoneyAll', 
        # 'raceStatus', 'lastSixStartsFigureForm',
        #  'driverInitials', 'driverLastName', 'driverNameShort', 'driverPreferredName', 'driverTitle',
        # 'horseFoalDateTime','trainerBirthDateTime', 'trainerInitials', 'trainerLastName', 'trainerNameShort', 'trainerPreferredName', 'trainerTitle',


        cols = ['ageRestriction', 'ageSexDescription', 'ageSexTrackRecord', 'alsoEligible',
                'barrierDrawType', 'betTypes', 'blackType', 'claim', 'claimRestrictionText',
                'discretionaryHandicap', 'distance', 'fieldSize',
                'gaitRace', 'meetingCode', 'monte', 'raceName', 'nameShort', 'notes',
                'numberAcrossFront',  'date',
                'prizemoneyPositions', 'raceClass', 'raceClassRestriction',
                'raceCode', 'raceNumber', 'raceStatus',
                'stakes', 'startType', 'stateBred', 'trackCondition',
                'age', 'barrier', 'breeder', 'breederId', 'claimingPrice', 'class',
                'colour', 'colourId', 'driverBirthDateTime', 'driverConcessionFlag',
                'driverGender', 'driverId', 'driverName', 
                'emergency', 'engagements', 'freezebrand', 'gaitHorse', 'handicap',
                'horseFoalDate',  'horseId', 'lateScratchingFlag',
                'horseName', 'odStatus', 'saddlecloth', 'scratchingFlag', 'sex',
                 'trainerDOB', 'trainerGender', 'trainerId', 
                'trainerName',  'trotterInPacersRace', 'club', 'clubId', 'dayNightTwilight',
                'driversAvailableTime', 'featureRaceText', 'lateScratchingTime',
                'meetingClass', 'state', 'tab', 'track', 'trackId', 'trials',
                'tab_horseNumber', 'betfair_horse_id', 
                'betfair_market_id', 'race_number', 'date_added']
        
        df_merged = static_functions.format_upcoming_data(df_merged[cols])
    
        if len(df_merged.trackCondition.unique()) > 1:
            send_telegram_message(f'Multiple track conditions found: {df_merged.trackCondition.unique()}')
                
        print('Saving to MongoDB')
        save_to_mongodb(df_merged, collection_name='harness_upcoming')
        clean_upcoming_greys_from_mongodb(collection_name='harness_upcoming')
        
        df_merged.to_csv('au_harness_upcoming.csv', index=False)
        try:
            if hasattr(bw.bw, '_BetwatchAsyncClient__exit'):
                bw.bw._BetwatchAsyncClient__exit()
                # Unregister the atexit handler after manual exit
                atexit.unregister(bw.bw._BetwatchAsyncClient__exit)
            else:
                print("The __exit method does not exist.")
        except Exception as e:
            print(f'Error closing betwatch client {e}')
    try:
        update_data()
    except Exception as e:
        send_telegram_message(f'Error updating upcoming database: {e}')

# This will run only once (so that when restarting the script it will also run + schedule)
def once():
    run()
    return schedule.CancelJob


def main(wait=None):
    # start_times = ["06:00", "08:00", "09:00", "10:00", "11:00", "13:00", "15:00", "17:00", "19:00"]
    start_times = ["09:00", "10:00", "11:00", "12:00", "13:00", 
                   "14:00", "15:00", "16:00", "17:00", "18:00"]
    for start_time in start_times:
        schedule.every().monday.at(start_time).do(run)
        schedule.every().tuesday.at(start_time).do(run)
        schedule.every().wednesday.at(start_time).do(run)
        schedule.every().thursday.at(start_time).do(run)
        schedule.every().friday.at(start_time).do(run)
        schedule.every().saturday.at(start_time).do(run)
        schedule.every().sunday.at(start_time).do(run)

    if not wait:
        schedule.every(1).seconds.do(once)
    else:
        print("Waiting until next scheduled time...")

    while True:
        schedule.run_pending()

if __name__ == "__main__":
    main()
    # get_data = GetUpcomingData()
    # master_upcoming_races = get_data.get_upcoming_meetings()
    # master_upcoming_races['date'] = master_upcoming_races['plannedStartTimestamp'].apply(lambda x: pd.to_datetime(x))
    # master_upcoming_races['date_day'] = master_upcoming_races['date'].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d'))
    # print(master_upcoming_races.columns)
    # print(master_upcoming_races.groupby(['date_day', 'track']).size())
    # master_upcoming_races.to_csv('master_upcoming_races.csv', index=False)
