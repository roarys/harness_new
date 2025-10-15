import schedule
import datetime
import pandas as pd

from data_acquisition.historical_data_source import GetHistoricalData
from database_management.mongodb import bulk_data_from_mongodb, save_to_mongodb
from static.telegram import send_telegram_message
from data_acquisition.historic_cleaning import DataCleaning
from static.static_data import StaticData
from static.static_functions import DataFormatting


# TODO
# - Refine scheduling timings

def main_db_run(force_full_check=False):
    get_data_historical = GetHistoricalData()
    data_cleaning = DataCleaning()
    track_update_dict = StaticData().track_name_updates
    static_functions = DataFormatting()
    
    # df = get_data_topaz.get_historical_data(days_ago=2)
    # df.to_csv('historical_data.csv', index=False)
    # df['date_added'] = datetime.datetime.now()
    # save_to_mongodb(df, collection_name='harness_historical')
    # return
    # print(df.head())
    # print(df.shape)
    today = datetime.datetime.today()

    if today.weekday() == 0 and today.day <= 7 or force_full_check:
        # send_telegram_message("First monday of the month, cleaning out the house.....") 
        df_old = bulk_data_from_mongodb(collection_name='harness_historical', total_records=60000) 
        df_topaz = get_data_historical.get_historical_data(days_ago=15)
 
    elif today.weekday() == 1:
        # send_telegram_message("Tuesday, datachecks for the last two weeks...")
        df_old = bulk_data_from_mongodb(collection_name='harness_historical', total_records=30000)
        df_topaz = get_data_historical.get_historical_data(days_ago=12)

    else:
        # send_telegram_message("Regular day, doing a basic check and updating database...")
        df_old = bulk_data_from_mongodb(collection_name='harness_historical', total_records=20000)
        df_topaz = get_data_historical.get_historical_data(days_ago=4)
    
    print(df_old.head())

    # df_old.to_csv('df_old.csv', index=False)
    # df_topaz.to_csv('df_topaz.csv', index=False)
    # df_old.rename(columns={'plannedStartTimestamp': 'date'}, inplace=True)
    df_old['date'] = df_old['plannedStartTimestamp']
    df_old['date'] = pd.to_datetime(df_old['date']).dt.tz_localize(None)
    # df_topaz.rename(columns={'plannedStartTimestamp': 'date'}, inplace=True)
    df_topaz['date'] = df_topaz['plannedStartTimestamp']
    df_topaz['date'] = pd.to_datetime(df_topaz['date']).dt.tz_localize(None)

    print(f'Old shape: {df_topaz.shape}')
    print(f'Most recent date: {df_topaz["date"].max()}')
    print(f'Oldest date: {df_topaz["date"].min()}')

    # trackname updates
    df_topaz['track'] = df_topaz['track'].replace(track_update_dict)
    
    seen = {}
    new_cols = []

    for col in df_topaz.columns:
        if col in seen:
            seen[col] += 1
            new_cols.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            new_cols.append(col)

    df_topaz.columns = new_cols
    df_topaz = df_topaz.rename(columns={'gait': 'gaitRace', 'gait_1': 'gaitHorse'})
    df_topaz = df_topaz.rename(columns={'name': 'raceName', 'name_1': 'horseName'})
    df_topaz['horseName'] = df_topaz['nameNoCountry']
    df_topaz.drop(columns=['nameNoCountry'], inplace=True)

    if 'raceCode_1' in df_topaz.columns:
        df_topaz.drop(columns=['raceCode_1'], inplace=True)
    
    # new_data = data_cleaning.lightweight_cleaning(df_topaz, df_recent_data=None)
    # new_data['date_added'] = datetime.datetime.now()
    # save_to_mongodb(new_data, collection_name='harness_historical')
    # return
    # Check the data to make sure its not crazy, if it is, send a message to telegram
    new_data = data_cleaning.lightweight_cleaning(df_topaz, df_recent_data=df_old)
    
    if new_data is None:
        send_telegram_message("No new data to update the database with, stopping here...")
        return

    df_new = new_data.copy()
    
    # Cleaning function.... 
    if today.weekday() == 0:
        print('Doing a heavy duty clean of the full, new dataset...')
        df_new = data_cleaning.heavy_cleaning(df_new)

    print(df_new.shape)
    print(df_new.head())
    df_new.to_csv('df_new.csv', index=False)
    print(df_new.groupby(['date', 'track']).size())
    df_new['date'] = df_new['date'].apply(lambda x: pd.to_datetime(x))

    df_new_shape = df_new.shape
    if df_new_shape[0] == 0:
        send_telegram_message(f"Data has not changed, no update to database. New shape: {df_new_shape}")
    elif df_new_shape[0] > 5000:
        send_telegram_message(f"Data has increased by a large amount, something might is wrong. Difference {df_new_shape[0]}")

    df_new = static_functions.extract_stew_data(df_new)
    try:
        # df_new.rename(columns={'date': 'plannedStartTimestamp'}, inplace=True)
        df_new.drop(columns=['date'], inplace=True)
        df_new['plannedStartTimestamp'] = pd.to_datetime(df_new['plannedStartTimestamp'], utc=True, errors='coerce').dt.tz_localize(None)
        df_new['scheduledDate'] = df_new['plannedStartTimestamp']
        df_new.to_csv('df_new_pre_filter.csv', index=False)

        for col in ['scheduledDate', 'plannedStartTimestamp', 'date_added', 'driversAvailableTime']:
            df_new[col] = pd.to_datetime(df_new[col], utc=True, errors='coerce').dt.tz_localize(None)
            df_new[col] = df_new[col].where(df_new[col].notna(), None)
        df_new.rename(columns={'scheduledDate': 'date'}, inplace=True)

        df_new = df_new[['club', 'clubId', 'dayNightTwilight', 'driversAvailableTime', 
                         'featureRaceText', 'meetingClass', 'meetingCode', 'meetingStage', 
                         'meetingStatus', 'numberOfRaces', 'state', 'tab', 
                         'track', 'trackId', 'trials', 'ageRestriction', 'ageSexDescription', 
                         'alsoEligible', 'barrierDrawType', 'claim', 'claimRestrictionText', 
                         'discretionaryHandicap', 'distance', 'fieldSize', 'gaitRace', 
                         'leadTime', 'marginFirstToSecond', 'marginSecondToThird', 'mileRate', 
                         'monte', 'raceName', 'nameShort', 'notes', 'overallTime',  
                         'plannedStartTimestamp', 'prizemoneyPositions', 'quarter1', 
                         'quarter2', 'quarter3', 'quarter4', 'raceClass', 'raceClassRestriction', 
                         'raceCode', 'raceNumber', 'raceStatus', 'stakes', 'startType', 
                         'trackCondition', 'age', 'barrier', 'beatenMargin', 'breeder', 'breederId', 
                         'broodmareSireId', 'broodmareSireName', 'claimingPrice', 'colour', 'damId', 
                         'damName', 'deadHeatFlag', 'driverConcessionFlag', 'driverDOB', 'driverGender', 
                         'driverId', 'driverName', 'gaitHorse', 'handicap', 'horseFoalDate', 'horseId', 
                         'horseMileRate', 'horseOverallTime', 'lateScratchingFlag', 'horseName', 'owner', 
                         'ownerId', 'pastThePostPlacing', 'place', 'prizemoney', 'saddlecloth', 
                         'scratchingFlag', 'sex', 'sireId', 'sireName', 'startingPriceTote', 
                         'stewardsCommentsLong', 'stewardsCommentsShort', 'trainerDOB', 
                         'trainerGender', 'trainerId', 'trainerName', 'trainerTitle', 'trotterInPacersRace', 
                         'bsp', 'preplay_last_price_taken', 'long_stew_unclustered_features', 
                         'long_stew_clustered_features', 'date_added', 'date', 
                         'lead_time_value', 'additional_distance_travelled', 'top_speed', 
                         'first_50m', 'first_100m', 'first_200m', 'time_400m', 'time_800m', 
                         'time_1200m', 'time_1600m', 'width_800m_pj', 'width_400m_pj'
                         ]]
    

        date_str = datetime.datetime.now().strftime('%Y_%m_%d')
        time_str = datetime.datetime.now().strftime('%H_%M')
        df_new.to_csv(f'rise_database_{date_str}_{time_str}.csv', index=False)
        save_to_mongodb(df_new)

        send_telegram_message(f'Successfully saved to MongoDB and local SQL, added {df_new_shape[0]} new rows')
        print(f'Successfully saved to MongoDB and local SQL, added {df_new_shape[0]} new rows')
    except Exception as e:
        send_telegram_message(f'Issues saving to MongoDB or local SQL {e}')
        print(f'Issues saving to MongoDB or local SQL {e}')
    
    
    # Check to see what sectional times im missing for tracks
    # Do a check against the upcoming database to make sure ive covered the meets
    today = datetime.datetime.today()
    from_date = today - datetime.timedelta(days=2)
    df_new['date_temp'] = df_new['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    track_date_combos = df_new[['track', 'date_temp']].drop_duplicates()

    cols_to_check = ['time_400m']

    for track, date in track_date_combos.itertuples(index=False):
        if pd.to_datetime(date) > from_date:
            continue
        temp_df = df_new[(df_new['track'] == track) & (df_new['date_temp'] == date)].copy()
        # Check if they are all 0s
        for col in cols_to_check:
            if temp_df[col].sum() == 0 and pd.to_datetime(date) > pd.to_datetime('2025-01-01'):
                print(f'Missing sectional times for {track} on {date}')
                # send_telegram_message(f'Missing harness sectional times for {track} on {date}')
    df_new.drop(columns=['date_temp'], inplace=True)

    # Pull data from teh upcoming database on mongo
    historical_upcoming_data = bulk_data_from_mongodb(collection_name='harness_upcoming', total_records=20000)

    # REMOVE TRIAL DATA
    historical_upcoming_data = historical_upcoming_data[historical_upcoming_data['trials'] == False]

    no_dates = historical_upcoming_data[historical_upcoming_data['date'].isna()]
    no_dates.to_csv('no_dates.csv', index=False)
    historical_upcoming_data = historical_upcoming_data[historical_upcoming_data['date'].notna()]

    historical_upcoming_data['date'] = historical_upcoming_data['date'].apply(lambda x: pd.to_datetime(x, errors='coerce'))
    historical_upcoming_data['track'] = historical_upcoming_data['track'].str.lower()
    historical_upcoming_data['track'] = historical_upcoming_data['track'].replace(track_update_dict)
    historical_upcoming_data = historical_upcoming_data[historical_upcoming_data['date'] <= from_date]
    historical_upcoming_data['date'] = historical_upcoming_data['date'].apply(lambda x: x.strftime('%Y-%m-%d'))

    # Pull data from the new data
    historical_raw_data = bulk_data_from_mongodb(collection_name='harness_historical', total_records=30000)
    historical_raw_data['date'] = historical_raw_data['date'].apply(lambda x: pd.to_datetime(x, errors='coerce').strftime('%Y-%m-%d'))
    historical_raw_data['track'] = historical_raw_data['track'].str.lower()

    # Make sure all the track and date combos are in the raw data
    historical_upcoming_track_date_combos = historical_upcoming_data[['date', 'track']].drop_duplicates()
    historical_raw_track_date_combos = historical_raw_data[['date', 'track']].drop_duplicates()

    # Merge the two dataframes to find the differences
    difference_df = historical_upcoming_track_date_combos.merge(
        historical_raw_track_date_combos, 
        left_on=['date', 'track'], 
        right_on=['date', 'track'], 
        how='left', 
        indicator=True
    )
    # Filter for the rows that are only in the upcoming data
    upcoming_not_in_raw = difference_df[difference_df['_merge'] == 'left_only'].drop(columns=['_merge'])
    print(f'Checking from {historical_upcoming_data["date"].min()}')

    if not upcoming_not_in_raw.empty:
        send_telegram_message(f'Missing track date combos in the raw data: {upcoming_not_in_raw}')
        print(f'Missing track date combos in the raw data: {upcoming_not_in_raw}')
    else:
        print('All track date combos are in the raw data')

    print('Finished updating database for the day...')
    send_telegram_message('Finished updating database for the day...')
        


# This will run only once (so that when restarting the script it will also run + schedule)
def once():
    main_db_run()
    return schedule.CancelJob


def main_db_schedule(wait):
    start_time = ["07:00"]
    for time in start_time:
        schedule.every().monday.at(time).do(main_db_run)
        schedule.every().tuesday.at(time).do(main_db_run)
        schedule.every().wednesday.at(time).do(main_db_run)
        schedule.every().thursday.at(time).do(main_db_run)
        schedule.every().friday.at(time).do(main_db_run)
        schedule.every().saturday.at(time).do(main_db_run)
        schedule.every().sunday.at(time).do(main_db_run)

    if not wait:
        schedule.every(1).seconds.do(once)
    else:
        print("Waiting until next scheduled time...")

    while True:
        schedule.run_pending()

if __name__ == "__main__":
    main_db_schedule(None)
