import time
import asyncio
import pandas as pd
import numpy as np

from static.telegram import send_telegram_message
from data_acquisition.data_cleaning import clean_error_dogs, clean_error_races, clean_datafields_formats, \
        bijective_dogname_and_id, bijective_race_ids, trainer_cleaning, check_order_of_runtime_and_places
from sp_data.betfair_data import merge_new_data_with_betfair, check_recent_mongo_for_bsp_updates
from database_management.mongodb import bulk_data_from_mongodb
from static.static_data import StaticData
from static.static_functions import DataFormatting
from stew_reports.meta_processor import MetaProcessor
from data_acquisition.sectional_cleaning import SectionalCleaning


class DataCleaning:
    def __init__(self, read_database=False):
        self.aus_track_distances = StaticData().aus_track_distances
        self.state_tracks = StaticData().state_tracks
        self.track_name_updates = StaticData().track_name_updates
        self.sectional_processor = MetaProcessor()
        self.sectional_cleaning = SectionalCleaning()
        self.bad_raceids = []
        if read_database:
            print('Reading database...')
            self.master_database = bulk_data_from_mongodb(collection_name='harness_historical')
        else:
            self.master_database = pd.DataFrame()

    def lightweight_cleaning(self, df, df_recent_data=None):
        t0 = time.time()
        print('Lightweight cleaning...')
        # df.to_csv('df_before_cleaning.csv', index=False)
        # df_recent_data.to_csv('df_recent_data_before_cleaning.csv', index=False)
        df_recent_data['date'] = pd.to_datetime(df_recent_data['date'])
        df['date'] = pd.to_datetime(df['date'])
        
        df = DataFormatting().format_historical_data(df)
        # return df
        
        # df = clean_error_dogs(df)
        # df = clean_error_races(df)
        # df = trainer_cleaning(df)
        
        # update track names
        df['track'] = df['track'].replace(self.track_name_updates)
        unique_tracks = df['track'].unique()
        for track in unique_tracks:
            if track not in self.aus_track_distances.keys():
                # send_telegram_message(f'Found a new track {track}??')
                print(f'Found a new track distance combo {track}')
            for distance in df[df['track'] == track]['distance'].unique():
                if distance not in self.aus_track_distances[track]:
                    # send_telegram_message(f'Found a new distance {distance} for {track}??')
                    print(f'Found a new distance {distance} for {track}')
        
        duplication_columns = ['raceCode', 'horseId']
        temp_merge = pd.concat([df, 
                                self.master_database], 
                                axis=0).drop_duplicates(subset=duplication_columns, 
                                                        keep='first')

        # Check for missing data
        num_no_horseName = temp_merge[temp_merge['horseName'].isna()]
        num_no_track = temp_merge[temp_merge['track'].isna()]
        num_no_idx = temp_merge[temp_merge['raceCode'].isna()]
        num_no_idy = temp_merge[temp_merge['horseId'].isna()]

        dataframes = {'num_no_horseName': num_no_horseName,
                        'num_no_track': num_no_track,
                        'num_no_idx': num_no_idx,
                        'num_no_idy': num_no_idy
                        }

        for name, dataframe in dataframes.items():
            if dataframe.shape[0] > 0:
                send_telegram_message(f'Found {dataframe.shape[0]} rows in {name} with no data... check logs...')
                print(f'DataFrame: {name}')
                print(dataframe)
                return None

        # Remove rows with no dogname
        temp_merge = temp_merge[~(temp_merge['horseId'].isna())]

        # Check to see if the @id_x has any other dognames
        idx_count = temp_merge.groupby('horseId')['horseName'].nunique()
        multiple_idx = idx_count[idx_count > 1].index
        for id_x in multiple_idx:
            # ONLY ADD HERE if they are genuinely different dogs...
            if str(id_x) in ['127028580', ]:
                continue
            unique_horseNames = temp_merge[temp_merge['horseId'] == id_x]['horseName'].unique()
            
            send_telegram_message(f'Found multiple horseNames for {id_x}, {unique_horseNames}')
            print(f'Found multiple horseName for {id_x}')
            print(unique_horseNames)
            races_to_drop = list(temp_merge[temp_merge['horseId'] == id_x]['raceCode'].unique())
            df = df[~df['raceCode'].isin(races_to_drop)]

        
        horseName_count = temp_merge.groupby('horseName')['horseId'].nunique()
        multiple_horseName = horseName_count[horseName_count > 1].index
        for horseName in multiple_horseName:
            # ONLY ADD HERE if they are genuinely the same dognames 
            if horseName in ['spring duty', 'spring vader', 
                            'evan almighty', 'hungry evan',
                            'brockie girl', 'brockies babe',
                            ]:
                continue
            unique_idx = temp_merge[temp_merge['horseName'] == horseName]['horseId'].unique()
            send_telegram_message(f'Found multiple id_x for {horseName}, {unique_idx}')
            print(f'Found multiple id_x for {horseName}')
            print(unique_idx)
            races_to_drop = list(temp_merge[temp_merge['horseName'] == horseName]['raceCode'].unique())
            df = df[~df['raceCode'].isin(races_to_drop)]

        # Check to see if the @id_y has any other races
        race_count = temp_merge.groupby('raceCode').date.nunique()
        multiple_race_ids = race_count[race_count > 1].index
        for id_y in multiple_race_ids:
            unique_dates = temp_merge[temp_merge['raceCode'] == id_y].date.unique()
            send_telegram_message(f'Found multiple dates for {id_y}')
            print(f'Found multiple dates for {id_y}')
            print(unique_dates)

        # Check to see if the order is the same for the places and runtimes
        order_check_results = check_order_of_runtime_and_places(df)
        bad_orders = order_check_results[order_check_results['is_order_same'] == False]
        if bad_orders.shape[0] > 0:
            send_telegram_message(f'Found {bad_orders.shape[0]} rows with bad racetime orders. ')
            print(bad_orders)
            races_to_drop = list(bad_orders['raceCode'].unique())
            df[df['raceCode'].isin(races_to_drop)].to_csv('bad_orders.csv', index=False)
            for race_id in races_to_drop:
                if len(df[(df['raceCode'] == race_id) & (df['horseOverallTime'] != '')]) < 2:
                    races_to_drop.remove(race_id)
            send_telegram_message(f'{len(races_to_drop)} bad races after filtering out races with only winners times... INSPECT THESE bad_orders_post.csv')
            df[df['raceCode'].isin(races_to_drop)].to_csv('bad_orders_post.csv', index=False)
            
            df = df[~df['raceCode'].isin(races_to_drop)]

        t5 = time.time()
        print(f'Lightweight cleaning done... now going to check updates and concat {round(t5 - t0, 2)}')

        # merge new data with betfair data
        df = merge_new_data_with_betfair(df)
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        min_date = df['date'].min()
        
        df_new_bsp_data = check_recent_mongo_for_bsp_updates(df_recent_data, min_date=min_date)
        if df_new_bsp_data.shape[0] > 0:
            send_telegram_message(f'Found {df_new_bsp_data.shape[0]} rows of data to update with BSP data...')
            print(df_new_bsp_data)

        # USING @id_x + @id_y combos as markers, check to see if there are any new rows of data to be added
        # this should return if there is onyl one piece of data that is different in the row too    
        # df_recent_data is the mongo db data which is the public facing one

        df['horseName'] = df['horseName'].apply(lambda x: str(x).lower())

        # check for new sectional data....
        df['date_test'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        new_dates = df['date_test'].unique()
        
        new_states = [x.lower() for x in df['state'].unique()]
        new_sectional_df = self.sectional_processor.run(
            states=new_states,
            dates=new_dates,
            days_back=0,
            skip_scraping=False,
            skip_processing=False,
            skip_cleaning=False
        )
        new_sectional_df['date_test'] = new_sectional_df['date']
        new_sectional_df.drop(columns=['date'], inplace=True)
        if new_sectional_df.shape[0] > 0:
            send_telegram_message(f'Found {new_sectional_df.shape[0]} rows of sectional data to add to the database...')
            # new_sectional_df.to_csv('new_sectional_df.csv', index=False)
            new_sectional_df['horse_name'] = new_sectional_df['horse_name'].apply(lambda x: str(x).lower())
            print(new_sectional_df.head())
            df = self.sectional_cleaning.staged_merge_with_aliases(
                df, new_sectional_df,
                date_col="date_test",
                left_name_col="horseName",
                right_name_col="horse_name",
                min_common_len=4,
                prefix_len=6,
                max_tail_pct=0.5,       
                max_next_word_len=12,     
                add_aliases=True,
                collapse_aliases=False,
                debug=True
            )
            df.to_csv('df_after_sectional_cleaning.csv', index=False)
            if 'date_test' in df.columns:
                df.drop(columns=['date_test'], inplace=True)            

        # Add some checks on data, anything that should 

        new_data = self.filter_new_data(df, df_recent_data)

        if new_data.shape[0] > 0:
            send_telegram_message(f'Found {new_data.shape[0]} new rows of data to add to the database...')
        else:
            send_telegram_message('No new data to update the database with, stopping here...')
            print('No new data to update the database with, stopping here...')
            return None
                 
        # Concatenate the new data with the updated BSP data for a full dataset that needs to be added. 
        # Only if the stew data has not been updated as this convers this step
        if df_new_bsp_data.shape[0] > 0: # and not updated_stew:
            print(f'No stew data to update the historical data with... just updating with bsp data...')
            new_data = pd.concat([new_data, df_new_bsp_data], axis=0)
        else:
            print(f'V strange, all historical data has stew and bsps...')
            send_telegram_message(f'V strange, all historical data has stew and bsps...')


        # NOWWWW should have this new_data dataframe which is the full dataset to be added to the database
        new_data['horseId'] = new_data['horseId'].astype(str)
        new_data['raceCode'] = new_data['raceCode'].astype(str)
        new_data['date_added'] = pd.Timestamp.now()
        new_data.drop_duplicates(subset=['horseId', 'raceCode'], keep='last', inplace=True)
        print(f'Ready to add {new_data.shape[0]} rows of data to the database...')
        send_telegram_message(f'Ready to add {new_data.shape[0]} rows of data to the database...')

        return new_data

    def heavy_cleaning(self, df):
        t0 = time.time()
        print('Heavy cleaning...')
        df = bijective_dogname_and_id(df)
        df = bijective_race_ids(df)
        

        t1 = time.time()
        print(f'Heavy cleaning done... {round(t1 - t0, 2)}')
        return df

    def filter_new_data(self, df, master_df):
        """
        Take in a DataFrame df and a master DataFrame master_df and return the rows in df that are not in master_df
        Very important to be checking even if there is only one datapoint different in the row
        """
        
        
        # Convert ID columns to the same data type. @@@ turn this into a function much earlier in the piece.... with data cleaning
        df['horseId'] = df['horseId'].apply(lambda x: str(x))
        df['raceCode'] = df['raceCode'].apply(lambda x: str(x))
        master_df['horseId'] = master_df['horseId'].apply(lambda x: str(x))
        master_df['raceCode'] = master_df['raceCode'].apply(lambda x: str(x))
        

        # Extract the common columns for comparison (excluding the identifiers)
        comparison_columns = [col for col in df.columns if col in ['place', 'horseName', 'horseId', 'driverName', 'driverId',
                                                                   'meetingClass', 'startingPriceTote', 'resultMargin', 'pir',
                                                                    'horseOverallTime', 'raceCode', 'meetingCode'
                                                                    'trainerName', 'trainerId', 'raceNumber', 'raceName', 
                                                                    'track', 'trackId', 
                                                                    'prizemoney', 'tab', 'trials', 'track', 'trackId', 'leadTime',
                                                                    'mileRate', 'marginFirstToSecond', 'marginSecondToThird',
                                                                    'overallTime', 'quarter1', 'quarter2', 'quarter3', 'quarter4',
                                                                    'distance', 'raceClass', 'date', 'stewardsCommentsLong',
                                                                    'stewardsCommentsShort', 'bsp', 'preplay_last_price_taken',
                                                                    'lead_time_value', 'additional_distance_travelled', 'top_speed', 
                                                                    'first_50m', 'first_100m', 'first_200m', 'time_400m', 'time_800m', 
                                                                    'time_1200m', 'time_1600m', 'width_800m_pj', 'width_400m_pj'
                                                                    ]]
        
        for col in comparison_columns:
            if col in ['bsp', 'preplay_last_price_taken']:
                master_df[col] = pd.to_numeric(master_df[col], errors='coerce').fillna(0)
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        df['place'] = df['place'].apply(lambda x: str(x) if pd.notna(x) else 0)
        master_df['place'] = master_df['place'].apply(lambda x: str(x) if pd.notna(x) else 0)
        
        df['date'] = df['date'].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d') if not pd.isna(x) else x)
        master_df['date'] = master_df['date'].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d') if not pd.isna(x) else x)

        # can maybe fill all na values with 0 for every col
        df.fillna(0, inplace=True)
        master_df.fillna(0, inplace=True)

        data_to_add = []
        for index, row in df.iterrows():
            to_add = True
            master_match = master_df[(master_df['horseId'] == row['horseId']) & (master_df['raceCode'] == row['raceCode'])]
            if not master_match.empty:
                to_add = False
                for col in comparison_columns:
                    # Properly handle NaN comparisons
                    row_value = row[col]
                    master_value = master_match[col].values[0]
                    
                    if (row_value in ['0', 0, '', 'nan']) or pd.isna(row_value):
                        if (master_value in ['0', 0, '', 'nan']) or pd.isna(master_value):
                            continue

                    if row_value != master_value:
                        if col in ['bsp', 'preplay_last_price_taken']:
                            # print(f'{row_value}:{master_value} in column {col} for @id_x {row["@id_x"]}, @id_y {row["@id_y"]}')
                            to_add = True
                            if master_value >= 1:
                                # if there is a valid datapoint, then we dont want to add the new data
                                to_add = False
                        elif col == 'place':
                            to_add = True
                            if row_value == '0':
                                # if the place is 0, then we dont want to add the new data, sometimes get 0's
                                to_add = False
                        elif col in ['lead_time_value', 'additional_distance_travelled', 'top_speed', 
                                'first_50m', 'first_100m', 'first_200m', 'time_400m', 'time_800m', 
                                'time_1200m', 'time_1600m', 'width_800m_pj', 'width_400m_pj']:
                            to_add = True
                            if ~((master_value in ['0', 0, '', 'nan']) or pd.isna(master_value)):
                                # if there is a valid datapoint, then we dont want to add the new data
                                to_add = False
                        else:
                            to_add = True
                            # Means we have a differing value in a non-bsp column
                            print(f'Found differing raw value in column {col} for horseId {row["horseId"]}, raceCode {row["raceCode"]}')
                            print(f'Value in df: {row_value}, Value in master_df: {master_value}')
                            break

            if to_add:
                data_to_add.append(row)

        # Display the results
        print(f"Found {len(data_to_add)} New Data to be added or updated in the master dataframe:")
        if len(data_to_add) == 0:
            return pd.DataFrame()

        new_data = pd.DataFrame(data_to_add)
        print(new_data.shape)
        new_data = new_data[~(new_data['horseId'].isna()) & ~(new_data['raceCode'].isna()) & ~(new_data['horseName'].isna())]
        print(f"NON NAN {len(data_to_add)} New Data to be added or updated in the master dataframe:")
        print(new_data.shape)
        print(master_df.shape)

        # Check for crossover data
        if not new_data.empty and not master_df.empty:
            crossover_df = new_data[(new_data['raceCode'].isin(master_df['raceCode'])) & (new_data['horseId'].isin(master_df['horseId']))]
            date_crossover = new_data[(new_data['date'].isin(master_df['date']))]
            inner_merge = pd.merge(new_data, master_df, on=['horseId', 'raceCode'], how='inner')

            if not inner_merge.empty:
                print(f'Found {inner_merge.shape[0]} rows of MERGED crossover data between the new data and the master data...')
                inner_merge.to_csv('crossover_data.csv', index=False)

            if not date_crossover.empty:
                if '0' in date_crossover['date'].unique():
                    print(f'ERROR: Got this strange date crossover with 0 date...')
                    bad_dates = date_crossover[date_crossover['date'] == '0'].copy()
                    bad_dates.to_csv('date_crossover_0.csv', index=False)
                    bad_races = bad_dates['raceCode'].unique()
                    new_data = new_data[~new_data['raceCode'].isin(bad_races)].copy()
                    date_crossover = date_crossover[~date_crossover['date'].isin(bad_dates['date'])]

                print(f'Found {date_crossover.shape[0]} rows of DATE crossover data between the new data and the master data...')

            if not crossover_df.empty:
                print(f'Found {crossover_df.shape[0]} rows of RAW crossover data between the new data and the master data...')
        

            # for the crossover data, check the differences between the new data and the master data 
            # to see if i should add the new data or if its redundant
            new_data['date'] = pd.to_datetime(new_data['date'], errors='coerce')
            oldest_new_data = new_data.sort_values(by='date')
            # new_data.to_csv('new_data_CHECK.csv', index=False)
            # new_data['date'] = new_data['date'].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d'))
            from pandas.api.types import is_datetime64_any_dtype as is_dt

            #  1) Parse once, tolerate junk
            if not is_dt(new_data['date']):
                new_data['date'] = pd.to_datetime(new_data['date'], errors='coerce', dayfirst=True)

            # 2a) If you truly want strings in the column:
            new_data['date'] = new_data['date'].dt.strftime('%Y-%m-%d')  # NaT -> NaN (no error)

            print(f'Oldest new data: {oldest_new_data.shape[0]} rows...')
            for index, row in oldest_new_data.iterrows():
                matching_master_row = master_df[(master_df['horseId'] == row['horseId']) & (master_df['raceCode'] == row['raceCode'])]
                if matching_master_row.shape[0] == 1:
                    matching_master_row = matching_master_row.iloc[0]
                                        
                    # Iterate over columns and print differences
                    for col in row.index:
                        if col in matching_master_row.index:
                            row_value = row[col]
                            master_value = matching_master_row[col]
                            if col in ['prizemoneyPositions']:
                                continue
                            if (row_value in ['0', 0, '', '0.0', 'nan']) or pd.isna(row_value):
                                if (master_value in ['0', 0, '', '0.0','nan']) or pd.isna(master_value):
                                    continue
                            if row_value != master_value and col in comparison_columns:
                                print(f"{col} - oldest_new_data: {row[col]}, master_df: {matching_master_row[col]}")

        new_data['date'] = pd.to_datetime(new_data['date']).dt.tz_localize(None)
        return new_data
    
