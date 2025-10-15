import asyncio
import pandas as pd
import numpy as np
import os 
import dotenv
dotenv.load_dotenv()

import betfairlightweight as bf
from datetime import datetime
from datetime import timedelta

from sp_data.betwatch_data import BetwatchData
from static.telegram import send_telegram_message


def merge_new_data_with_betfair(df):
    # make df date timezone naive
    df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
    
    days_since_last_bet = (pd.to_datetime('today') - df.date.min()).days + 1
    print(f'Combining with Betfair Data. GOing to pull {days_since_last_bet} days of data from Betwatch.')

    bw = BetwatchData()
    asyncio.run(bw.pull_sequential_dates(num_search_days=days_since_last_bet, 
                                         meeting_types='H', 
                                         look_forwards=False))

    # make a dataframe from the betwatch data
    bw_data_list = []
    for race in bw.races:
        for runner in race.runners:
            if runner.is_scratched():
                continue
            try:
                if runner.betfair_markets[0].market_name == 'win':
                    bw_data_list.append([race.meeting.date, race.meeting.track, runner.name, 
                                         round(runner.betfair_markets[0].starting_price,2), 
                                         round(runner.betfair_markets[0].last_price_traded,2),
                                         round(runner.betfair_markets[1].starting_price,2)])
                elif runner.betfair_markets[1].market_name == 'win':
                    bw_data_list.append([race.meeting.date, race.meeting.track, runner.name, 
                                         round(runner.betfair_markets[1].starting_price,2), 
                                         round(runner.betfair_markets[1].last_price_traded,2),
                                         round(runner.betfair_markets[0].starting_price,2)])
                else:
                    print(f'WTF some issue with the betwatch markets not having a win? {[x.market_name for x in runner.betfair_markets]}')
            except Exception as e:
                pass

    bw_df = pd.DataFrame(bw_data_list, columns=['date', 'track', 'runner', 'bsp', 'preplay_last_price_taken', 'bsp_place'])
    bw_df['date'] = pd.to_datetime(bw_df['date']).dt.tz_localize(None)   
    bw_df['track'] = bw_df['track'].apply(lambda x: x.lower().split('(')[0].strip())
    bw_df['runner'] = bw_df['runner'].apply(lambda x: x.upper().replace("'", '').replace(".", '').split('(')[0].strip())
    bw_df = bw_df[bw_df['bsp'].notna()]
    # bw_df.to_csv('bw_df_CHECKNZNAMES.csv', index=False)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    bw_df['date'] = pd.to_datetime(bw_df['date'], errors='coerce')
    bw_df['date'] = bw_df['date'].dt.strftime('%Y-%m-%d')

    # Trying to add bsp to the whole dataframe
    df = df.merge(bw_df, right_on=['date', 'track', 'runner'], left_on=['date', 'track', 'horseName'], how='left')
    df.to_csv('df_CHECK_MERGE.csv', index=False)

    if 'runner' in df.columns:
        df.drop(columns=['runner'], inplace=True)
    df['preplay_last_price_taken'] = df.apply(lambda x: x['preplay_last_price_taken'] if x['preplay_last_price_taken'] > 1 else x['bsp'], axis=1)
    
    df, bsp_filled_with_startprice_count = fill_with_startprice_if_no_betfair_data(df)
    print(f'Filled {bsp_filled_with_startprice_count} rows with startprice data')
    if bsp_filled_with_startprice_count > 0:
        send_telegram_message(f'Filled {bsp_filled_with_startprice_count} rows with startprice data')
        
    df, adjustment_count = bidirectional_prioritisation_bsp_ltp(df)
    print(f'Adjusted {adjustment_count} rows based on BSP and LTP')
    if adjustment_count > 0:
        send_telegram_message(f'Adjusted {adjustment_count} rows based on having some bad BSP and LTP')

    return df

def fill_with_startprice_if_no_betfair_data(df):
    # Ensure 'bsp' is numeric and replace 0 with NaN
    df['bsp'] = pd.to_numeric(df['bsp'], errors='coerce').replace(0, np.nan)
    df['startingPriceTote'] = pd.to_numeric(df['startingPriceTote'], errors='coerce').replace(0, np.nan)
    df['inv_bsp'] = 1 / df['bsp']
    
    # Calculate market percentages for each race
    bsp_market_percentages = df.groupby(['raceCode', 'date'])['inv_bsp'].sum().reset_index(name='bsp_market_percentage')
    
    # Merge the market percentages back into the original DataFrame 
    df = df.merge(bsp_market_percentages, on=['raceCode', 'date'], how='left')

    # Introduce a flag to identify rows where 'bsp' will be replaced by 'startprice'
    def replace_bsp(row):
        if row['bsp_market_percentage'] == 0 and pd.notna(row['startingPriceTote']):
            return row['startingPriceTote'], 1
        return row['bsp'], 0
    
    df['bsp'], df['changed'] = zip(*df.apply(replace_bsp, axis=1))
    
    unique_changes_count = df.loc[df['changed'] == 1, 'raceCode'].nunique()
    
    # Drop the helper columns
    df.drop(columns=['inv_bsp', 'bsp_market_percentage', 'changed'], inplace=True)

    # Return the modified DataFrame and the count of unique '@id_y' values changed
    return df, unique_changes_count

def bidirectional_prioritisation_bsp_ltp(df, lower_threshold=0.8, upper_threshold=1.4):
    df['bsp'] = pd.to_numeric(df['bsp'], errors='coerce').replace(0, np.nan)
    df['preplay_last_price_taken'] = pd.to_numeric(df['preplay_last_price_taken'], errors='coerce').replace(0, np.nan)

    # Initialize a counter for adjustments and a list to store modified rows
    adjustment_count = 0
    
    # Calculate the inverse of bsp and ltp
    df['inv_bsp'] = 1 / df['bsp']
    df['inv_ltp'] = 1 / df['preplay_last_price_taken']
    
    # Calculate the market percentage for each race
    bsp_market_percentages = df.groupby(['raceCode', 'date'])['inv_bsp'].sum().reset_index(name='bsp_market_percentage')
    ltp_market_percentages = df.groupby(['raceCode', 'date'])['inv_ltp'].sum().reset_index(name='ltp_market_percentage')
    
    # Merge the two DataFrames on '@id_y' and 'date'
    market_percentages = pd.merge(bsp_market_percentages, ltp_market_percentages[['raceCode', 'date', 'ltp_market_percentage']], on=['raceCode', 'date'])
    
    # Merge the market percentages back into the original DataFrame
    df = df.merge(market_percentages, on=['raceCode', 'date'], how='left')
    
    # Adjust either 'preplay_last_price_taken' or 'bsp' based on the market percentages
    def check_and_adjust(row):
        nonlocal adjustment_count
        ltp_in_range = lower_threshold <= row['ltp_market_percentage'] <= upper_threshold
        bsp_in_range = lower_threshold <= row['bsp_market_percentage'] <= upper_threshold
        
        if not ltp_in_range and bsp_in_range:
            adjustment_count += 1
            return row['bsp'], row['bsp']
        elif ltp_in_range and not bsp_in_range:
            adjustment_count += 1
            return row['preplay_last_price_taken'], row['preplay_last_price_taken']
        else:
            # No change if both are in range or both are out of range
            return row['preplay_last_price_taken'], row['bsp']
    
    # Apply the function and track changes
    df[['preplay_last_price_taken', 'bsp']] = df.apply(lambda row: pd.Series(check_and_adjust(row)), axis=1)
    
    # Drop the helper columns
    df.drop(columns=['inv_bsp', 'inv_ltp', 'bsp_market_percentage', 'ltp_market_percentage'], inplace=True)
    
    return df, adjustment_count


def check_recent_mongo_for_bsp_updates(master_df, min_date):
    """ 
    See if there is any SP data that i should update with BSP data based on the given historical time horizon
    Need something like if date after x and sp is same as bsp? 
    """
    master_df['date'] = pd.to_datetime(master_df['date'])
    # Limit historical data to the last x days based on the new_data min date
    master_df = master_df[master_df['date'] >= min_date]
    no_bsp = master_df[(master_df['bsp'].isna()) | (master_df['bsp'] == 0) | (master_df['preplay_last_price_taken'].isna()) | (master_df['preplay_last_price_taken'] == 0)]
    
    # Assume we will need to do some checks on if they actually ran.. 
    no_bsp_has_runtime = no_bsp[~no_bsp['horseOverallTime'].isna()]
    print('No BSP but has runtime')
    print(no_bsp_has_runtime.shape)
    print(no_bsp_has_runtime.head())
    print(no_bsp_has_runtime.date.value_counts())
    print(no_bsp_has_runtime.track.value_counts())

    # NOW deal with these datapoints.. 
    no_bsp_has_runtime['date'] = no_bsp_has_runtime['date'].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d'))
    missing_dates = list(no_bsp_has_runtime.date.unique())
    bw = BetwatchData()
    df_missing = asyncio.run(bw.pull_specific_dates(missing_dates, meeting_types='H'))

    # Now want to fill the missing values in the bsp column of df_cleaned with the bsp column from df_bw
    def fill_bsp(row, df_bw, value='bsp'):
        
        updated_value = value
        if pd.isnull(row[value]):
            if value == 'preplay_last_price_taken':
                updated_value = 'ltp'
            try:
                new_datapoint = df_bw.loc[(df_bw['date'] == row['date']) & 
                            (df_bw['horseName'] == row['horseName']), updated_value].values[0]
                print(f'Filling {value} with {new_datapoint}')
                if pd.isnull(new_datapoint):
                    return row[value]
                return new_datapoint
            except:
                return row[value]
        else:
            return row[value]
    
    to_update = no_bsp_has_runtime.copy()
    
    to_update['bsp'] = to_update.apply(lambda x: fill_bsp(x, df_missing, value='bsp'), axis=1)
    to_update['preplay_last_price_taken'] = to_update.apply(lambda x: fill_bsp(x, df_missing, value='preplay_last_price_taken'), axis=1)

    print(to_update[to_update.bsp.isna()].date.value_counts())

    # Check to see if the data has changed
    print(f'Found {to_update.shape[0]} rows to update')
    df_changed_rows = []
    for index, row in to_update.iterrows():
        if row['bsp'] != no_bsp_has_runtime.loc[index, 'bsp']:
            if not pd.isnull(row['bsp']):
                print(f'Adding {row["bsp"]}')
                df_changed_rows.append(row)
        elif row['preplay_last_price_taken'] != no_bsp_has_runtime.loc[index, 'preplay_last_price_taken']:
            if not pd.isnull(row['preplay_last_price_taken']):
                print(f'Adding {row["preplay_last_price_taken"]}')
                df_changed_rows.append(row)

    df_changed_rows = pd.DataFrame(df_changed_rows)

    if df_changed_rows.shape[0] > 0:
        return df_changed_rows
    else:
        return pd.DataFrame()


def pull_betfair_data(race_code=None):
    def login():
        client = bf.APIClient(os.environ.get("BETFAIR_USER"), os.environ.get("BETFAIR_PASS"), app_key=os.environ.get("BETFAIR_API_KEY"))
        client.login_interactive()

        return client

    def format_betfair_markets(betfair_markets_today, harness_only=False):
        if len(betfair_markets_today) == 0:
            return pd.DataFrame()
        win_markets = []
        runners = []
        for market_object in betfair_markets_today:
            win_markets.append({
                'event_name': market_object.event.name,
                'event_id': market_object.event.id,
                'event_venue': market_object.event.venue,
                'market_name': market_object.market_name,
                'market_id': market_object.market_id,
                'market_start_time': market_object.market_start_time,
                'total_matched': market_object.total_matched
            })
            for runner in market_object.runners:
                runners.append({
                    'market_id': market_object.market_id,
                    'runner_id': runner.selection_id,
                    'runner_name': runner.runner_name.split('. ')[1].lower()
                })

        win_markets_df = pd.DataFrame(win_markets)
        runners_df = pd.DataFrame(runners)
        if harness_only:
            win_markets_df = win_markets_df[win_markets_df['market_name'].str.lower().str.contains('trot|pace', na=False)]

        win_markets_df.to_csv('win_markets_df.csv', index=False)
        runners_df.to_csv('runners_df.csv', index=False)

        win_markets_df['race_number'] = win_markets_df['market_name'].apply(
            lambda x: x[1:3].strip() if x[0] == 'R' else None)
        
        # Combine betfair market and runner information
        runners_df = runners_df.merge(
            win_markets_df[['market_id', 'event_venue', 'race_number', 'market_start_time']],
            how='left',
            on='market_id')

        runners_df['race_number'] = pd.to_numeric(runners_df['race_number'])
        runners_df['runner_name'] = runners_df['runner_name'].apply(
            lambda x: str(x).replace("'", "").replace(".", "").replace("Res", "").strip().lower())
        runners_df['event_venue'] = runners_df['event_venue'].apply(
            lambda x: str(x).lower().split('(')[0].strip())
        
        runners_df.rename(columns={'event_venue': 'track', 'market_id': 'betfair_market_id'}, inplace=True)
        return runners_df
    
    def pull_betfair_greyhound_markets(race_code=None, format_data=True):
        client = login()
        print(f'Pulling betfair markets for {race_code}')
        harness_only = False
        if race_code == 'all' or race_code == None:
            event_filter = bf.filters.market_filter(
                event_type_ids=["4339", "7"],  # filter event types to racing
                market_countries=["AU"],  # filter countries
                market_type_codes=["WIN"],  # filter on just WIN market types
            )
        elif race_code == 'greyhounds':
            event_filter = bf.filters.market_filter(
                event_type_ids=["4339"],  # filter event types to racing
                market_countries=["AU"],  # filter countries
                market_type_codes=["WIN"],  # filter on just WIN market types
            )
        elif race_code == 'harness':
            harness_only = True
            event_filter = bf.filters.market_filter(
                event_type_ids=["7"],  # filter event types to racing
                market_countries=["AU"],  # filter countries
                market_type_codes=["WIN"],  # filter on just WIN market types
                race_types=["Harness"]

            )
        elif race_code == 'horses':
            event_filter = bf.filters.market_filter(
                event_type_ids=["7"],  # filter event types to racing
                market_countries=["AU"],  # filter countries
                market_type_codes=["WIN"],  # filter on just WIN market types
                race_types=["Flat", "Hurdle", "Chase", "Bumper", "NH Flat"]
            )
        else:
            raise ValueError(f'Invalid race code: {race_code}')

        catalogues = client.betting.list_market_catalogue(
            filter=event_filter,
            market_projection=[
                "EVENT",
                "EVENT_TYPE",
                "MARKET_START_TIME",
                "MARKET_DESCRIPTION",
                "RUNNER_DESCRIPTION",
                "RUNNER_METADATA",
            ],
            max_results=100,
        )

        markets = []
        for catalogue in catalogues:
            markets.append(catalogue)

        markets = sorted(
            markets, key=lambda x: x.market_start_time
        )
        print(f'Got {len(markets)} betfair markets sauteed')
        # Remove markets that are not in the future
        # markets = [market for market in markets if market.market_start_time >= datetime.now()]
        # print(f'Removed {len(markets)} markets that are not in the future')

        if format_data:
            print('Formatting betfair markets into cute dataframes')
            markets = format_betfair_markets(markets, harness_only=harness_only)

        return markets

    return pull_betfair_greyhound_markets(race_code=race_code)


if __name__ == '__main__':
    print(f'Pulling Betfair Data')
    pull_betfair_data(race_code='harness')
