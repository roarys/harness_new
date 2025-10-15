import asyncio
import os
import pandas as pd
import atexit

from datetime import datetime
from datetime import timedelta

import logging

from datetime import datetime, timedelta

import betwatch
from betwatch import *
from betwatch.types import *

import os
import dotenv
dotenv.load_dotenv()


class BetwatchData:
    def __init__(self):
        try:
            self.api_key = os.environ.get("BETWATCH_API_KEY")
            print(f'Successfully logged in to Betwatch')
        except KeyError:
            raise Exception("BETWATCH_API_KEY environment variable not set")

        self.bw = betwatch.connect_async(self.api_key)
        # setup logging
        logging.basicConfig(
                    level=logging.INFO,
                    format="%(asctime)s [%(levelname)s]: %(message)s",
                    handlers=[logging.StreamHandler()],
                    datefmt="%Y-%m-%d %H:%M:%S",
                )

        self.races: list[Race] = []
        # load races from pickle if exists
        self.data = {}

        # get some subset of races
        self.start_from_date = datetime.today()
        # days_ago = (datetime.today() - pd.to_datetime('2024-05-05')).days # amount of days to backtrack
        self.projection = RaceProjection(
                markets=True,
                flucs=True,
                links=True,
                betfair=True,
                bookmakers=[Bookmaker.TAB, Bookmaker.SPORTSBET],
            )
        
    async def pull_betwatch_data(self, date, meeting_types=None):
        existing_race_ids = [r.id for r in self.races]
        existing_race_dates = [r.meeting.date for r in self.races if r.meeting and r.meeting.date]

        if meeting_types:
            bw_meet_types = []
            for code in meeting_types:
                if code.upper() == 'G':
                    bw_meet_types.append(MeetingType.GREYHOUND)
                elif code.upper() == 'R' or code.upper() == 'T':
                    bw_meet_types.append(MeetingType.THOROUGHBRED)
                elif code.upper() == 'H':
                    bw_meet_types.append(MeetingType.HARNESS)
        else:
            bw_meet_types = [MeetingType.THOROUGHBRED, MeetingType.GREYHOUND, MeetingType.HARNESS]

        race_filter = RacesFilter(
            date_from=date,
            date_to=date,
            types=bw_meet_types,
            # has_bookmakers=[Bookmaker.TAB]
        )

        day_races = await self.bw.get_races(self.projection, race_filter)
        
        for race in day_races:
            if race.id not in existing_race_ids:
                self.races.append(race)
                existing_race_ids.append(race.id)
        print(f"Got {len(day_races)} races for {pd.to_datetime(date).strftime('%Y-%m-%d')}")
    
        return day_races
    

    async def pull_specific_dates(self, dates: list[str], meeting_types=None, add_tab_sb_prices=False):
        for date in dates:
            await self.pull_betwatch_data(date, meeting_types)
        return self.get_dataframe(add_tab_sb_prices=add_tab_sb_prices)

    async def pull_sequential_dates(self, num_search_days, meeting_types='G', look_forwards=True, add_tab_sb_prices=False):
        if look_forwards:
            dates = [self.start_from_date + timedelta(days=i) for i in range(num_search_days)] 
        else:
            dates = [self.start_from_date - timedelta(days=i) for i in range(num_search_days)]
        
        for date in dates:
            await self.pull_betwatch_data(date, meeting_types)
        
        df = self.get_dataframe(add_tab_sb_prices=add_tab_sb_prices)
        # df = pd.read_csv('betwatch_data.csv')
        if look_forwards:
            for col in ['bsp']:
                if col in df.columns:
                    df.drop(columns=[col], inplace=True)        
        return df

    
    def get_dataframe(self, add_tab_sb_prices=False):
        races = []

        for race in self.races:
            race_number = race.number
            race_name = race.meeting.track
            race_date = race.meeting.date
            race_state = race.meeting.location
            race_places = race.results
            if race_state in ['GBR']:
                continue

            for runner in race.runners:
                bsp = None
                ltp = None
                place_price = None
                runner_place = None
                runner_number = runner.number
                tab_price0 = None
                tab_time0 = None
                tab_price1 = None
                tab_time1 = None
                tab_price2 = None
                tab_time2 = None
                tab_price3 = None
                tab_time3 = None
                tab_price4 = None
                tab_time4 = None
                tab_price5 = None
                tab_time5 = None
                sb_price0 = None
                sb_time0 = None
                sb_price1 = None
                sb_time1 = None
                sb_price2 = None
                sb_time2 = None
                sb_price3 = None
                sb_time3 = None
                sb_price4 = None
                sb_time4 = None
                sb_price5 = None
                sb_time5 = None


                
                if not runner.is_scratched():
                    
                    if len(runner.betfair_markets) > 0:
                        # race_places is a list of lists, eg [[6], [1], [8], [4]] meaning dog with runner_number 6 finished in 1st, 8th and 4th place
                        if race_places:
                            for place_list in race_places:
                                if runner_number in place_list:
                                    runner_place = race_places.index(place_list) + 1
                                    break

                        try:
                            if runner.betfair_markets[0].market_name == 'win':
                                bsp = round(runner.betfair_markets[0].starting_price,2)
                                ltp = round(runner.betfair_markets[0].last_price_traded,2)
                                place_price = round(runner.betfair_markets[1].starting_price,2)

                            elif runner.betfair_markets[1].market_name == 'win':
                                bsp = round(runner.betfair_markets[1].starting_price,2)
                                ltp = round(runner.betfair_markets[1].last_price_traded,2)
                                place_price = round(runner.betfair_markets[0].starting_price,2)
                        except:
                            pass
                # somethign about adding tab and sb prices
                if add_tab_sb_prices:
                    for market in runner.bookmaker_markets:
                        if not market._fixed_win.flucs:
                            continue
                        if market._bookmaker.lower() == 'tab':
                            try:
                                tab_price0 = round(market._fixed_win.flucs[0].price,2)
                                tab_time0 = market._fixed_win.flucs[0].last_updated
                            except:
                                pass
                            try:
                                tab_price1 = round(market._fixed_win.flucs[1].price,2)
                                tab_time1 = market._fixed_win.flucs[1].last_updated
                            except:
                                pass
                            try:
                                tab_price2 = round(market._fixed_win.flucs[2].price,2)
                                tab_time2 = market._fixed_win.flucs[2].last_updated
                            except:
                                pass
                            try:
                                tab_price3 = round(market._fixed_win.flucs[3].price,2)
                                tab_time3 = market._fixed_win.flucs[3].last_updated
                            except:
                                pass
                            try:
                                tab_price4 = round(market._fixed_win.flucs[4].price,2)
                                tab_time4 = market._fixed_win.flucs[4].last_updated
                            except:
                                pass
                            try:
                                tab_price5 = round(market._fixed_win.flucs[5].price,2)
                                tab_time5 = market._fixed_win.flucs[5].last_updated
                            except:
                                pass
                        if market._bookmaker.lower() == 'sportsbet':
                            try:
                                sb_price0 = round(market._fixed_win.flucs[0].price,2)
                                sb_time0 = market._fixed_win.flucs[0].last_updated
                            except:
                                pass
                            try:
                                sb_price1 = round(market._fixed_win.flucs[1].price,2)
                                sb_time1 = market._fixed_win.flucs[1].last_updated
                            except:
                                pass
                            try:
                                sb_price2 = round(market._fixed_win.flucs[2].price,2)
                                sb_time2 = market._fixed_win.flucs[2].last_updated
                            except:
                                pass
                            try:
                                sb_price3 = round(market._fixed_win.flucs[3].price,2)
                                sb_time3 = market._fixed_win.flucs[3].last_updated
                            except:
                                pass
                            try:
                                sb_price4 = round(market._fixed_win.flucs[4].price,2)
                                sb_time4 = market._fixed_win.flucs[4].last_updated
                            except:
                                pass
                            try:
                                sb_price5 = round(market._fixed_win.flucs[5].price,2)
                                sb_time5 = market._fixed_win.flucs[5].last_updated
                            except:
                                pass
                    races.append([race_date, race_state, race_name, race_number,
                                    runner.name, runner.number, runner.betfair_id, runner_place, bsp, ltp, place_price, 
                                    tab_price0, tab_time0, tab_price1, tab_time1, tab_price2, tab_time2, tab_price3, tab_time3, tab_price4, tab_time4, tab_price5, tab_time5,
                                    sb_price0, sb_time0, sb_price1, sb_time1, sb_price2, sb_time2, sb_price3, sb_time3, sb_price4, sb_time4, sb_price5, sb_time5])
                else:
                    races.append([race_date, race_state, race_name, race_number,
                                    runner.name, runner.number, runner.betfair_id, bsp, ltp, place_price])

        if not add_tab_sb_prices:
            df = pd.DataFrame(races, columns=['date', 'state', 'track', 'race_number', 'dogname', 'dog_number', 'betfair_dog_id', 'bsp', 'ltp', 'bsp_place'])
        else:
            df = pd.DataFrame(races, columns=['date', 'state', 'track', 'race_number', 'dogname', 'dog_number', 'betfair_dog_id', 'place', 'bsp', 'ltp', 'bsp_place', 
                                              'tab_price0', 'tab_time0', 'tab_price1', 'tab_time1', 'tab_price2', 'tab_time2', 'tab_price3', 'tab_time3', 'tab_price4', 'tab_time4', 'tab_price5', 'tab_time5',
                                              'sb_price0', 'sb_time0', 'sb_price1', 'sb_time1', 'sb_price2', 'sb_time2', 'sb_price3', 'sb_time3', 'sb_price4', 'sb_time4', 'sb_price5', 'sb_time5'])  
        df['dogname'] = df['dogname'].apply(lambda x: x.lower().replace("'", '').replace(".", '').split('(')[0].strip())
        df['track'] = df['track'].apply(lambda x: x.lower().split('(')[0].strip())
        if not add_tab_sb_prices:
            df = df[df['bsp'].notna()]

        return df
                                 

if __name__ == '__main__':
    # from betfair_data import pull_betfair_data
    
    num_search_days = (datetime.today() - pd.to_datetime('2022-11-07')).days
    # num_search_days = 3
    bw = BetwatchData()
    df = asyncio.run(bw.pull_sequential_dates(num_search_days=num_search_days, meeting_types='R', look_forwards=False, add_tab_sb_prices=True))
    print(df.shape)
    if hasattr(bw.bw, '_BetwatchAsyncClient__exit'):
        bw.bw._BetwatchAsyncClient__exit()
        # Unregister the atexit handler after manual exit
        atexit.unregister(bw.bw._BetwatchAsyncClient__exit)
    else:
        print("The __exit method does not exist.")
    df.to_csv('betwatch_data_history_horses.csv', index=False)
    
        

    ### FOR remaking data to be added... 
    # file_name = 'ft_data'
    # df = pd.read_csv(f'{file_name}.csv')
    # df = df[df['place'].isin(['S', 'R', 'F', 'N', 'B', 'T', 'D', 'P'])]
    # print(df.place.value_counts())

    # # add betwatch data 
    # bw = BetwatchData()
    # df_bw = asyncio.run(bw.pull_sequential_dates(num_search_days=200, meeting_types='G', look_forwards=False))

    # # merge with betwatch data and keep 'bsp' and 'ltp' columns
    # df['date'] = pd.to_datetime(df['date'])
    # df['track'] = df['track'].apply(lambda x: x.lower().strip())
    # df['track'] = df['track'].apply(lambda x: 'sandown park' if 'sandown' in x.lower() else x.lower())
    # df['track'] = df['track'].apply(lambda x: 'the meadows' if 'meadows' in x.lower() else x.lower())
    # df_bw['date'] = pd.to_datetime(df_bw['date'])
    # df_bw['track'] = df_bw['track'].apply(lambda x: x.lower().strip())
    # df_bw['track'] = df_bw['track'].apply(lambda x: 'sandown park' if 'sandown' in x.lower() else x.lower())
    # df_bw['track'] = df_bw['track'].apply(lambda x: 'the meadows' if 'meadows' in x.lower() else x.lower())
    # df['dogname'] = df['dogname'].apply(lambda x: x.lower().strip())
    # df_bw['dogname'] = df_bw['dogname'].apply(lambda x: x.lower().strip())

    # df = df.merge(df_bw[['date', 'track', 'dogname', 'bsp', 'ltp']], right_on=['date', 'track', 'dogname'], left_on=['date', 'track', 'dogname'], how='left')
    # print(df.shape, df[df['bsp'].notna()].bsp.count())
    # print(df.ltp.describe())
    # df['bsp'] = df.apply(lambda x: x['ltp'] if x['bsp'] < 1 else x['bsp'], axis=1)
    # df['ltp'] = df.apply(lambda x: x['bsp'] if x['ltp'] < 1 else x['ltp'], axis=1)

    # rename = {'ltp': 'preplay_last_price_taken'}
    # df.rename(columns=rename, inplace=True)
    # df.to_csv(f'{file_name}_bsp_ltp.csv', index=False)


