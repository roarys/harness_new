import re
import PyPDF2
import pytesseract
import pdf2image
import numpy as np
from fuzzywuzzy import fuzz
import ast
import pandas as pd
from datetime import datetime
from dateutil.parser import parse
from PIL import Image
from static.telegram import send_telegram_message

from database_management.mongodb import bulk_data_from_mongodb
from static.static_data import StaticData

class DataFormatting:
    def __init__(self) -> None:
        self.track_update_dict = {
            'tabcorp pk menangle': 'menangle',
            'globe derby park': 'globe derby',
            'lockyer (gatton)': 'gatton',
            'central wheatbelt': 'kellerberrin'
        }
        self.valid_race_classes = ['C-CLASS', 'M-CLASS', 'B-CLASS', 'C-CLASS (MV)', 'AD-TRIAL', 'QU-TRIAL', 'OF-TRIAL', 'SHOW', 'RA-TRIAL', 'AWASKY']
        self.cols_to_drop = ['lastUpdatedTime', 'lateScratchingTime', 'ageSexTrackRecord', 
                'betTypes', 'blackType', 'scheduledDateAsISO8601', 'distanceInLaps', 'numberAcrossFront',
                'plannedStartTimeLocal', 'prizemoney13', 'prizemoney14', 'prizemoneyAll', 
                'stateBred', 'driverInitials', 'driverLastName', 'driverNameShort',
                'driverPreferredName', 'driverTitle', 'horseFoalDateTime', 
                'nameNoCountry', 'odStatus', 'stewardsComments', 'toteFavourite', 
                'trainerBirthDateTime', 'trainerInitials', 'trainerLastName', 
                'trainerNameShort', 'trainerPreferredName', 'driverBirthDateTime', 'startTypeWord']
        
        self.df_long_stew_word_counts = pd.read_csv('long_word_counts.csv')
        self.df_lookup = pd.read_csv('final_harness_master_stewards_lookup_table.csv')
        self.unclustered_features = pd.read_csv('unclustered_features.csv')
        self.upcoming_id_cols = ['clubId', 'trackId', 'breederId', 'broodmareSireId', 'damId', 'driverId', 
                   'horseId', 'trainerId']
        self.historical_id_cols = ['clubId', 'trackId', 'breederId', 'broodmareSireId', 'damId', 'driverId', 
                   'horseId', 'ownerId', 'sireId', 'trainerId']


    def extract_stew_data(self, df):
        self.df_lookup['MAJOR CATEGORY'] = self.df_lookup['MAJOR CATEGORY'].apply(lambda x: x.lower().strip().replace(' ', '_'))
        self.df_lookup['SUB-CATEGORY'] = self.df_lookup['SUB-CATEGORY'].apply(lambda x: x.lower().strip().replace(' ', '_'))
        df['stewardsCommentsLong'] = df['stewardsCommentsLong'].str.replace("'", "").str.replace('.', '').str.lower()
        
        def fuzzy_match_all_terms(text, 
                                terms=None, 
                                threshold=100, 
                                lookup_dict=None):
            
            if terms is None:
                return False
            
            # text = str(text).lower()

            matched_subcategories = {}
            if lookup_dict is None:
                # full_dict = {
                #                 term: int(fuzz.partial_ratio(term.lower(), text) >= threshold)
                #                 for term in terms
                #             }
                # matched_subcategories = {
                #         key.replace(' ', '_'): value for key, value in full_dict.items() if value == 1
                #     }
                for term in terms:
                    term_lower = term.lower()
                    index = text.lower().find(term_lower)

                    if index != -1:
                        after_index = index + len(term_lower)
                        if after_index == len(text) or text[after_index] == ',':
                            matched_subcategories[term] = 1
                matched_subcategories = {
                        key.replace(',', '').replace(' ', '_'): value for key, value in matched_subcategories.items() if value == 1
                    }
            else:
                # print(f'Text: {text}')
                for term in terms:
                    term_lower = term.lower()
                    index = text.lower().find(term_lower)

                    if index != -1:
                        after_index = index + len(term_lower)
                        if after_index == len(text) or text[after_index] == ',':
                            matched_subcategories[lookup_dict[term].replace(' ', '_')] = 1
            #     matched_subcategories = {
            #     lookup_dict[term].replace(' ', '_'): 1
            #     for term in terms
            #     if fuzz.partial_ratio(term, text) >= threshold
            # }
            return matched_subcategories

        ### CLUSTERED FEATURES

        term_to_subcategory_mapping = dict(zip(self.df_lookup['INDIVIDUAL COMMENT'].str.lower().replace(',', ''), 
                                               self.df_lookup['SUB-CATEGORY']))
        terms = list(term_to_subcategory_mapping.keys())

        df['clustered_features'] = df['stewardsCommentsLong'].apply(fuzzy_match_all_terms, 
                                                                            terms=terms, 
                                                                            threshold=100,
                                                                            lookup_dict=term_to_subcategory_mapping)

        df.rename(columns={'clustered_features': 'long_stew_clustered_features'}, inplace=True)
        

        # UNCLUSTERED FEATURES
        df['long_stew_unclustered_features'] = df['stewardsCommentsLong'].apply(fuzzy_match_all_terms, 
                                                                                  terms=self.unclustered_features['term'].tolist(), 
                                                                                  threshold=99)

        # POST PROCESSING FEATURES
        df['long_stew_clustered_features'] = df['long_stew_clustered_features'].apply(self.safe_to_dict)
        df['long_stew_unclustered_features'] = df['long_stew_unclustered_features'].apply(self.safe_to_dict)
        
        def post_process_stew(df_temp, col_name):
            df_temp[col_name] = df_temp[col_name].map(
                lambda d: '|'.join(d.keys())
                )
            return df_temp
        
        df = post_process_stew(df, 'long_stew_unclustered_features')
        df = post_process_stew(df, 'long_stew_clustered_features')

        return df
    
    def safe_to_dict(self, val):
        if isinstance(val, str):
            try:
                parsed = ast.literal_eval(val)
                if isinstance(parsed, dict):
                    return parsed
            except:
                pass
        return val
        
    def format_historical_data(self, df):
        df = self.set_datapoint_types(df)
        df = self.update_track_names(df)
        df = self.handle_duplicate_columns(df)
        df = self.drop_cols(df)
        df = self.convert_hex_to_decimal_string(df, self.historical_id_cols)

        df['stewardsCommentsLong'] = df['stewardsCommentsLong'].apply(lambda x: str(x).replace("'", "").replace(".", "").lower())
        df['marginFirstToSecond'] = df['marginFirstToSecond'].apply(lambda x: str(x).replace("m", "") if pd.notna(x) else np.nan)
        df['marginSecondToThird'] = df['marginSecondToThird'].apply(lambda x: str(x).replace("m", "") if pd.notna(x) else np.nan)

        if len(df[~df['raceClass'].isin(self.valid_race_classes)]) > 0:
            bad_races = df[~df['raceClass'].isin(self.valid_race_classes)]
            send_telegram_message(f'Found {len(bad_races)} bad races in database with raceClasses: {bad_races["raceClass"].unique()}')
            df = df[df['raceClass'].isin(self.valid_race_classes)]

        return df
    
    def format_upcoming_data(self, df):
        df = self.set_datapoint_types(df)
        df = self.update_track_names(df)
        df = self.convert_hex_to_decimal_string(df, self.upcoming_id_cols)

        return df
    
    def convert_hex_to_decimal_string(self, df, cols):
        for col in cols:
            if col in df.columns:
                df[col] = df[col].apply(
                        lambda x: str(int(x, 16)) if (pd.notna(x) and str(x).strip() != '') else np.nan
                    )
        return df
    
    def drop_cols(self, df):
        for col in self.cols_to_drop:
            if col in df.columns:
                df = df.drop(columns=[col])
        return df
    
    def update_track_names(self, df):
        df['track'] = df['track'].str.lower()
        df['track'] = df['track'].replace(self.track_update_dict)

        return df
    
    def handle_duplicate_columns(self, df):
        seen = {}
        new_cols = []

        for col in df.columns:
            if col in seen:
                seen[col] += 1
                new_cols.append(f"{col}_{seen[col]}")
                if col in ['gait', 'name']:
                    # known duplicate columns
                    pass
                else:
                    send_telegram_message(f'Found duplicate column in database: {col}')
            else:
                seen[col] = 0
                new_cols.append(col)

        df.columns = new_cols

        df = df.rename(columns={'gait': 'gaitRace', 'gait.1': 'gaitHorse'})
        df = df.rename(columns={'name': 'raceName', 'name.1': 'horseName'})

        if 'raceCode.1' in df.columns: df.drop(columns=['raceCode.1'], inplace=True) 
        if 'Unnamed: 0' in df.columns: df.drop(columns=['Unnamed: 0'], inplace=True) 
        return df
    
    def set_datapoint_types(self, df):
        for col in df.columns:
            if col in ['date', 'driversAvailableTime', 'scheduledDate', 'plannedStartTimestamp', 
                    'driverDOB', 'horseFoalDate', 'trainerDOB', 'date_added', 'lateScratchingTime']:
                df = self.set_to_datetime(df, col, set_utc=True)

            elif col in ['numberOfRaces', 'fieldSize', 'raceNumber',  
                        'age', 'claimingPrice', 'saddlecloth', 'distance', 'numberAcrossFront',
                        'race_number', 'tab_horseNumber']:
                df = self.set_to_int(df, col, fill_na_value=np.nan)

            elif col in ['prizemoney', 'leadTime', 'beatenMargin', 'startingPriceTote', 'stakes']:
                df = self.set_to_float(df, col, fill_na_value=np.nan)

            elif col in ['tab', 'trials', 'claim', 'discretionaryHandicap', 
                        'monte', 'deadHeatFlag', 'lateScratchingFlag', 'scratchingFlag', 
                        'trotterInPacersRace']:
                df = self.set_to_bool(df, col)
            
            elif col in ['prizemoneyPositions']:
                # Other formats like dictionaries, lists, etc.
                continue
            elif col in ['quarter1', 'quarter2', 'quarter3', 'quarter4']:
                df = self.set_quarters(df, col)
            else:
                df = self.set_to_string(df, col)
        return df
    
    def set_quarters(self, df, col):
        df = df.copy()
        def string_of_float_if_not_none(x):
            x = str(x)
            if x is None or str(x) == 'nan':
                return 'nan'
            elif '.' in x:
                return x
            else:
                return x+'.0'
        df[col] = df[col].apply(string_of_float_if_not_none)
        return df
    
    def set_to_string(self, df, col):
        df = df.copy()
        df[col] = df[col].astype(str).where(df[col].notnull(), None)
        return df

    def set_to_int(self, df, col, fill_na_value=np.nan):
        df = df.copy()
        # Replace NaNs with sentinel, cast, then restore
        sentinel = -999
        df[col] = df[col].fillna(sentinel)
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        df.loc[df[col] == sentinel, col] = pd.NA
        if pd.isna(fill_na_value):
            df[col] = df[col].astype('Int64')
        else:
            df[col] = df[col].fillna(fill_na_value).astype(int)
        return df

    def set_to_float(self, df, col, fill_na_value=np.nan):
        df = df.copy()
        sentinel = -999.0
        df[col] = df[col].fillna(sentinel)
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df.loc[df[col] == sentinel, col] = np.nan
        if not pd.isna(fill_na_value):
            df[col] = df[col].fillna(fill_na_value)
        return df

    def set_to_bool(self, df, col):
        df = df.copy()
        df[col] = df[col].apply(lambda x: str(x).strip().lower() == 'true' if pd.notna(x) else False)
        return df

    def set_to_datetime(self, df, col, set_utc=False):
        df = df.copy()
        df[col] = pd.to_datetime(df[col], utc=set_utc)
        df[col] = df[col].replace({pd.NaT: None})
        return df
    

class StaticFunctions:
    def __init__(self) -> None:
        self.upcoming_data = bulk_data_from_mongodb(collection_name='harness_upcoming', total_records=60000)
        self.race_id_to_prize_money = {}
        self.static_data = StaticData()
        self.historic_data = pd.read_csv('test_df_recent_data.csv')
        self.historic_data['date'] = pd.to_datetime(self.historic_data['date'])
        self.historic_data = self.historic_data[self.historic_data['date'] >= pd.to_datetime('2025-01-01')]
        self.vic_tracks = self.static_data.state_tracks['vic']
        self.prize_money_data = self.static_data.prize_money_data
        
    def run(self):
        print(self.historic_data[self.historic_data['track'].isin(self.vic_tracks)].groupby(['track', 'date']).raceId.nunique())
        print(self.upcoming_data[self.upcoming_data['track'].isin(self.vic_tracks)].groupby(['track', 'date']).raceId.nunique())
        self.historic_data['prizeMoney'] = self.historic_data.apply(lambda row: self.update_vic_prizemoney(row) if row['track'] in self.vic_tracks else row['prizeMoney'], axis=1)
        self.historic_data.to_csv('test_df_recent_data_updated.csv')

    def update_vic_prizemoney(self, row):
        if row.date < pd.to_datetime('2025-01-03'):
            return row.prizeMoney
        prize_money = row.prizeMoney
        if row.prizeMoney < 1:
            race_id = str(row.raceId).split('.')[0]
            race_df = self.upcoming_data[self.upcoming_data['raceId'] == race_id]
            if race_id not in self.race_id_to_prize_money.keys():
                if race_df.shape[0] == 0:
                    print(f'No race data for race {race_id} in upcoming data')
                    return row.prizeMoney
                # most common prize money
                prize_money = race_df['prizeMoney'].mode()[0]
                # print(f'Most common prize money for race {race_id} {row.track} {row.date} {row.raceNumber} is {prize_money}')
                self.race_id_to_prize_money[race_id] = prize_money
            else:
                prize_money = self.race_id_to_prize_money[race_id]
            
            if prize_money == 0 or prize_money is None or prize_money == 'nan':
                print(f'No prize money for race {race_id}')
                return row.prizeMoney
            place = row.place
            if place not in [1, 2, 3, 4]:
                if place not in ['1', '2', '3', '4']:
                    return 0
                
            (first_guess, second_guess, third_guess, fourth_guess) = self.compute_prizes(int(prize_money), row)
            if first_guess is None:
                return 0
            place = str(float(place))
            race_df['place'] = race_df['place'].astype(str)
            place_df = race_df[race_df['place'] == place] ### error here 
            # print(f'Made it here {place_df.shape}')
            double = False
            if len(place_df) > 1:
                print(f'Double up on place {place} in race {race_id}')
                double = True
                    # get the runIds to update prizeMoney data
            if place == '1.0':
                if double:
                    prize_money = (first_guess + second_guess) / 2
                else:
                    prize_money = first_guess
            elif place == '2.0':
                if double:
                    prize_money = (second_guess + third_guess) / 2
                else:
                    prize_money = second_guess
            elif place == '3.0':
                if double:
                    prize_money = (third_guess + fourth_guess) / 2
                else:
                    prize_money = third_guess
            elif place == '4.0':
                if double:
                    prize_money = (fourth_guess) / 2
                else:
                    prize_money = fourth_guess
            else:
                print(f'Place is not 1, 2, 3, or 4: {place}')
                return row.prizeMoney
        else:
            prize_money = row.prizeMoney
            print(f'Already have prize money {prize_money} for race {row.raceId} at {row.track} on {row.date}')
        return prize_money
    
    def compute_prizes(self, total_prize_money, row=None):
        if total_prize_money % 5 != 0:
            if row is not None:
                print(f'Total prize money is not divisible by 5: {total_prize_money} for race {row.date} at {row.track} raceNumber {row.raceNumber}')
            else:
                print(f'Total prize money is not divisible by 5: {total_prize_money}')
            try:
                send_telegram_message(f'Total prize money is not divisible by 5: {total_prize_money} for race {row.date} at {row.track} raceNumber {row.raceNumber}')
            except:
                send_telegram_message(f'Total prize money is not divisible by 5: {total_prize_money}')
            return (None, None, None, None)
        

        # Create DataFrame
        mapping_df = pd.DataFrame(self.prize_money_data)
        total_prize_money = int(total_prize_money)
        if total_prize_money in mapping_df['prize_money_sum'].values:
            # return as a tuple
            return tuple(mapping_df[mapping_df['prize_money_sum'] == total_prize_money][['first', 'second', 'third', 'fourth']].values[0])
        else:
            try:
                send_telegram_message(f'No prize money data for {total_prize_money} on {row.date} at {row.track} raceNumber {row.raceNumber}')
            except:
                send_telegram_message(f'No prize money data for {total_prize_money}.....')

        def complicated_round(x):
            if x < 100:
                return np.round(x / 25) * 25
            elif x < 4000:
                return np.round(x / 50) * 50
            elif x < 10000:
                return np.round(x / 100) * 100
            elif x < 600000:
                return np.round(x / 500) * 500
            else:
                return np.round(x / 1000000) * 1000000
        print(f'Figuring out prize money distribution for: {total_prize_money}: {row.date} at {row.track} raceNumber {row.raceNumber}')
        first_guess = complicated_round((2 / 3) * total_prize_money)
        
        if total_prize_money > 500000:
            first_guess_error_range = 0.1
            range_steps = 10000
        elif total_prize_money > 100000:
            first_guess_error_range = 0.04
            range_steps = 500
        else:
            first_guess_error_range = 0.04
            range_steps = 10
        range_start = int(round((0.66 - first_guess_error_range) * total_prize_money, -2))
        range_end = int(round((0.66 + first_guess_error_range) * total_prize_money, -2))

        valid_first_guesses = [x for x in range(range_start, range_end, range_steps)]        
        print(f'Got {len(valid_first_guesses)} valid first guesses from {range_start} to {range_end}')


        # FIX FIRST now iterate over the rest to get a good distribution
        complete = False
        guesses = 0
        combos_searches = 0
        while not complete:
            all_valid_combinations = []
            for first_guess in valid_first_guesses:
                ### CAN ADD LOGIC
                # if total prizemoney ends with a 5, then its going to be the fourth guess that is 75
                # Make first guess a better guess

                if guesses > 1000:
                    break
                guesses += 1
                total_remaining_money = total_prize_money - first_guess
                if total_prize_money > 500000:
                    fourth_guess = round(total_remaining_money * 0.01 / 10000) * 10000
                    max_percent = 0.1
                elif total_prize_money > 100000:
                    fourth_guess = round(total_remaining_money * 0.01 / 500) * 500
                    max_percent = 0.04
                else:
                    fourth_guess = 25
                    max_percent = 0.04

                while fourth_guess < max_percent*total_prize_money and fourth_guess < 40:
                    
                    if total_prize_money > 500000:
                        fourth_guess += 10000
                    else:
                        fourth_guess += 25  
                    margin_of_error = 0.02
                    rounding_factor = -1 if total_prize_money < 100000 else -3
                    range_steps = 5
                    if total_prize_money > 500000:
                        rounding_factor = -4
                        range_steps = 10000

                    # ALL VALID COMBINATIONS BETWEEN 9% AND 11% OF THE TOTAL PRIZE MONEY
                    range_start = int(round((0.2 - margin_of_error) * total_prize_money, rounding_factor))
                    range_end = int(round((0.2 + margin_of_error) * total_prize_money, rounding_factor))

                    valid_second_guesses = [x for x in range(range_start, range_end, range_steps)]
                    # print(f'Building combos for fourth guess: {fourth_guess} and total prize money: {total_prize_money}, got {len(valid_second_guesses)} valid second guesses from {range_start} to {range_end}')
                    
                    range_start = int(round((0.1 - margin_of_error) * total_prize_money, rounding_factor))
                    range_end = int(round((0.1 + margin_of_error) * total_prize_money, rounding_factor))
                    valid_third_guesses = [x for x in range(range_start, range_end, range_steps)]
                    # print(f'Building combos for fourth guess: {fourth_guess} and total prize money: {total_prize_money}, got {len(valid_third_guesses)} valid third guesses from {range_start} to {range_end}')
                    
                    # Check for any valid combinations
                    for second_guess in valid_second_guesses:
                        for third_guess in valid_third_guesses:
                            if first_guess + second_guess + third_guess + fourth_guess == total_prize_money:
                                if second_guess / third_guess > 1.97 and second_guess / third_guess < 2.03:
                                    all_valid_combinations.append((first_guess, second_guess, third_guess, fourth_guess))
                    combos_searches += len(valid_second_guesses)*len(valid_third_guesses)
                
            if len(all_valid_combinations) > 0:
                solutions = []
                # we have many solutions, so we need to find the one that is closest to the target based on the %'s of the total prize money for second and third
                for first_guess, second_guess, third_guess, fourth_guess in all_valid_combinations:
                    second_percent = abs(second_guess / total_prize_money)
                    third_percent = abs(third_guess / total_prize_money)
                    total_percent = second_percent + third_percent
                    solutions.append((total_percent, first_guess, second_guess, third_guess, fourth_guess))
                solutions.sort(key=lambda x: x[0])
                first_guess, second_guess, third_guess, fourth_guess = solutions[0][1:]
                complete = True
                break
        print(f'Prize money: {total_prize_money}, combos_searched: {combos_searches}, num_solutions: {len(all_valid_combinations)}')                
        print(f'First guess: {first_guess}, Second guess: {second_guess}, Third guess: {third_guess}, Fourth guess: {fourth_guess}')
        # prizes.append([first_guess, second_guess, third_guess, fourth_guess])
        return (first_guess, second_guess, third_guess, fourth_guess)


class ManualDataExtraction:
    def __init__(self) -> None:
        self.all_meets = ['albion park', 'angle park', 'armidale', 'ballarat', 'bathurst', 
                            'bendigo', 'broken hill', 'bulli', 'bundaberg', 'cairns', 'cannington', 
                            'capalaba', 'casino', 'coonabarabran', 'coonamble', 'cowra',
                            'cranbourne', 'dapto', 'darwin', 'devonport', 'dubbo', 'gawler', 
                            'geelong', 'gosford', 'goulburn', 'grafton', 'gunnedah', 'healesville', 
                            'hobart', 'horsham', 'ipswich', 'kempsey', 'launceston', 'lismore', 
                            'lithgow', 'maitland', 'mandurah', 'moree', 'mount gambier', 'mudgee', 
                            'murray bridge', 'muswellbrook', 'northam', 'nowra', 'port augusta', 
                            'potts park', 'richmond', 'rockhampton', 'sale', 'sandown park', 
                            'shepparton', 'strathalbyn', 'tamworth', 'taree',
                            'temora', 'the gardens', 'gardens', 'the meadows', 'townsville', 'traralgon',
                            'wagga', 'warragul', 'warrnambool', 'wauchope', 'wentworth park', 'young']
        self.month_names = ['january', 'february', 'march', 'april', 'may', 'june', 
                            'july', 'august', 'september', 'october', 'november', 'december']
        self.day_names = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        self.month_corruptions = self._generate_all_splits(self.month_names)
        self.day_corrputions = self._generate_all_splits(self.day_names)
        
    
    def _generate_all_splits(self, list_strings):
        # Helper method to generate all potential corrupted forms of month names
        corruptions = {}
        for single_string in list_strings:
            splits = []
            for i in range(1, len(single_string)):
                split_version = single_string[:i] + ' ' + single_string[i:]
                splits.append(split_version)
            corruptions[single_string] = splits
        return corruptions
    
    def combine_numbers_in_string(self, text):
        # This regex pattern finds groups of numbers separated by spaces
        pattern = r'(\b\d+)\s+(\d+\b)'
        
        # Function to replace each match
        def replacer(match):
            # Combine the groups into one, removing spaces
            return ''.join(match.groups())
        
        # Use re.sub() to replace the pattern in the text
        # This loops until no more replacements can be made
        while re.search(pattern, text):
            text = re.sub(pattern, replacer, text)
        
        return text

    def clean_and_parse_date(self, input_string):
        if input_string.strip() == 'thursday 16th 2018':
            input_string = 'thursday 16th august 2018'
        if input_string.strip() == 'saturday 9th may 29th 2020':
            input_string = 'saturday 9th may 2020'
        if input_string.strip() == 'thursday, 24 goulburn 2019':
            input_string = 'thursday, 24th january 2019'
        if input_string.strip() == 'monday 13 february 223':
            input_string = 'monday 13th february 2023'
        if input_string.strip() == 'january 6th december, 2019':
            input_string = '6th january 2019'
        if input_string.strip() == 'sunday, 31st sunday 2020':
            input_string = 'sunday, 31st may 2020'
        if input_string.strip() == 'wednesday, 21staugust 2019':
            input_string = 'wednesday, 21st august 2019'
        if input_string.strip() == 'thursday 25h july 2019':
            input_string = 'thursday 25th july 2019'


        # For each month and its potential corruptions, replace in the original string
        input_string = input_string.replace('wednesda monday', 'monday').replace('th', ' ')
        input_string = self.combine_numbers_in_string(input_string)

        try:
            date = pd.to_datetime(input_string).strftime('%A, %d %B %Y')
            return date
        except:
            pass

        for month, corruptions in self.month_corruptions.items():
            for corruption in corruptions:
                input_string = re.sub(re.escape(corruption), month, input_string, flags=re.IGNORECASE)
        
        # Try to parse the date after all corrections'
        try:
            # print(input_string)
            parsed_date = parse(input_string, fuzzy=True)
            # print(parsed_date)
            # print(parsed_date.strftime('%A, %d %B %Y'))
            return parsed_date.strftime('%A, %d %B %Y')  # formatted output
        except ValueError:
            return None
        
    # Breaks up words with multiple capital letters, will help identifying dogs and feature identification
    def clean_text(self, text):
        patterns = [
            r'(\b[A-Z][a-z]+)([A-Z][a-z]+)',  # Split words where two capitalized words are conjoined
        ]
        for pattern in patterns:
            text = re.sub(pattern, r'\1 \2', text)

        # specific fixes
        text = text.replace('. 19', '.19').replace('20203', '2023').replace('20023', '2023').replace('14th gosford 2019', '14th feb 2019')
        text = text.replace("’", "'").replace(" '", "'").replace(" ,", ",").replace('  ', ' ').replace("'", '').replace('’', '')

        return text.lower()

    def process_string(self, input_string):
        # Define the regex to match exactly four digits interrupted by a space within a span of five characters
        pattern = r'(?<!\d)(\d{1,3})\s(\d{1,3})(?!\d)'
        
        # Replace the pattern in the string
        # print(input_string)
        processed_string = re.sub(pattern, lambda m: m.group(1) + m.group(2) if len(m.group(1) + m.group(2)) == 4 else m.group(0), input_string)
        processed_string = self.clean_and_parse_date(processed_string)
        # print(processed_string)
        return processed_string

    def find_date(self, text, state='nsw', meet=None, last_date=None, report_path=None, df=None):
        meet_list = [meet.lower() for meet in meet]
        if '2021_10_12_Bulli_stew_report' in report_path:
            return pd.to_datetime('2021-10-12')
        if '2019_02_16_Bulli_stew_report' in report_path:
            return pd.to_datetime('2019-02-16')
        if '2021_11_29_Maitland_stew_report' in report_path:
            return pd.to_datetime('2021-11-29')
        if '2023_04_09_Richmond_stew_report' in report_path:
            return pd.to_datetime('2023-04-09')
        if '2023_02_15_Richmond_stew_report' in report_path:
            return pd.to_datetime('2023-02-15')
        if '2023_01_06_Richmond_stew_report' in report_path:
            return pd.to_datetime('2023-01-06')
        if '2023_01_04_Richmond_stew_report' in report_path:
            return pd.to_datetime('2023-01-04')
        if '2021_02_05_Richmond_stew_report' in report_path:
            return pd.to_datetime('2021-02-05')
        if 'GAW13SEPT2023' in report_path:
            return pd.to_datetime('2023-09-13')
        if 'MBR24AUG2022' in report_path:
            return pd.to_datetime('2022-08-24')
        if '20230219_capa' in report_path:
            return pd.to_datetime('2023-02-19')
        if '20220112_albi' in report_path:
            return pd.to_datetime('2022-01-12')
        if '20211215_albi' in report_path:
            return pd.to_datetime('2021-12-15')
        if '20230604_capa' in report_path:
            return pd.to_datetime('2023-06-04')
        if '20211031_capa' in report_path:
            return pd.to_datetime('2021-10-31')
        if '20200925_capa' in report_path:
            return pd.to_datetime('2020-09-25')
        if '20230524_rock' in report_path:
            return pd.to_datetime('2023-05-24')
        if '20200605_town' in report_path:
            return pd.to_datetime('2020-06-05')
        if '20200517_capa' in report_path:
            return pd.to_datetime('2020-05-17')
        if '20190714_capa' in report_path:  
            return pd.to_datetime('2019-07-14')
        if '20190714_albi' in report_path:
            return pd.to_datetime('2019-07-14')
        if '20190516_albi' in report_path:
            return pd.to_datetime('2019-05-16')
        if '20190504_ipsw' in report_path:
            return pd.to_datetime('2019-05-04')
        if '20190625_ipsw' in report_path:
            return pd.to_datetime('2019-06-25')
        if '20191117_albi' in report_path:
            return pd.to_datetime('2019-11-17')
        if '2018_08_02_Dapto_stew_report' in report_path:
            return pd.to_datetime('2018-08-02')
        if '2018_08_10_Casino_stew_report' in report_path:
            return pd.to_datetime('2018-08-10')
        if '2018_08_13_Nowra_stew_report' in report_path:
            return pd.to_datetime('2018-08-13')
        if '2023_05_06_Lithgow_stew_report' in report_path:
            return pd.to_datetime('2023-05-06')
        if '2020_02_07_Casino_stew_report' in report_path:
            return pd.to_datetime('2020-02-07')
        if '2023_11_18_Potts Park_stew_report' in report_path:
            return pd.to_datetime('2023-11-25')
        if '2019_02_14_Gosford_stew_report' in report_path:
            return pd.to_datetime('2019-02-14')
        if '2019_11_17_Gunnedah_stew_report' in report_path:
            return pd.to_datetime('2019-11-17')
        if '20190331_capa' in report_path:
            return pd.to_datetime('2019-03-31')
        if '20190609_albi' in report_path:
            return pd.to_datetime('2019-06-09')

        if '20210416_ipsw' in report_path:
            return pd.to_datetime('2021-04-16')
        if '263377' in report_path:
            return pd.to_datetime('2021-10-08')
        if '248558' in report_path:
            return pd.to_datetime('2018-10-14')
        if '247871' in report_path:
            return pd.to_datetime('2018-08-02')
        if 'APK06OCT2023' in report_path:
            return pd.to_datetime('2023-10-06')
        if '247629' in report_path:
            return pd.to_datetime('11th July 2018')
        elif '247779' in report_path:
            return pd.to_datetime('25th July 2018')
        elif '245140' in report_path:
            return pd.to_datetime('11th June 2018')
        elif '248376' in report_path:
            return pd.to_datetime('24th September 2018')
        elif '248143' in report_path:
            return pd.to_datetime('30th August 2018')
        elif '247690' in report_path:
            return pd.to_datetime('16th July 2018')
        elif '245092' in report_path:
            return pd.to_datetime('7th June 2018')
        elif '248124' in report_path:
            return pd.to_datetime('29th August 2018')
        elif '247902' in report_path:
            return pd.to_datetime('6th August 2018')
        elif '248113' in report_path:
            return pd.to_datetime('28th August 2018')
        elif '246606' in report_path:
            return pd.to_datetime('1st July 2018')
        elif '247733' in report_path:
            return pd.to_datetime('19th July 2018')
        elif '247569' in report_path:
            return pd.to_datetime('3rd July 2018')
        elif '245373' in report_path:
            return pd.to_datetime('27th June 2018')
        elif '247820' in report_path:
            return pd.to_datetime('30th July 2018')
        elif '248279' in report_path:
            return pd.to_datetime('13th September 2018')
        elif '247603' in report_path:
            return pd.to_datetime('5th July 2018')
        elif '248262' in report_path:
            return pd.to_datetime('12th September 2018')
        elif '248004' in report_path:
            return pd.to_datetime('16th August 2018')
        elif '247874' in report_path:
            return pd.to_datetime('2nd August 2018')
        elif '248210' in report_path:
            return pd.to_datetime('6th September 2018')
        elif '248342' in report_path:
            return pd.to_datetime('20th September 2018')
        elif '248323' in report_path:
            return pd.to_datetime('19th September 2018')
        elif '247621' in report_path:
            return pd.to_datetime('9th July 2018')
        elif '247943' in report_path:
            return pd.to_datetime('9th August 2018')
        elif '247800' in report_path:
            return pd.to_datetime('26th July 2018')
        elif '247973' in report_path:
            return pd.to_datetime('13th August 2018')
        elif '245040' in report_path:
            return pd.to_datetime('4th June 2018')
        elif '247661' in report_path:
            return pd.to_datetime('12th July 2018')
        elif '245243' in report_path:
            return pd.to_datetime('18th June 2018')
        elif '245176' in report_path:
            return pd.to_datetime('13th June 2018')
        elif 'ADELAIDE CUP' in report_path:
            return pd.to_datetime('7th October 2022')
        elif '247714' in report_path:
            return pd.to_datetime('18th July 2018')
        elif '248197' in report_path:
            return pd.to_datetime('5th September 2018')
        elif '248033' in report_path:
            return pd.to_datetime('20th August 2018')
        elif '247567' in report_path:
            return pd.to_datetime('2nd July 2018')
        elif '245270' in report_path:
            return pd.to_datetime('20th June 2018')
        elif '248236' in report_path:
            return pd.to_datetime('10th September 2018')
        elif '248103' in report_path:
            return pd.to_datetime('27th August 2018')
        elif '245344' in report_path:
            return pd.to_datetime('25th June 2018')
        elif '247854' in report_path:
            return pd.to_datetime('1st August 2018')
        
        new_date = report_path.split('/')[1].split('_', 1)[1].split('_', 1)[1][:10].replace('_', '/')
        return pd.to_datetime(new_date)
        if state == 'nsw':
            # new_date = report_path.split('/')[1][:10].replace('_', '/') # OLD FORMAT
            new_date = report_path.split('/')[1].split('_', 1)[1].split('_', 1)[1][:10].replace('_', '/')
            return pd.to_datetime(new_date)
            pattern = r'(?<=date)\s*(.*?)\s*\n'  # Match "Date:" preceded by any characters until the next newline
            match = re.search(pattern, text.lower())
            if match:
                match = match.group(1).replace(':', '').strip().lower()
                if 'weather' in match:
                    match = match.split('weather')[0].strip()
               
                new_date = pd.to_datetime(parse(self.process_string(match), fuzzy=True))
                if df[(df['track'].isin(meet_list)) & (df['date'] == pd.to_datetime(new_date))].shape[0] > 0:
                    return new_date
                try:
                    # check absolute difference between last date and new date
                    if last_date:
                        if (new_date - last_date).days < -5 or (new_date - last_date).days > 5:
                            # print(f'New date {new_date} is too far from last date {last_date}')
                            # try swapping the day and month
                            swapped_date = pd.to_datetime(f'{new_date.day}/{new_date.month}/{new_date.year}')
                            # print(f'Swapped date from {new_date} to {swapped_date}')

                            # Check to see if there was racing day or day after
                            if df[(df['track'].isin(meet_list)) & (df['date'] == pd.to_datetime(last_date))].shape[0] > 0:
                                return last_date
                            elif df[(df['track'].isin(meet_list)) & (df['date'] == pd.to_datetime(last_date) + pd.Timedelta(days=1))].shape[0] > 0:
                                return last_date + pd.Timedelta(days=1)
                                
                            return swapped_date
                    return new_date
                            
                except:
                    # print(f'Some error date {match.group(1).replace(":", "").strip().lower()}')
                    pass
                
            # Trying to find the right date based on the last date used or plus one day
            if df[(df['track'].isin(meet_list)) & (df['date'] == pd.to_datetime(last_date))].shape[0] > 0:
                return last_date
            elif df[(df['track'].isin(meet_list)) & (df['date'] == pd.to_datetime(last_date) + pd.Timedelta(days=1))].shape[0] > 0:
                return last_date + pd.Timedelta(days=1)
            else:
                try:
                    split_text = text.split('stewards report', 1)[1].strip().split('\n')
                    date = pd.to_datetime(parse(self.process_string(split_text[0]), fuzzy=True))
                    return date
                except Exception as e:
                    print(f'Error date: {e}')
                    return None
                
        if state == 'vic':
            split_text = text.split('\n')
            date = pd.to_datetime(parse(self.process_string(split_text[2].strip()), fuzzy=True))
            # swapped_date = pd.to_datetime(f'{date.day}/{date.month}/{date.year}')
            # print(f'Date: {date}, Swapped date: {swapped_date}')
            if df[(df['track'].isin(meet_list)) & (df['date'] == pd.to_datetime(date))].shape[0] > 0:
                return date
            elif df[(df['track'].isin(meet_list)) & (df['date'] == pd.to_datetime(date) + pd.Timedelta(days=1))].shape[0] > 0:
                return date + pd.Timedelta(days=1)
            elif df[(df['track'].isin(meet_list)) & (df['date'] == pd.to_datetime(date) - pd.Timedelta(days=1))].shape[0] > 0:
                return date - pd.Timedelta(days=1)
            else:
                return date
            
        if state == 'qld':
            text = text.replace('febuary', 'february')
            pattern = r'(?<=\bdate:)\s*(.*?)\s*\n'  # Match "Date:" preceded by any characters until the next newline     
            match = re.search(pattern, text.lower())

            if match and match.group(1).strip() != 'after a':
                new_date = pd.to_datetime(parse(self.process_string(match.group(1).replace(':', '').strip().lower())))
                if df[(df['track'].isin(meet_list)) & (df['date'] == pd.to_datetime(new_date))].shape[0] > 0:
                    return new_date
                try:
                    # check absolute difference between last date and new date
                    if last_date:
                        if (new_date - last_date).days < -5 or (new_date - last_date).days > 5:
                            # print(f'New date {new_date} is too far from last date {last_date}')
                            # try swapping the day and month
                            swapped_date = pd.to_datetime(f'{new_date.day}/{new_date.month}/{new_date.year}')
                            # print(f'Swapped date from {new_date} to {swapped_date}')

                            # Check to see if there was racing day or day after
                            if df[(df['track'].isin(meet_list)) & (df['date'] == pd.to_datetime(last_date))].shape[0] > 0:
                                return last_date
                            elif df[(df['track'].isin(meet_list)) & (df['date'] == pd.to_datetime(last_date) + pd.Timedelta(days=1))].shape[0] > 0:
                                return last_date + pd.Timedelta(days=1)
                                
                            return swapped_date
                    return new_date
                            
                except:
                    print(f'Some error date {match.group(1).replace(":", "").strip().lower()}')
                    pass
            else:
                text = text.lower().replace("'", '')
                if 'stewards report' in text:
                    text = text.split('stewards report', 1)[1].strip()

                split_text = text.split('\n')
                # remove blanks
                split_text = [x.strip() for x in split_text if x.strip()]
                if split_text[1] == 'thursday, 13th decemeber 2018':
                    return pd.to_datetime('2018-12-13')
                
                # print(f'Split text: {split_text[1]}')
                # print(f'{split_text[2]}')
                if 'raceway' in split_text[1] or 'showgrounds' in split_text[1]:
                    return pd.to_datetime(parse(self.process_string(split_text[2]), fuzzy=True))
                return pd.to_datetime(parse(self.process_string(split_text[1]), fuzzy=True))

        if state == 'sa':
            text = text.lower().replace("'", '')
            if 'stewards report ' in text:
                text = text.split('stewards report ', 1)[1].strip()
            
            split_text = text.split('\n')

            # remove blank lines etc
            split_text = [x.strip() for x in split_text if len(x.strip()) > 1]
            # print(f'Split text: {split_text[1]}, {split_text[2]}')
            
            try:
                raw_date = split_text[1]
                if ',' in raw_date:
                    raw_date = raw_date.split(',')[1].strip()
                date = pd.to_datetime(parse(self.process_string(raw_date), fuzzy=True))
            except:
                # print(f'Split text: {split_text[0]}, {split_text[1]}, {split_text[2]}')
                date = pd.to_datetime(parse(self.process_string(split_text[2]), fuzzy=True))

            # print(f'Date: {date}')
            if meet:
                if df[(df['track'].isin(meet_list)) & (df['date'] == pd.to_datetime(date))].shape[0] > 0:
                    return date
                elif df[(df['track'].isin(meet_list)) & (df['date'] == pd.to_datetime(date) + pd.Timedelta(days=1))].shape[0] > 0:
                    return date + pd.Timedelta(days=1)
                elif df[(df['track'].isin(meet_list)) & (df['date'] == pd.to_datetime(date) - pd.Timedelta(days=1))].shape[0] > 0:
                    return date - pd.Timedelta(days=1)

            return date
        if state == 'wa':
            text = text.lower().replace("'", '')
            try:
                new_text = text.split('meeting held at')[1]
                # print(new_text[:50])
                date = new_text.split('on', 1)[1]
                date = date.split('\n', 1)[0].strip()
                date = pd.to_datetime(parse(date, fuzzy=True))
                return date
            except:
                return None
        if state == 'nt':
            text = text.lower().replace("'", '')
            try:
                date = text.split('park on', 1)[1]
                date = date.split('\n', 1)[0].strip()
                date = pd.to_datetime(parse(date, fuzzy=True))
                return date
            except:
                return None  

    def find_meet_name(self, text, state='nsw', race_names=None, pdf_path=None):
        text = text.replace('meetin g', 'meeting')

        if '2019_02_18_Grafton_stew_report' in pdf_path or '2019_02_04_Grafton_stew_report' in pdf_path \
            or '2019_03_04_Grafton_stew_report' in pdf_path or '2019_02_25_Grafton_stew_report' in pdf_path:
            return ['grafton']
        if '2019_02_16_Bulli_stew_report' in pdf_path or '2018_09_15_Bulli_stew_report' in pdf_path:
            return ['bulli']
        if '20210416_ipsw' in pdf_path or '20231201_ipsw' in pdf_path:
            return ['ipswich']
        if '2021_03_05_Richmond_stew_report' in pdf_path: # or '2021_12_10_Richmond_stew_report' in pdf_path:
            return ['richmond']
        if '20190707_capa' in pdf_path:
            return ['capalaba']
        if '20191117_albi' in pdf_path or '20191125_albi' in pdf_path or '20190609_albi' in pdf_path:
            return ['albion park']
        if '2019_04_16_Lismore_stew_report' in pdf_path:
            return ['lismore']
        if '2019_02_15_Casino_stew_report' in pdf_path or '2021_06_10_Casino_stew_report' in pdf_path:
            return ['casino']
        if '2021_10_06_Wentworth Park_stew_report' in pdf_path:
            return ['wentworth park']
        if '2020_07_13_Bathurst_stew_report' in pdf_path:
            return ['bathurst']
        if '2019_03_20_Richmond_stew_report' in pdf_path or '2019_03_27_Richmond_stew_report' in pdf_path \
            or '2019_03_22_Richmond_stew_report' in pdf_path:
            return ['richmond']
        if '2019_11_17_Gunnedah_stew_report' in pdf_path:
            return ['gunnedah']
        if '20190331_capa' in pdf_path:
            return ['capalaba']
        

        if any(id in pdf_path for id in [
            '247629', '247779', '245140', '248376', '248143',
            '247690', '245092', '248124', '247902', '248113',
            '246606', '247733', '247854', '245373', '247820',
            '248279', '247603', '248262', '248004', '247874',
            '248210', '248342', '248323', '247621', '247943',
            '247800', '247973', '245040', '247661', '245243',
            '245176', 'ADELAIDE CUP', '247714', '248197', '248033',
            '247567', '245270', '248236', '248103', '245344',]):
            return ['angle park']
        if '247569' in pdf_path:
            return ['gawler']

        
        if state == 'nt':
            return ['darwin']
            
        if state == 'nsw':
            # if 'Track' in text:
            #     pattern = r'(?<=Track:)\s*(.*?)\s*\n'
            # else:
            pattern = r'(?<=meeting:)\s*(.*?)\s*\n'  # Match "Meeting:" preceded by any characters until the next newline
            match = re.search(pattern, text.lower().replace(' :', ':'))
            if match:
                if match.group(1).lower().strip() in ['night', 'twilight', 'day']:
                    pattern = r'(?<=track:)\s*(.*?)\s*\n'  # Match "Track:" preceded by any characters until the next newline
                    match = re.search(pattern, text.replace(' :', ':').lower())
                    if match:
                        return [match.group(1)]
                    else:
                        for race_name in race_names:
                            if race_name.lower() in text.lower()[:150]:
                                return [race_name]
                        return None
                return [match.group(1)]
            else:
                for race_name in race_names:
                    if race_name.lower() in text.lower()[:150]:
                        return [race_name]
                return None
        if state == 'vic':
            split_text = text.split('\n')
            return [split_text[1].strip()]
        
        if state == 'qld':
            text = text.lower().replace("'", '').replace('greyhound racing club', '').replace('racing club', '').replace('greyhounds', '').replace('greyhound', '').replace('monday', '').replace('queensland', '').strip()
            if 'stewards report' in text:
                text = text.split('stewards report', 1)[1].strip()
            meet_text = text.lower()
            return [meet_text.split('\n')[0]]
        
        if state == 'sa':
            text = text.lower().replace("'", '').replace('greyhound', '').replace(" ", "")
            for meet_name in race_names:
                if meet_name.replace(" ", "").lower() in text[:150].lower().strip():
                    return [meet_name]

        if state == 'wa':
            text = text.lower().replace("'", '')
            try:
                new_text = text.split('meeting held at')[1]
                # print(new_text[:50])
                meet = new_text.split('on', 1)[0]
                return [meet.strip()]
            except:
                return None
            
        return None
        
    def extract_text_from_pdf(self, pdf_path, clean=True):

        with open(pdf_path, 'rb') as file:
            text = ''
            pdf_reader = PyPDF2.PdfReader(file)
            num_pages = len(pdf_reader.pages)
            # print(f'Number of pages: {num_pages}')
            for page_num in range(num_pages):
                page = pdf_reader.pages[page_num]
                text += page.extract_text()

        if 'a' not in text or '4_2020_12_10-december-stewards-report' in pdf_path:
            print(f'PDF Path: {pdf_path} need to use OCR')
            images = pdf2image.convert_from_path(pdf_path, dpi=200, poppler_path=r'/opt/homebrew/bin')
            # Extract text using OCR
            text = ''
            for image in images:
                original_text = pytesseract.image_to_string(image)
                text += original_text + '\n'
        if clean:
            return self.clean_text(text)
        return text

    def get_clean_meet_name(self, text, state, race_names=None, pdf_path=None):
        meet_names = self.find_meet_name(text=text, state=state, race_names=race_names, pdf_path=pdf_path)
        master_meets = []
        if not meet_names:
            return None
        
        for meet_name in meet_names:
            updated_meet_name = meet_name.replace('–', '-').replace('—', '-').replace('morning', '').replace('twilight', '').replace('night', '').replace('day', '').replace('matinee', '').strip()  

            # CLEAN OUT UNWANTED TEXT
            
            if ' race ' in updated_meet_name.lower():
                updated_meet_name = updated_meet_name.lower().split('race')[0].strip() 
            if '(' in updated_meet_name.lower():
                updated_meet_name = updated_meet_name.lower().split('(')[0].strip()
            if '-' in updated_meet_name.lower():
                updated_meet_name = updated_meet_name.lower().split('-')[0].strip()
            if '\t' in updated_meet_name.lower():
                updated_meet_name = updated_meet_name.lower().split('\t')[0].strip()
            if 'finish' in updated_meet_name.lower():
                updated_meet_name = updated_meet_name.lower().split('finish')[0].strip()
            if 'weather' in updated_meet_name.lower():
                updated_meet_name = updated_meet_name.lower().split('weather')[0].strip()
            if 'track' in updated_meet_name.lower():
                updated_meet_name = updated_meet_name.lower().split('track')[0].strip()
            if updated_meet_name.lower().endswith(' c'):
                updated_meet_name = updated_meet_name[:-2]

            # NAME REPLACEMENT
            if updated_meet_name.lower() == 'townville':
                updated_meet_name = 'townsville'
            if updated_meet_name.lower() == 'wentworth':
                updated_meet_name = 'wentworth park'
            if updated_meet_name.lower() == 'ladbrokes gardens' or updated_meet_name.lower() == 'gardens' or \
                updated_meet_name.lower() == 'gardens c' or updated_meet_name.lower() == 'the gardens c':
                updated_meet_name = 'the gardens'
            if 'gardens' in updated_meet_name.lower().strip():
                updated_meet_name = 'the gardens'
            if updated_meet_name.lower().strip() == 'richmond straight':
                updated_meet_name = 'richmond'
            if updated_meet_name.lower().replace(' ', '') == 'waggawagga':
                updated_meet_name = 'wagga'
            if updated_meet_name.lower() == 'canberra @ goulburn' or updated_meet_name.lower() == 'canberra at goulburn' or updated_meet_name.lower() == 'gooulburn':
                updated_meet_name = 'goulburn'
            if updated_meet_name.lower() == 'pt augusta':
                updated_meet_name = 'port augusta'
            if updated_meet_name.lower() == 'wagga wagga':
                updated_meet_name = 'wagga'
            
            if updated_meet_name.lower() == 'meadows (mep)' or updated_meet_name.lower() == 'meadows':
                updated_meet_name = 'the meadows'
            if updated_meet_name.lower() == 'sandown (sap)' or updated_meet_name.lower() == 'sandown':
                updated_meet_name = 'sandown park'
            if 'richmond' in updated_meet_name.lower():
                updated_meet_name = 'richmond'   
                
            if 'bundaberg' in updated_meet_name.lower():
                updated_meet_name = 'bundaberg'

            if updated_meet_name.lower().replace(' ', '') == 'mountgambier' or updated_meet_name.lower().replace(' ', '') == 'mtgambier':
                updated_meet_name = 'mount gambier'
            if updated_meet_name.lower().replace(' ', '') == 'canningt':
                updated_meet_name = 'cannington'

            
            
            if 'brisbane' in updated_meet_name.lower() or updated_meet_name.lower() == 'albion':
                master_meets.append('albion park')
                master_meets.append('ipswich')
                    
            else:
                for meet in self.all_meets:
                    if meet.lower().replace(' ', '') == updated_meet_name.lower().replace(' ', ''):
                        updated_meet_name = meet
                updated_meet_name = updated_meet_name.lower().replace("'", '').replace("-", '')
                master_meets.append(updated_meet_name.strip())
        return master_meets

if __name__ == '__main__':
    static_functions = StaticFunctions()
    static_functions.run()