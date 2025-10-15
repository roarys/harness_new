import pandas as pd
from static.telegram import send_telegram_message


def clean_error_dogs(df):
    df['dogId'] = df['dogId'].apply(lambda x: str(x))
    return df

def clean_error_races(df):
    df_merged = df.copy()    
    df_merged['raceCode'] = df_merged['raceCode'].apply(lambda x: str(x))
    df_merged['track'] = df_merged['track'].apply(lambda x: x.lower())
    
    # Some races have no run time - abandoned etc so dropping them off.
    non_run_races = df_merged.groupby(['raceCode', 'date']).filter(lambda x: (x.resultTime.sum() < 1))
    print(non_run_races['raceCode'].unique())
    if len(non_run_races['raceCode'].unique()) > 0:
        print(f'Found {len(non_run_races["raceCode"].unique())} races that werent run, based off no runtimes so dropping them off the dataset')
        print(non_run_races[['date', 'track', 'raceNumber', 'horseOverallTime']])
        non_run_races.to_csv('non_run_races.csv', index=False)
        
    df_merged = df_merged[~df_merged['raceCode'].isin(non_run_races)]

    return df_merged

def clean_datafields_formats(df):
    df['raceCode'] = df['raceCode'].astype(str)
    df['horseId'] = df['horseId'].astype(str)
    
    # df['boxNumber'] = pd.to_numeric(df['boxNumber'], errors='coerce').fillna(0).astype(int)
    df['plannedStartTimestamp'] = pd.to_datetime(df['plannedStartTimestamp'], format='mixed')
    df['distance'] = df['distance'].apply(lambda x: int(str(x).replace('m', '')))
    df['horseName'] = df['horseName'].apply(lambda x: str(x).lower().replace("'", "").replace(".", "").replace("?", "").replace("  ", " ").strip())
    df['resultMargin'] = pd.to_numeric(df['resultMargin']).fillna(0).astype(float)
    df['prizeMoney'] = pd.to_numeric(df['prizeMoney']).fillna(0).astype(float)
    
    for col in ['startingPriceTote']:
        df[col].fillna(0, inplace=True)
        df[col] = df[col].apply(
            lambda x: pd.to_numeric(str(x).replace('$', '').replace('F', '')))
        df[col] = df[col].apply(lambda x: 0 if x <= 1 else x)


    if df[df['track'].isnull()].shape[0] > 0:
        print('Found some tracks that are null')
        print(df[df['track'].isnull()])
        send_telegram_message(f'Found some tracks that are null... {df[df["track"].isnull()].shape}')
        df[df['track'].isnull()].to_csv('tracks_null.csv', index=False)

    return df


def trainer_cleaning(df):
    multiple_trainernames = pd.DataFrame(df.groupby('trainerId').trainerName.unique().apply(lambda x: len(x)).sort_values(ascending=False))
    multiple_trainernames = multiple_trainernames[multiple_trainernames['trainerName'] > 1]
    multiple_trainernames['trainerId'] = multiple_trainernames.index
    multiple_trainernames.reset_index(drop=True, inplace=True)

    for id_x in multiple_trainernames['trainerId'].unique():
        df_trainer = df[df['trainerId'] == id_x].sort_values(by='date')
        if len(df_trainer['trainerName'].unique()) > 1:
            counts = {}
            max_min_dates = {}
            dog_trainers = {}
            for tname in df_trainer['trainerName'].unique():
                if tname in ['R Vines', 'R Vines Jr', 'R Vines Jnr',
                            'T Kennedy-Harris', 'T Kennedy - Harris',
                            'S McInerney', 'S Mc Inerney',
                            'R Panagiotou', 'T Panagiotou',
                            'J Mundy', 'F Mundy',
                            'S Abela', 'J Abela',
                            'J & D Bell', 'Jake & Dayze Bell',
                            'T Lagana', 'A Lagana',
                            'H Harnath Chapman', 'H Chapman',
                            'D Burnett', 'd Burnett',
                            "R O'Brien", 'R Obrien',
                            'S Harris', 'S Barton',
                            'A Courts', 'A Gibbons',
                            'E Bowles', 'E Scott',
                            'L Harborne', 'L Davis',
                            'C Bermingham', 'C Berminhgam',
                            'S Monaghan', 'S Fitzgerald',
                            'M Felton-Cleghorn', 'M Felton',
                            'T Cornell', 'T Boreland',
                            'J Warzywoda', 'J Blake',
                            'K Wilson', 'K Gauci',
                            'S Ferguson', 'S Grenfell',
                            'T Augustin', 'T Hancock',
                            'K Rowe', 'K Rowe Kara',
                            'M Vodagaz', 'M Vodogaz',
                            'J Sant', 'J Conquest',
                            'S Sinclair', 'S Deppeler',
                            'C Allan', 'C Van De Maat',
                            'L Walker', 'L Goodwin',
                            'L Polato', 'L Burke',
                            'A Ellen', 'A Fry',
                            'M Shambler', 'M Frankland-Shambler',
                            'J Hampshire', 'J Willcocks',
                            'M Mallia-Magri', 'M Mallia',
                            'K Gommans', 'K Lincoln-Papuni',
                            'J Quinlivian', 'Y Rishon', # unsure on this one but dates line up...
                            'K Gommans', 'K Lincoln-Papuni',
                            'J Forrest', 'J McGovern',
                            'D Burns', 'D Ivers',
                            'S Scott', 'S Craig',
                            'H McLachlan', 'H Male',
                            'H Oaten', 'H Cavanagh',
                            'H McLachlan', 'H Male',
                            'B Robertson -Leech', 'B Robertson-Leech',
                            'N Cachia', 'N Salna',
                            'N Stanton', 'N Weightman',
                            'M Fullerton', 'M Docking',
                            'H Moffitt', 'H Gilbert',
                            'tess simmons', 'therese simmons',
                            ]:
                    continue
                
                print(f'Found a double up {id_x} {df_trainer.trainerName}')
                send_telegram_message('Found a double up ', id_x, df_trainer.trainerName)
                # managing double ups in id's that are actually legit - change of owner or change of trainer
                counts[tname] = len(df_trainer[(df_trainer['trainerName'] == tname)])
                max_min_dates[tname] = [df_trainer[(df_trainer['trainerName'] == tname)].date.min(), df_trainer[(df_trainer['trainerName'] == tname)].date.max()]
                print(tname, len(df_trainer[(df_trainer['trainerName'] == tname)]), df_trainer[(df_trainer['trainerName'] == tname)].date.min(), df_trainer[(df_trainer['trainerName'] == tname)].date.max())
                dog_trainers[tname] = df_trainer[(df_trainer['trainerName'] == tname)].trainerId.values[0]
            
        
        if len(counts) < 2:
            continue

        trainer_names = list(counts.keys())
        trainer_last_names = [x.split(' ')[-1] for x in trainer_names]
        trainer_first_names = [x.split(' ')[0] for x in trainer_names]
        trainer_names_stripped = [x.lower().strip().replace(' ', '').replace("'", '') for x in trainer_names]

        first_section_max_date = pd.to_datetime(max_min_dates[list(max_min_dates.keys())[0]][1])
        second_section_min_date = pd.to_datetime(max_min_dates[list(max_min_dates.keys())[1]][0])

        
        if len(trainer_names) == 2:
            if trainer_names_stripped[0] == trainer_names_stripped[1]:
                print('Same trainer, dont need to do anything', trainer_names)
                continue
            if trainer_last_names[0] == trainer_last_names[1]:
                print('Same last name, can leave as is', trainer_names)
                continue
            if trainer_first_names[0] == trainer_first_names[1] and (second_section_min_date - first_section_max_date).days < 50:
                print((second_section_min_date - first_section_max_date).days)
                print('Keeping the way it is because name change', trainer_names)
                continue
        elif len(trainer_names) == 3:
            if trainer_names_stripped[0] == trainer_names_stripped[1] and trainer_names_stripped[0] == trainer_names_stripped[2]:
                print('Same trainer, dont need to do anything', trainer_names)
                continue
            if trainer_last_names[0] == trainer_last_names[1] and trainer_last_names[0] == trainer_last_names[2]:
                print('Same last name, can leave as is', trainer_names)
                continue
            if trainer_first_names[0] == trainer_first_names[1] and trainer_first_names[0] == trainer_first_names[2]:
                print('Same first name, can leave as is', trainer_names)
                continue
        
        
    return df

def bijective_race_ids(df):
    # find race_ids with multiple racenames
    multiple_racenames = pd.DataFrame(df.groupby('raceCode').raceName.unique().apply(lambda x: len(x)).sort_values(ascending=False))
    multiple_racenames = multiple_racenames[multiple_racenames['raceName'] > 1]
    multiple_racenames['raceCode'] = multiple_racenames.index
    multiple_racenames.reset_index(drop=True, inplace=True)
    if multiple_racenames.shape[0] > 0:
        for raceCode in multiple_racenames['raceCode'].unique():
            print('Found a double up ', raceCode)
            send_telegram_message(f'Found multiple racenames for {raceCode}')
    return df


def bijective_dogname_and_id(df):
    print('Checking for multiple horseId for each horseName')
    # Find horses with multiple horseId and keep the one with the most results
    multiple_ids = pd.DataFrame(df.groupby('horseName')['horseId'].unique().apply(lambda x: len(x)).sort_values(ascending=False))
    multiple_ids = multiple_ids[multiple_ids['horseId'] > 1]
    multiple_ids['horseName'] = multiple_ids.index
    multiple_ids.reset_index(drop=True, inplace=True)

    for horseName in multiple_ids.horseName.unique():
        if horseName in ['lady bella', 'mums the word', 'bold charlm', 'big boy charlm']: #known different dogs
            continue
        df_dog = df[df['horseName'] == horseName].sort_values(by='date')
        if len(df_dog['horseId'].unique()) > 1:
            print('Found a double up ', horseName)
            dogid_counts = {}
            max_min_dates = {}
            for horse_id in df_dog['horseId'].unique():
                dogid_counts[horse_id] = len(df_dog[(df_dog['horseId'] == horse_id)])
                max_min_dates[horse_id] = [df_dog[(df_dog['horseId'] == horse_id)].date.min(), df_dog[(df_dog['horseId'] == horse_id)].date.max()]
                print(horse_id, len(df_dog[(df_dog['horseId'] == horse_id)]), df_dog[(df_dog['horseId'] == horse_id)].date.min(), df_dog[(df_dog['horseId'] == horse_id)].date.max())
            
            # soemthing about max dates being siginificantly different then i can keep the way it is
            # if not then i can just keep the one with the most results
            first_section_max_date = pd.to_datetime(max_min_dates[list(max_min_dates.keys())[0]][1])
            second_section_min_date = pd.to_datetime(max_min_dates[list(max_min_dates.keys())[1]][0])

            print(first_section_max_date, second_section_min_date)
            if (second_section_min_date - first_section_max_date).days > 100:
                print((second_section_min_date - first_section_max_date).days)
                print('Keeping the way it is because different dogs')
                # time.sleep(10)
                continue
            
            max_index = max(dogid_counts, key=dogid_counts.get)
            
            print(f'Keeping {max_index}')
            df.loc[(df['horseName'] == horseName), 'horseId'] = max_index
    
    print('Checking for multiple dognames for each id_x')    
    multiple_dognames = pd.DataFrame(df.groupby('horseId').horseName.unique().apply(lambda x: len(x)).sort_values(ascending=False))
    multiple_dognames = multiple_dognames[multiple_dognames['horseName'] > 1]
    multiple_dognames['horseId'] = multiple_dognames.index
    multiple_dognames.reset_index(drop=True, inplace=True)

    for id_x in multiple_dognames['horseId'].unique():
        if str(id_x) in ['198404519', '634935361', '127028580',
                    '542053444', '357308739', '423787940',
                    '198404519', '523685291', '482776693',
                    '795350037', '314052358', '707214745',
                    '828167028', '817729204', '775588700',
                    '815513855', '857017839', '707215189',
                    '829355575', '801424682', '889125354']: #known dogs that have differnt names but same dog
            continue
        df_dog = df[df['horseId'] == id_x].sort_values(by='date')
        if len(df_dog['horseName'].unique()) > 1:
            print('Found a double up ', id_x)
            dogid_counts = {}
            max_min_dates = {}
            for horse_id in df_dog['horseName'].unique():
                dogid_counts[horse_id] = len(df_dog[(df_dog['horseName'] == horse_id)])
                max_min_dates[horse_id] = [df_dog[(df_dog['horseName'] == horse_id)].date.min(), df_dog[(df_dog['horseName'] == horse_id)].date.max()]
                print(horse_id, len(df_dog[(df_dog['horseName'] == horse_id)]), df_dog[(df_dog['horseName'] == horse_id)].date.min(), df_dog[(df_dog['horseName'] == horse_id)].date.max())
            if df_dog.shape[0] == 2:
                print('Only two results, mayve need to reset one...')
                send_telegram_message(f'Only two results for {id_x}, check the logs and reset both to be a new idx')
                continue

            # soemthing about max dates being siginificantly different then i can keep the way it is
            # if not then i can just keep the one with the most results
            first_section_max_date = pd.to_datetime(max_min_dates[list(max_min_dates.keys())[0]][1])
            second_section_min_date = pd.to_datetime(max_min_dates[list(max_min_dates.keys())[1]][0])
            print(first_section_max_date, second_section_min_date)
            print(dogid_counts)

            if (second_section_min_date - first_section_max_date).days > 100:
                print((second_section_min_date - first_section_max_date).days)
                print('Keeping the way it is because different dogs?? check')
                # time.sleep(10)
                continue
            
            max_index = max(dogid_counts, key=dogid_counts.get)
            
            # print(f'Keeping {max_index}')
            # df.loc[(df['@id_x'] == id_x), 'dogname'] = max_index
    
    return df

# def check_order_of_runtime_and_places(df):
#     def is_order_same(group):
#         # Sort group by place and then by runtime within each place
#         place_sorted_indices = group.sort_values(by=['place_processed', 'resultTime']).index
#         # Sort group by runtime
#         runtime_sorted_indices = group.sort_values(by='resultTime').index
        
#         # Check if both orderings are the same
#         return all(place_sorted_indices == runtime_sorted_indices)
    
#     # Apply the function to each group and collect results
#     df_temp = df[~(df['place'].isin(['R', 'S', 'D', 'F', 'T', 'P', 'B', 'N'])) & (df['place'].notnull())]
#     # df_temp['place_processed'] = df_temp['place'].apply(lambda x: int(str(x).replace('=', '')) if x else x)
#     df_temp['place_processed'] = df_temp['place']
#     result = df_temp.groupby('raceCode').apply(is_order_same)
    
#     # Return the results as a DataFrame for better readability
#     return result.reset_index(name='is_order_same')
def time_to_seconds_milliseconds(time_str):
    if time_str is None:
        return 0
    # Split into minutes and the rest
    if ':' in time_str:
        minutes, sec_ms = time_str.split(':')
        seconds_parts = sec_ms.split('.')
        seconds = int(seconds_parts[0]) if seconds_parts[0] != '' else 0
        milliseconds = int(seconds_parts[1]) if len(seconds_parts) > 1 else 0
        total_seconds = int(minutes) * 60 + seconds
    else:
        seconds_parts = time_str.split('.')
        total_seconds = int(seconds_parts[0]) if seconds_parts[0] != '' else 0
        milliseconds = int(seconds_parts[1]) if len(seconds_parts) > 1 else 0
    return total_seconds*1000 + milliseconds

def check_order_of_runtime_and_places(df):
    df_temp = df.copy()
    df_temp = df_temp[(df_temp['horseOverallTime'].notna()) & 
                      (~df_temp['place'].isin(['r', 'n', 'b', 'f', 't', 'p', 'd', 's', 'u']))]
    
    df_temp['horseOverallTime'] = df_temp['horseOverallTime'].apply(lambda x: time_to_seconds_milliseconds(x))
    df_temp['pastThePostPlacing'] = df_temp['pastThePostPlacing'].apply(lambda x: int(str(x).replace('=', '')) if x else None)
    df_temp = df_temp[(df_temp['pastThePostPlacing'].notna()) & (df_temp['trials'] == False)]

    def is_order_same(group):
        # Convert place to a numeric value if needed
        # If place contains '=', remove it and convert to int
        # Sort by place_processed
        group_sorted = group.sort_values(by='pastThePostPlacing')

        # Extract the resultTime values as a numpy array
        times = group_sorted['horseOverallTime'].values

        # Check if times are non-decreasing3
        # This allows ties: e.g., times[i] <= times[i+1]
        return (times[:-1] <= times[1:]).all()

    # Filter out irrelevant places
    df_temp = df_temp[~(df_temp['pastThePostPlacing'].isin(['R', 'S', 'D', 'F', 'T', 'P', 'B', 'N', 'r', 's', 'd', 'f', 't', 'p', 'b', 'n'])) & 
                      (df_temp['pastThePostPlacing'].notnull())]
    
    # Apply the function to each raceCode group
    result = df_temp.groupby('raceCode').apply(is_order_same)
    return result.reset_index(name='is_order_same')


if __name__ == '__main__':
    df = pd.read_csv('historical_topaz_data.csv')

    df = clean_datafields_formats(df)
    df = clean_error_dogs(df)
    df = clean_error_races(df)
    df = trainer_cleaning(df)