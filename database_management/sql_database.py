import sqlite3
import pandas as pd

from datetime import datetime


class Database:
    def __init__(self):
        pass

    def read_database(self, aus=False):
        print('FIND ME DOG')
        # Read data in from SQLite3 database'
        dogs_data_con = sqlite3.connect('final_mw_greyhounds_data.db')
        # df = pd.read_sql_query("SELECT * from aus_2018_2024", dogs_data_con)
        dogs_data = pd.read_sql_query("SELECT * from aus_2018_2024_merged_clean", dogs_data_con)
        dogs_data_con.close()
        print(f'Data read from database: {dogs_data.shape}')

        return dogs_data

    def add_to_database(self, dataframe, aus=False):
        df = self.read_database(aus=aus)
        # add the new dataframe to the existing database
        df_merged = pd.concat([df, dataframe], ignore_index=True)

        df_merged['date'] = df_merged['date'].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d'))
        df_merged['date_added'] = df_merged['date_added'].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d'))
        self.save_to_database(df_merged, aus=aus, if_exists='append')            

    def save_to_database(self, dataframe, aus=False, if_exists='replace'):
        print('Saving data to database')
        # Store data into a simple SQLite3 database
        if 'level_0' in dataframe.columns:
            dataframe.drop(columns=['level_0'], inplace=True)

        dogs_data_con = sqlite3.connect('final_mw_greyhounds_data.db')
        dataframe.to_sql('aus_2018_2024_merged_clean', dogs_data_con, if_exists=if_exists, index=False)
        dogs_data_con.close()
        print('Data saved to database')

if __name__ == "__main__":
    db = Database()
    df = db.read_database()
    print(df.head())
    print(df.shape)
    print(df.date.max())
    print(df.date.min())
    print(df.date.tail())
