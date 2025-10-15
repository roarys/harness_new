import json
import datetime
import time
import os 
import pandas as pd
from tqdm import tqdm
import re

from pymongo import MongoClient, errors, UpdateOne
from bson.objectid import ObjectId

from dotenv import load_dotenv
load_dotenv()  # take environment variables from .env.

def connect_to_mongodb(collection_name='harness_historical', database_name='harness_rise'):
    if collection_name not in ['harness_historical', 'harness_upcoming', 'greys_topaz_cleaned', 'greys_database_cleaned']:
        print(f"Collection name {collection_name} not recognised")
        return None
    client = MongoClient(f'mongodb+srv://{os.environ.get("MONGODB_USER")}:{os.environ.get("MONGODB_PASS")}@serverlessinstance0.v0fuehq.mongodb.net/', 
                         connectTimeoutMS=5000)
    db = client[database_name]
    collection = db[collection_name]
    return collection

def bulk_data_from_mongodb(collection_name='harness_historical', total_records=10000, batch_size=10000, max_retries=5):
    print(f'Pulling data from MongoDB... trying to get {"ALL" if total_records is None else total_records} records')
    collection = connect_to_mongodb(collection_name)
    if collection is None:
        return None
    create_index_on_date(collection_name=collection_name)

    data = []
    record_count = 0
    retries = 0

    while True:
        if total_records is not None and record_count >= total_records:
            break

        try:
            limit = batch_size
            if total_records is not None:
                limit = min(batch_size, total_records - record_count)

            pipeline = [
                {"$sort": {"date": -1}},
                {"$skip": record_count},
                {"$limit": limit}
            ]

            cursor = collection.aggregate(pipeline, allowDiskUse=True)
            batch = list(cursor)

            if not batch:
                break

            data.extend(batch)
            record_count += len(batch)

            print(f'Processed {record_count} records so far')

        except errors.AutoReconnect as e:
            retries += 1
            if retries >= max_retries:
                print("Max retries reached. Failed to pull data from MongoDB.")
                return pd.DataFrame()

            wait_time = 2 ** retries
            print(f'AutoReconnect error: {e}. Retrying in {wait_time} seconds...')
            time.sleep(wait_time)
            collection = connect_to_mongodb()

        except Exception as e:
            print(f'An error occurred: {e}')
            return pd.DataFrame()

    print(f'Found {record_count} records in MongoDB')
    mongo_df = pd.DataFrame(data)

    for col in ['_id', 'level_0', 'index']:
        if col in mongo_df.columns:
            mongo_df.drop(columns=col, inplace=True)

    if 'date' in mongo_df.columns:
        mongo_df['date'] = pd.to_datetime(mongo_df['date'])

    print(f'Data Size: {mongo_df.shape}')
    if 'date_added' in mongo_df.columns:
        print(f'Most recently updated: {mongo_df["date_added"].max()}')

    return mongo_df

def clean_upcoming_greys_from_mongodb(collection_name='harness_upcoming'):
    collection = connect_to_mongodb(collection_name)
    if collection is None:
        return
    
    # Remove records older x days old. if this is set to 1, it will remove all yesterdays records 
    # days_old = 3
    # result = collection.delete_many({"date": {"$lt": datetime.datetime.now() - datetime.timedelta(days=days_old)}})
    # print(f"{result.deleted_count} records too old so removed successfully.")

    # Specify the columns to check for duplicates
    columns_to_check = ['track', 'raceNumber', 'horseName', 'date']
    
    # Identify the record with the most recent date_added or with a betfair_market_id for each group
    pipeline = [
        {
            "$sort": {"date_added": -1}
        },
        {
            "$group": {
                "_id": {col: f"${col}" for col in columns_to_check},
                "first_id": {
                    "$first": {
                        "$cond": {
                            "if": {"$ne": ["$betfair_market_id", None]},
                            "then": "$_id",
                            "else": None
                        }
                    }
                },
                "ids": {"$push": "$_id"},
                "date_added_ids": {"$push": {"_id": "$_id", "date_added": "$date_added"}}
            }
        }
    ]

    # Run the aggregation pipeline
    results = list(collection.aggregate(pipeline))

    to_remove = []
    to_keep = set()
    
    # Determine which records to keep
    for doc in results:
        if doc['first_id'] is not None:
            to_keep.add(doc['first_id'])
        else:
            # If no document with betfair_market_id, keep the most recent date_added
            sorted_ids = sorted(doc['date_added_ids'], key=lambda x: x['date_added'], reverse=True)
            to_keep.add(sorted_ids[0]['_id'])
        
        for item in doc['ids']:
            if item not in to_keep:
                to_remove.append(item)

    # Delete any duplicate records
    if to_remove:
        result = collection.delete_many({"_id": {"$in": to_remove}})
        if result.deleted_count:
            print(f"{result.deleted_count} Duplicates removed successfully from {collection_name} collection.")
        else:
            print("No duplicates to remove.")
    else:
        print("No duplicates to remove.")


def save_to_mongodb(dataframe, 
                    collection_name='harness_historical', 
                    database_name='harness_rise'):
    
    def generate_unique_ids(data):
        for doc in data:
            doc['_id'] = ObjectId()  # Generate a unique _id for each record
        return data

    def insert_chunks(data, collection, max_retries=5):
        retries = 0
        data = generate_unique_ids(data)
        
        while retries < max_retries:
            try:
                collection.insert_many(data, ordered=False)
                return True
            # Common error handling for MongoDB
            except errors.BulkWriteError as bwe:
                print(f"Bulk write error: {bwe.details}")
                return False
            except errors.AutoReconnect as e:
                retries += 1
                wait_time = 2 ** retries
                print(f'AutoReconnect error: {e}. Retrying in {wait_time} seconds...')
                time.sleep(wait_time)
            except Exception as e:
                print(f"An error occurred: {e}")
                return False
        return False

    # Limit chunk size to avoid exceeding Mongo limits - was tested and 10000 is fine
    chunk_size = 10000
    collection = connect_to_mongodb(collection_name=collection_name, database_name=database_name)

    # Iterate over the dataset in chunks
    print(f'Inserting {len(dataframe)} records into MongoDB...')    
    for i in range(0, len(dataframe), chunk_size):
        print(f'Inserting records {i} to {i+chunk_size}')
        chunk = dataframe[i:i+chunk_size].to_dict(orient='records')
        if not insert_chunks(chunk, collection):
            print(f'Failed to insert records {i} to {i+chunk_size}')
        else:
            print(f'Successfully inserted records {i} to {i+chunk_size}')

    print("Data insertion complete.")

def pull_recent_data_from_mongodb(collection_name='harness_upcoming', added_days_ago=3, ran_days_ago=1):
    create_index_on_date(collection_name)
    collection = connect_to_mongodb(collection_name)
    if collection is None:
        return None
    # Define the query and projection
    # query = {"date_added": {"$gte": datetime.datetime.now() - datetime.timedelta(days=days_old)}}
     # Define the query and projection
    query = {
        "$and": [
            {"date_added": {"$gte": (datetime.datetime.now() - datetime.timedelta(days=added_days_ago)).replace(hour=0, minute=0, second=0, microsecond=0)}},
            {"date": {"$gte": (datetime.datetime.now() - datetime.timedelta(days=ran_days_ago)).replace(hour=0, minute=0, second=0, microsecond=0)}}
        ]
    }
    projection = {"_id": 0}  # Exclude the _id field, include others as needed

    # Pull data from the last x days with projection
    cursor = collection.find(query, projection).batch_size(1000)

    # Efficiently convert cursor to a list of documents
    data = []
    for document in cursor:
        data.append(document)
    
    print(f'Found {len(data)} records added in the last {added_days_ago} days and ran in the last {ran_days_ago} days.')
    return pd.DataFrame(data)
    

def create_index_on_date(collection_name):
    # improves pull speed and data efficiency for queries
    collection = connect_to_mongodb(collection_name)
    collection.create_index([("date", 1)])  # 1 for ascending order, -1 for descending order
    collection.create_index([("date_added", 1)])  
    print("Index field created.")

def turn_all_values_to_lower_replace_quotes(col, collection_name='harness_historical'):
    updates = []
    collection = connect_to_mongodb(collection_name)
    for doc in tqdm(collection.find({col: {"$type": "string"}})):
        original = doc[col]
        # lowercased = original.lower().replace("'", "").replace(".", "").replace("m", "")
        lowercased = str(original).replace("m", "")
        if original != lowercased:
            updates.append(
                {
                    "filter": {"_id": doc["_id"]},
                    "update": {"$set": {col: lowercased}}
                }
            )

    if updates:
        for update in tqdm(updates):
            collection.update_one(update["filter"], update["update"])
    return

def delete_most_recent_date_added_rows():
    print(f'BE VERY CAREFUL WITH THIS FUNCTION')
    time.sleep(5)
    collection = connect_to_mongodb()
    
    # Find the most recent date_added value
    most_recent_doc = collection.find().sort('date_added', -1).limit(1)
    most_recent_date_added = list(most_recent_doc)[0]['date_added']
    
    # Find all documents with the most recent date_added value
    docs_to_delete = collection.find({'date_added': most_recent_date_added})
    
    # Print the most recent date_added value and the number of rows to delete
    num_rows_to_delete = len(list(docs_to_delete))
    print(f"Most recent date_added: {most_recent_date_added}")
    print(f"Number of rows to delete: {num_rows_to_delete}")
    
    # If confirmed, delete the documents
    if num_rows_to_delete > 0 and input("Delete these rows? (YES/n): ") == 'YES':
        result = collection.delete_many({'date_added': most_recent_date_added})
        print(f"{result.deleted_count} records with date_added {most_recent_date_added} deleted successfully.")

def change_col_name(collection_name, old_col_name, new_col_name):
    collection = connect_to_mongodb(collection_name)
    count = collection.count_documents({old_col_name: {"$exists": True}})
    print(f"Documents to update: {count}")
    collection.update_many(
        {old_col_name: {"$exists": True}},
        {"$rename": {old_col_name: new_col_name}}
    )
    print(f"Documents updated: {count}")

def convert_hex_to_decimal_string(collection_name, fields, batch_size=1000):
    collection = connect_to_mongodb(collection_name)

    for field in fields:
        print(f"\n🔄 Converting field: {field}")
        update_count = 0
        skip_count = 0
        failed_docs = []
        bulk_ops = []

        # Count how many docs to process
        total = collection.count_documents({field: {"$exists": True}})
        print(f"  📦 Total documents to scan for '{field}': {total}")

        cursor = collection.find({field: {"$exists": True}}, projection={field: 1}, no_cursor_timeout=True)
        progress_bar = tqdm(cursor, total=total, desc=f"Processing '{field}'", unit="doc")

        for doc in progress_bar:
            value = doc.get(field)

            if value is None or (isinstance(value, str) and value.strip() == ''):
                skip_count += 1
                continue

            try:
                decimal_str = str(int(value, 16))
                bulk_ops.append(
                    UpdateOne({"_id": doc["_id"]}, {"$set": {field: decimal_str}})
                )
            except Exception as e:
                skip_count += 1
                failed_docs.append({"_id": doc["_id"], "original_value": value, "error": str(e)})
                continue

            if len(bulk_ops) >= batch_size:
                collection.bulk_write(bulk_ops, ordered=False)
                update_count += len(bulk_ops)
                bulk_ops = []

        if bulk_ops:
            collection.bulk_write(bulk_ops, ordered=False)
            update_count += len(bulk_ops)

        cursor.close()
        progress_bar.close()

        # Summary
        print(f"  ✅ Updated: {update_count}")
        print(f"  ⚠️ Skipped: {skip_count}")

        if failed_docs:
            print("  ⛔ Failed conversions (first 10 shown):")
            for doc in failed_docs[:10]:
                print(f"    _id: {doc['_id']}, value: {doc['original_value']}, error: {doc['error']}")

    print("\n✅ All fields processed with progress tracking.")


# GREYHOUNDS FUNCTIONS
def update_track_for_race_ids(race_ids, new_track_value, collection_name='greys_topaz_cleaned'):
    from pymongo import UpdateMany
    if not race_ids:
        print("No race IDs provided.")
        return {"matched": 0, "modified": 0}

    collection = connect_to_mongodb(collection_name, database_name='greys_topaz')
    collection.create_index("raceId")

    if collection is None:
        print('Collection not found')
        return {"matched": 0, "modified": 0}

    print(f'Updating track for {len(race_ids)} raceIds to "{new_track_value}"...')

    # Bulk update operations    
    operations = [
        UpdateMany({"raceId": race_id}, {"$set": {"track": new_track_value}})
        for race_id in race_ids
    ]

    result = collection.bulk_write(operations)

    print(f"Matched {result.matched_count}, Modified {result.modified_count}")
    return {"matched": result.matched_count, "modified": result.modified_count}

def export_latest_date_added_to_csv(
    collection_name: str,
    database_name: str,
    output_csv_path: str
):
    """
    Pulls all records from MongoDB with the most recent 'date_added' and saves as a CSV.

    Args:
        mongo_uri: MongoDB connection string (e.g., 'mongodb://localhost:27017')
        database_name: Name of the database.
        collection_name: Name of the collection.
        output_csv_path: File path for the output CSV (e.g., 'latest_data.csv').
    """
    # Connect to MongoDB
    
    collection = connect_to_mongodb(collection_name, database_name)

    # Step 1: Get the most recent date_added
    latest_doc = collection.find_one(
        filter={"date_added": {"$exists": True}},
        sort=[("date_added", -1)]
    )
    if not latest_doc:
        print("No documents found with 'date_added'.")
        return

    latest_date = latest_doc["date_added"]
    print(f"Latest date_added: {latest_date}")

    # Step 2: Find all docs with that latest date
    docs = list(collection.find({"date_added": latest_date}))

    if not docs:
        print(f"No documents found for date_added {latest_date}")
        return

    # Step 3: Convert to DataFrame and save as CSV
    df = pd.DataFrame(docs)
    df.to_csv(output_csv_path, index=False)
    print(f"Exported {len(df)} records to {output_csv_path}")


if __name__ == "__main__":    
    print('Testing MongoDB functions...')
    # Test saving the database onto MongoDB
    # df = bulk_data_from_mongodb(collection_name='harness_historical', total_records=60000)
    # # df = pull_recent_data_from_mongodb(collection_name='greys_database_cleaned', added_days_ago=2, ran_days_ago=10)
    # df['date'] = pd.to_datetime(df['date'])
    # df.sort_values(by='date', inplace=True) 
    # print(df.head())
    # print(df.shape)
    # print(df.date_added.value_counts())
    
    # date_counts = df['date'].value_counts().reset_index()
    # date_counts.columns = ['date', 'count']
    # print(date_counts.sort_values(by='date'))
    # # track date combos
    # betfair_ids = df[df['date'] >= pd.to_datetime(pd.Timestamp.now().strftime('%Y-%m-%d'))].betfair_market_id.unique()
    # print(f'Found {len(betfair_ids)} unique betfair markets from today onwards')

    # df.to_csv('harness_historical.csv', index=False)
    # import numpy as np

    # df = pd.read_csv('harness_historical_with_stew_post_processed.csv', low_memory=False)
    # df['scheduledDate'] = pd.to_datetime(df['scheduledDate'], format='mixed')
    # df['date_added'] = datetime.datetime.now()
    # df.sort_values(by='scheduledDate', inplace=True)
    # # add data formatting
    # def set_to_string(df, col):
    #     df[col] = df[col].apply(lambda x: str(x) if x is not None else None)
    #     return df

    # def set_to_int(df, col, fill_na_value=np.nan):
    #     df[col] = df[col].fillna(-999)
    #     # set to float then to int
    #     df[col] = df[col].astype(float)
    #     df[col] = df[col].astype(int)
    #     # updat the -999 to np.nan
    #     df.loc[df[col] == -999, col] = fill_na_value
    #     return df

    # def set_to_float(df, col, fill_na_value=np.nan):
    #     df[col] = df[col].fillna(-999)
    #     df[col] = df[col].astype(float)
    #     # updat the -999 to np.nan
    #     df.loc[df[col] == -999, col] = fill_na_value
    #     return df

    # def set_to_datetime(df, col, set_utc=False):
    #     df[col] = pd.to_datetime(df[col], utc=set_utc, format='mixed')
    #     df[col] = df[col].replace({pd.NaT: None})
    #     return df

    # def set_to_bool(df, col):
    #     df[col] = df[col].apply(lambda x: True if (x == 'True' or x == True) else False)
    #     return df

    # def set_to_timedelta(df, col):
    #     # set to string first
    #     # df[col] = df[col].astype(str)
    #     df[col] = pd.to_timedelta(df[col])
    #     return df

    # # SET THE TYPES
    # for col in df.columns:
    #     if col in ['driversAvailableTime', 'scheduledDate', 'plannedStartTimestamp', 
    #             'driverDOB', 'horseFoalDate', 'trainerDOB', 'date_added']:
    #         df = set_to_datetime(df, col, set_utc=True)

    #     elif col in ['numberOfRaces', 'fieldSize', 'raceNumber', 'stakes', 
    #                 'age', 'claimingPrice', 'saddlecloth', 'distance']:
    #         df = set_to_int(df, col, fill_na_value=np.nan)

    #     elif col in ['prizemoney', 'leadTime', 'beatenMargin', 'startingPriceTote' ]:
    #         df = set_to_float(df, col, fill_na_value=np.nan)

    #     elif col in ['tab', 'trials', 'claim', 'discretionaryHandicap', 
    #                 'monte', 'deadHeatFlag', 'lateScratchingFlag', 'scratchingFlag', 
    #                 'trotterInPacersRace']:
    #         df = set_to_bool(df, col)
        
    #     elif col in ['prizemoneyPositions', 'long_stew_clustered_features', 'long_stew_unclustered_features']:
    #         # Other formats like dictionaries, lists, etc.
    #         continue

    #     else:
    #         df = set_to_string(df, col)

    # if 'Unnamed: 0' in df.columns:
    #     df.drop(columns=['Unnamed: 0'], inplace=True)

    # print(df.head())
    # print(df.shape)
    # print(df.columns)

    # save_to_mongodb(df, collection
    # 
    # 

    # delete_most_recent_date_added_rows()
    # turn_all_values_to_lower_replace_quotes('stewardsCommentsLong')
    # change_col_name(collection_name='harness_historical', 
    #                 old_col_name='scheduledDate', 
    #                 new_col_name='date')

    
    # pull all data from mongodb
    # df = bulk_data_from_mongodb(collection_name='harness_historical', total_records=None)
    # df.to_csv('harness_historical_all.csv', index=False)


    list_of_ids_historical = ['clubId', 'trackId', 'breederId', 'broodmareSireId', 'damId', 'driverId', 
                   'horseId', 'ownerId', 'sireId', 'trainerId']
    
    list_of_ids_upcoming = ['clubId', 'trackId', 'breederId', 'broodmareSireId', 'damId', 'driverId', 
                   'horseId', 'trainerId']
    
    export_latest_date_added_to_csv(collection_name='greys_database_cleaned', 
                                    database_name='greys_database', 
                                    output_csv_path='old_greys_latest.csv')
    # convert_hex_to_decimal_string(collection_name='harness_historical', fields=list_of_ids_historical[1:])
    # convert_hex_to_decimal_string(collection_name='harness_upcoming', fields=list_of_ids_upcoming)
