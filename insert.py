from datetime import datetime
import pandas as pd
from pymongo import MongoClient

batch_size = 20000
mongo_uri = "mongodb+srv://nainsagar45:sagar123@cluster0.xrzfzmj.mongodb.net/"
database_name = "ucdb"
collection_name = "green_eat"

client = MongoClient(mongo_uri)
db = client[database_name]
collection = db[collection_name]

collection.delete_many({})

try:
    collection.create_index([("TRANSACTION DATE", 1)])  
    collection.create_index([("Business Facility", 1)]) 
    print("Indexes created successfully!")
except Exception as e:
    print(f"An error occurred while creating indexes: {e}")
finally:
    client.close()

file_path = "/home/sagarnain/python/flask1/uc_results_gf.csv"
chunk_iter = pd.read_csv(file_path, chunksize=batch_size)

def convert_dates(data):
    for record in data:
        if "TRANSACTION DATE" in record and pd.notna(record["TRANSACTION DATE"]):
            try:
                record["TRANSACTION DATE"] = datetime.strptime(record["TRANSACTION DATE"], "%d/%m/%y")
            except ValueError as e:
                print(f"Date conversion error for {record['TRANSACTION DATE']}: {e}")
                record["TRANSACTION DATE"] = None
    return data

for chunk_index, chunk in enumerate(chunk_iter):
    start = datetime.now()
    data = chunk.to_dict(orient="records")
    
    data = convert_dates(data)
    
    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]
    
    try:
        result = collection.insert_many(data)
        print(f"Batch {chunk_index + 1} inserted successfully! {len(result.inserted_ids)} documents added.")
    except Exception as e:
        print(f"An error occurred in batch {chunk_index + 1}: {e}")
    finally:
        client.close()
    
    end = datetime.now()
    print('Time taken: ', (end - start).total_seconds())

print("Insertions completed.")

client = MongoClient(mongo_uri)
db = client[database_name]
collection = db[collection_name]

try:
    result = collection.update_many(
        { "CO2_ITEM": { "$type": "double", "$eq": float('nan') } },
        { "$set": { "CO2_ITEM": 0 } }                              
    )
    print(f"Matched {result.matched_count} documents, Modified {result.modified_count} documents.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    client.close()
