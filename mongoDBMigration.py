from bson.datetime_ms import DatetimeMS
from bson.codec_options import DatetimeConversion
from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

def main():
    # Connect to the source MongoDB instance
    source_client = MongoClient(host=os.getenv("SOURCE_HOST"), datetime_conversion=DatetimeConversion.DATETIME_AUTO)
    source_db = source_client[os.getenv("SOURCE_DB")]


    # Connect to the destination MongoDB instance
    destination_client = MongoClient(os.getenv("DESTINATION_HOST"))
    destination_db = destination_client[os.getenv("DESTINATION_DB")]

    # Get collection names from the source db
    collection_names = source_db.list_collection_names()

    # Iterate the collections
    for collection_name in collection_names:
        source_collection = source_db[collection_name]
        destination_collection = destination_db[collection_name]

        # Get data from the collection
        data = source_collection.find()

        # Move documents to destination
        for document in data:
            try:
                destination_collection.insert_one(document)
            except Exception as e:
                print(f"Error during migration: {e}")

    # Close db connections
    source_client.close()
    destination_client.close()

if __name__ == "__main__":
    main()