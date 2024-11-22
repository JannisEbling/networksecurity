import json
import os
import sys

from dotenv import load_dotenv

load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")

import certifi

ca = certifi.where()

import numpy as np
import pandas as pd
import pymongo

from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging


class NetworkDataExtract:
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def csv_to_json_convertor(self, file_path):
        try:
            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)
            records = list(json.loads(data.T.to_json()).values())
            return records
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def insert_data_mongodb(self, records, database, collection):
        try:
            self.database = database
            self.collection = collection
            self.records = records

            logging.info("Attempting to connect to MongoDB...")
            self.mongo_client = pymongo.MongoClient(
                MONGO_DB_URL,
                tlsCAFile=certifi.where(),
                serverSelectionTimeoutMS=60000,  # Increase timeout to 60 seconds
                connectTimeoutMS=30000,
                socketTimeoutMS=30000,
                maxPoolSize=50,
                retryWrites=True,
            )
            
            # Test the connection
            try:
                self.mongo_client.admin.command('ping')
                logging.info("Successfully connected to MongoDB!")
            except Exception as e:
                logging.error(f"Connection test failed: {str(e)}")
                raise

            self.database = self.mongo_client[self.database]

            self.collection = self.database[self.collection]
            
            logging.info(f"Inserting {len(self.records)} records...")
            self.collection.insert_many(self.records)
            logging.info("Data insertion completed successfully!")
            return len(self.records)
        except Exception as e:
            raise NetworkSecurityException(e, sys)


if __name__ == "__main__":
    FILE_PATH = "Network_Data\phisingData.csv"
    DATABASE = "JSEGSecurity"
    Collection = "NetworkData"
    networkobj = NetworkDataExtract()
    records = networkobj.csv_to_json_convertor(file_path=FILE_PATH)
    no_of_records = networkobj.insert_data_mongodb(records, DATABASE, Collection)
    print(no_of_records)
