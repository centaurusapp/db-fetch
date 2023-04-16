import logging
import sys
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import os

def insertNewItem(db):
    try:
        logger = logging.getLogger("db-fetch") 
        mydict = { "status": "new", "prompt": os.getenv('PROMPT'), "locked": False}
        db.images.insert_one(mydict)
    except Exception as e:
        logger.error(e)

def checker():
    try:
        uri = os.getenv('DB_URI')
        # Create a new client and connect to the server
        client = MongoClient(uri, server_api=ServerApi('1'))
        fh = logging.FileHandler("checker.log")
        logging.basicConfig(
            handlers=[fh, logging.StreamHandler(sys.stdout)],
            format="%(asctime)s [%(levelname)s] - p%(process)s {%(pathname)s:%(lineno)d} %(funcName)s: %(message)s",
            level=logging.INFO
            )
        logger = logging.getLogger("db-fetch")
        client.admin.command('ping')
        db = client["mydatabase"]
        db["images"]
        db.images.create_index("status")
        insertNewItem(db)
    except Exception as e:
        logger.error(e)




if __name__ == '__main__':
   checker()