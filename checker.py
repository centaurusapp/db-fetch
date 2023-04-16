import logging
import schedule
import time
import sys
import requests
import io
import base64
import os
import boto3
from PIL import Image
from pymongo.mongo_client import MongoClient
from pymongo.database import Database
from pymongo import ReturnDocument
from pymongo.server_api import ServerApi
from queue import Queue
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from botocore.config import Config

url = "http://127.0.0.1:7861"

def checkNewItem(db: Database, itemQueue):
    try:
        logger = logging.getLogger("db-fetch")
        lockedItem = db.images.find_one_and_update(
            {"status": "new", "locked": False},
            {"$set": {"locked": True}}, return_document= ReturnDocument.AFTER)
        if lockedItem:
            logger.info("fetched locked new status: %s", lockedItem)
            itemQueue.put(lockedItem)
    except Exception as e:
        logger.error(e)

def generateImage(newItem):
    try:
        logger = logging.getLogger("db-fetch") 
        if not 'prompt' in newItem:
            logger.error("no expected field")
            return "ERROR"
        payload = {
            "prompt": newItem.get('prompt') + ',' + os.getenv('PROMPT_EXTRA'),
            "steps": 25,
            "negative_prompt":  os.getenv('NEGATIVE_PROMPT'),
        }
        logger.info("processing... %s", payload)
        response = requests.post(url=f'{url}/sdapi/v1/txt2img', json=payload)
        r = response.json()
        for i in r['images']:
            image = Image.open(io.BytesIO(base64.b64decode(i.split(",",1)[0])))
            filename = str(newItem.get('_id')) +'.jpg'
            image.save(filename, quality=95)
            logger.info("file saved: %s", filename)
            image.close()
            uploadImage(filename)
        return "OK"
    except Exception as e:
        logger.error(e)

def uploadImage(filename):
    try:
        logger = logging.getLogger("db-fetch")
        session = boto3.session.Session()
        client = session.client('s3',
                                endpoint_url=os.getenv('S3_URI'),
                                config=Config(
                                        s3={'addressing_style': 'virtual'}, 
                                        retries = {
                                            'max_attempts': 10,
                                            'mode': 'standard'
                                        }),
                                region_name='nyc3',
                                aws_access_key_id=os.getenv('ACCESS_KEY_ID'),
                                aws_secret_access_key=os.getenv('ACCESS_KEY'))
        client.upload_file(filename, os.getenv('BUCKET'), filename, ExtraArgs={'ContentType':'image/jpeg', 'ACL': 'public-read'})
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
        itemQueue = Queue()
        schedule.every(10).seconds.do(checkNewItem, db, itemQueue)
        # schedule.every(1).minutes.do(insertNewItem)
        logger.info("start fetching...")
        while True:
            try:
                if not itemQueue.empty():
                    lockedItem = itemQueue.get()
                    with ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(generateImage, lockedItem)
                        if future.result() == 'OK':
                            logger.info("newItem in progress %s", lockedItem)
                            db.images.update_one(lockedItem, {"$set": {"status": "finished", "last_modified": datetime.utcnow()}})
                        else:
                            logger.error("can't process, unlock item %s", lockedItem)
                            db.images.update_one(lockedItem, {"$set": {"locked": False, "last_modified": datetime.utcnow()}})
                time.sleep(1)
                schedule.run_pending()
            except Exception as e:
                logger.error(e)
    except Exception as e:
        logger.error(e)




if __name__ == '__main__':
   checker()
    # uploadImage("643ac308ee44958bea6d07f8.jpg")