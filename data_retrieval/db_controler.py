import uvicorn
from fastapi import FastAPI
from typing import List, Dict
from DAL.dal import TweetDAL
from connection.connection_to_db import Connection
from pymongo import MongoClient
import os

MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_DB = os.getenv("MONGO_DB", "tweets_db")
MONGO_URI = os.getenv("MONGO_URI", f"mongodb://{MONGO_HOST}:{MONGO_PORT}")


conn = Connection(client=MongoClient(MONGO_URI), db_name=MONGO_DB)
dal= TweetDAL(connection=conn)
app = FastAPI()

@app.get('/')
def index():
    return {'status': 'OK'}


@app.get('/not_antisemitic_tweets')
def get_not_antisemitic_tweets():
    return dal.get_tweets("not_antisemitic_tweets")

@app.get('/antisemitic_tweets')
def get_not_antisemitic_tweets():
    return dal.get_tweets("antisemitic_tweets")

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8001)