import os
from pymongo import MongoClient
from DAL import DataAccessLayer
from publisher import Producer

MONGO_PASS = os.getenv("MONGO_PASSWORD", "iran135")
MONGO_USER = os.getenv("MONGO_USER", "IRGC_NEW")
MONGO_DB = os.getenv("MONGO_DB", "IranMalDB")
MONGO_URI = os.getenv("MONGO_URI", f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@cluster0.6ycjkak.mongodb.net/")




class PipelineManager:
    def __init__(self):
        self.dal = DataAccessLayer(db_client=MongoClient(MONGO_URI), db_name=MONGO_DB, collection_name="tweets")
        self.pub = Producer()

    def run_pipeline(self,target_column):
        tweets = self.dal.get_100_tweets()
        for tweet in tweets:
            # tweet["CreateDate"] = str(tweet["CreateDate"])
            if tweet.get(target_column):
                self.pub.publish_message("raw_tweets_antisemitic",tweet)
            else:
                self.pub.publish_message("raw_not_tweets_antisemitic",tweet)
        return len(tweets)
