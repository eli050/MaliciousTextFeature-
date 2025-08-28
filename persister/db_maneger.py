import os
from pymongo import MongoClient
from DAL.dal import TweetDAL
from connection.connection_to_db import Connection
from kafka_objects.consumer import Consumer

MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_DB = os.getenv("MONGO_DB", "tweets_db")
MONGO_URI = os.getenv("MONGO_URI", f"mongodb://{MONGO_HOST}:{MONGO_PORT}")

class ManagerPypline:
    def __init__(self):
        self.conn_antisemitic = Connection(client=MongoClient(MONGO_URI), db_name=MONGO_DB, collection_name="antisemitic_tweets")
        self.dal_antisemitic = TweetDAL(connection=self.conn_antisemitic)
        self.conn_not_antisemitic = Connection(client=MongoClient(MONGO_URI), db_name=MONGO_DB, collection_name="not_antisemitic_tweets")
        self.dal_not_antisemitic = TweetDAL(connection=self.conn_not_antisemitic)
        self.cons = Consumer(topics=["enriched_preprocessed_tweets_antisemitic", "enriched_preprocessed_tweets_not_antisemitic"])

    def run_pipeline(self):
        """Consume tweets from Kafka topics and insert them into MongoDB."""
        try:
            for message in self.cons.get_consumer():
                tweet = message.value
                if tweet.get("Antisemitic"):
                    self.dal_antisemitic.insert_tweet(tweet)
                else:
                    self.dal_not_antisemitic.insert_tweet(tweet)
            return "Pipeline completed"
        except Exception as e:
            return f"Error in pipeline: {e}"


