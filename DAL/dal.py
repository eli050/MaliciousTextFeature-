from pymongo import MongoClient

from DAL.models import TweetInDB


class DALError(Exception):
    """Base class for exceptions in this module."""
    pass

class TweetDAL:
    def __init__(self, client:MongoClient,collection_name:str, db_name='tweets_db'):
        self.client = client
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    @staticmethod
    def _to_tweet_out(tweets:dict) -> TweetInDB:
        try:
            tweet = TweetInDB(**tweets)
            return tweet
        except Exception as e:
            raise DALError(f"Error converting to TweetInDB: {e}")


    def get_tweets(self, limit=100):
        try:
            return [self._to_tweet_out(tweet) for tweet in self.collection.find({},{"_id":0}).limit(limit)]
        except Exception as e:
            raise DALError(f"Error retrieving tweets: {e}")
    def insert_tweet(self, tweet:dict):
        try:
            self.collection.insert_one(tweet)
        except Exception as e:
            raise DALError(f"Error inserting tweet: {e}")

