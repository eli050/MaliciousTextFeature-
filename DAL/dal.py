from pymongo import MongoClient

from DAL.models import TweetInDB
from connection.connection_to_db import Connection


class DALError(Exception):
    """Base class for exceptions in this module."""
    pass

class TweetDAL:
    def __init__(self, connection: Connection):
        """Initialize with a MongoDB connection."""
        self.conn = connection
        self.db_conn = self.conn.connection

    @staticmethod
    def _to_tweet_out(tweets:dict) -> TweetInDB:
        try:
            tweet = TweetInDB(**tweets)
            return tweet
        except Exception as e:
            raise DALError(f"Error converting to TweetInDB: {e}")


    def get_tweets(self,collection_name, limit=None):
        try:
            collection_conn = self.db_conn[collection_name]
            if limit is None:
                return [self._to_tweet_out(tweet) for tweet in collection_conn.find({},{"_id":0})]
            return [self._to_tweet_out(tweet) for tweet in collection_conn.find({},{"_id":0}).limit(limit)]
        except Exception as e:
            raise DALError(f"Error retrieving tweets: {e}")

    def insert_tweet(self,collection_name, tweet:dict):
        try:
            collection_conn = self.db_conn[collection_name]
            collection_conn.insert_one(tweet)
        except Exception as e:
            raise DALError(f"Error inserting tweet: {e}")

