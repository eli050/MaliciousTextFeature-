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


    def get_tweets(self, limit=100):
        try:
            return [self._to_tweet_out(tweet) for tweet in self.db_conn.find({},{"_id":0}).limit(limit)]
        except Exception as e:
            raise DALError(f"Error retrieving tweets: {e}")

    def insert_tweet(self, tweet:dict):
        try:
            self.db_conn.insert_one(tweet)
        except Exception as e:
            raise DALError(f"Error inserting tweet: {e}")

