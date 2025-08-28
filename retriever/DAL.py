from pymongo import MongoClient

from connection.connection_to_db import Connection


class DALError(Exception):
    """Base class for exceptions in this module."""
    pass

class DataAccessLayer:
    """Data Access Layer for MongoDB operations."""
    def __init__(self,connection:Connection,collection_name:str = "tweets"):
        """Initialize with a MongoDB connection."""
        self.conn = connection
        self.db_conn = self.conn.connection[collection_name]
        self._last_timestamp = None

    def get_100_tweets(self) -> list:
        """Retrieve up to 100 tweets from the collection, sorted by CreateDate."""
        try:
            if self._last_timestamp is None:
                data = list(self.db_conn.find({},{"_id":0}).sort("CreateDate", 1).limit(100))
                self._last_timestamp = data[-1]["CreateDate"]
            else:
                data = list(self.db_conn.find({"CreateDate":
                             {"$gt": self._last_timestamp}}, {"_id":0})
                            .sort("CreateDate", 1).limit(100))
                if data:
                    self._last_timestamp = data[-1]["CreateDate"]
            return data
        except Exception as e:
            raise DALError(f"Error retrieving tweets: {e}")

