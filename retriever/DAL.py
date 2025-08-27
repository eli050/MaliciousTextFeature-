from pymongo import MongoClient


class DALError(Exception):
    """Base class for exceptions in this module."""
    pass

class DataAccessLayer:
    def __init__(self, db_client:MongoClient, db_name: str, collection_name: str):
        self.db_client = db_client
        self.db = self.db_client[db_name]
        self.collection = self.db[collection_name]
        self.last_timestamp = None

    def get_100_tweets(self) -> list:
        try:
            if self.last_timestamp is None:
                data = list(self.collection.find({},{"_id":0}).sort("CreateDate", 1).limit(100))
                self.last_timestamp = data[-1]["CreateDate"]
            else:
                data = list(self.collection.find({"CreateDate":
                             {"$gt": self.last_timestamp}},{"_id":0})
                            .sort("CreateDate", 1).limit(100))
                if data:
                    self.last_timestamp = data[-1]["CreateDate"]
            self.db_client.close()
            return data
        except Exception as e:
            raise DALError(f"Error retrieving tweets: {e}")

