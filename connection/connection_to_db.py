from pymongo import MongoClient


class Connection:
    """Manages the connection to a MongoDB collection."""
    def __init__(self, client: MongoClient, db_name: str):
        """Initialize the connection to the MongoDB db."""
        self._client = client
        self._db = self._client[db_name]


    @property
    def connection(self):
        return self._db



