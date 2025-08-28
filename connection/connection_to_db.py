from pymongo import MongoClient



class Connection:
    """Manages the connection to a MongoDB collection."""
    def __init__(self, client: MongoClient, db_name: str, collection_name: str = None):
        """Initialize the connection to the MongoDB collection."""
        self._client = client
        self._db = self._client[db_name]
        self._collection = self._db[collection_name]

    @property
    def connection(self):
        return self._collection

    @connection.setter
    def connection(self, value):
        self._collection = self._db[value]


    def close_connection(self):
        self._collection.close()


