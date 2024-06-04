from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class FetchDiscordPlatforms:
    """
    A class to fetch Discord platform data from a MongoDB collection.

    Attributes:
        client (MongoClient): The MongoDB client.
        db (Database): The MongoDB database.
        collection (Collection): The MongoDB collection.
    """

    def __init__(self, db_name='Core', collection='platforms'):
        """
        Initializes the FetchDiscordPlatforms class.

        Args:
            db_name (str): The name of the database. Defaults to 'Core'.
            collection (str): The name of the collection. Defaults to 'platforms'.
        """
        self.client = MongoSingleton.get_instance().client
        self.db = self.client[db_name]
        self.collection = self.db[collection]

    def fetch_all(self):
        """
        Fetches all Discord platforms from the MongoDB collection.

        Returns:
            list: A list of dictionaries, each containing platform data with the following fields:
                - platform_id: The platform ID (_id from MongoDB).
                - metadata: A dictionary containing action, window, period, selectedChannels, and id.
                - recompute: A boolean set to False.
        """
        query = {'platform': 'discord'}
        projection = {
            '_id': 1,
            'metadata.action': 1,
            'metadata.window': 1,
            'metadata.period': 1,
            'metadata.selectedChannels': 1,
            'metadata.id': 1,
        }

        cursor = self.collection.find(query, projection)
        platforms = []

        for doc in cursor:
            platform_data = {
                'platform_id': str(doc['_id']),
                'metadata': {
                    'action': doc.get('metadata', {}).get('action', None),
                    'window': doc.get('metadata', {}).get('window', None),
                    'period': doc.get('metadata', {}).get('period', None),
                    'selectedChannels': doc.get('metadata', {}).get('selectedChannels', None),
                    'id': doc.get('metadata', {}).get('id', None)
                },
                'recompute': False
            }
            platforms.append(platform_data)

        return platforms
