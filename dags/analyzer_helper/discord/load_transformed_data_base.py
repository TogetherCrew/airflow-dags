class LoadTransformedDataBase:
    def __init__(self, platform_id: str):
        """
        Initialize the load raw data class for a specific platform
        """
        self._platform_id = platform_id

    def get_platform_id(self) -> str:
        """
        Returns the platform ID for subclasses
        """
        return self._platform_id

    def load(self, processed_data: list[dict], recompute: bool = False):
        """
        Load the raw transformed data into their
        respective collection under platform_id database
        If recompute is True, then replace the whole data with the processed data
        """
        pass
