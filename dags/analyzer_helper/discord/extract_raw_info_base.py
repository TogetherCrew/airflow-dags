from datetime import datetime


class ExtractRawInfosBase:
    def __init__(self, platform_id: str):
        """
        Initialize the class for a specific platform
        """
        self._platform_id = platform_id

    def get_platform_id(self) -> str:
        """
        Returns the platform ID for subclasses
        """
        return self._platform_id

    def extract(self, period: datetime, recompute: bool = False) -> list:
        """
        Extract raw information for a specific platform
        If recompute is True, then ignore the period, and extract the whole data
        """
        raise NotImplementedError("This method should be overridden by subclasses")
