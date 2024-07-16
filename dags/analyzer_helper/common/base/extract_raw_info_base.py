from datetime import datetime


class ExtractRawInfosBase:
    def __init__(self, guild_id: str):
        """
        Initialize the class for a specific platform
        """
        self._guild_id = guild_id

    def get_guild_id(self) -> str:
        """
        Returns the guild ID for subclasses
        """
        return self._guild_id

    def extract(self, period: datetime, recompute: bool = False) -> list:
        """
        Extract raw information for a specific platform
        If recompute is True, then ignore the period, and extract the whole data
        """
        raise NotImplementedError("This method should be overridden by subclasses")