class ExtractRawMembersBase:

    def __init__(self, guild_id: str):
        """
        initialize the class for a specific guild
        """
        self._guild_id = guild_id

    def get_guild_id(self) -> str:
        """
        Returns the guild ID for subclasses
        """
        return self._guild_id

    def extract(self, recompute: bool = False) -> list:
        """
        extract members data
        if recompute = True, then extract the whole members
        else, start extracting from latest saved member's `joined_at` date

        Note: if the user id was duplicate, then replace.
        """
        raise NotImplementedError("This method should be overridden by subclasses")
