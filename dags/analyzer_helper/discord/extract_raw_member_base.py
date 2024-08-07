class ExtractRawMembersBase:
    def __init__(self, platform_id: str):
        """
        initialize the class for a specific platform
        """
        self._platform_id = platform_id

    def get_platform_id(self) -> str:
        """
        Returns the platform_id ID for subclasses
        """
        return self._platform_id

    def extract(self, recompute: bool = False) -> list:
        """
        extract platform_id members data
        if recompute = True, then extract the whole members
        else, start extracting from latest saved member's `joined_at` date

        Note: if the user id was duplicate, then replace.
        """
        raise NotImplementedError("This method should be overridden by subclasses")
