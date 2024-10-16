from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TelegramPlatform:
    def __init__(self, chat_id: str) -> None:
        """
        Parameters
        -----------
        chat_id : str
            check if there's any platform exists
        """
        self._client = MongoSingleton.get_instance().get_client()
        self.chat_id = chat_id

    def check_platform_existance(self) -> bool:
        """
        check if there's any platform exist for a chat_id

        Returns
        --------
        exists : bool
            if the platform exist, return True
            else, False
        """
        pass

    def create_platform(self, community_id: str) -> bool:
        """
        create a platform for the chat_id having the community id

        Parameters
        -----------
        community_id : str
            the community id to assign a platform

        Returns
        ---------
        is_created : bool
            whether the creation of the platform was successful
        """
        pass
