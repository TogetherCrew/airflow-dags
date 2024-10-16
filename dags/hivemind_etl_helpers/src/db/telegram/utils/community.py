from hivemind_etl_helpers.src.utils.mongo import MongoSingleton


class TelegramCommunity:
    def __init__(self, chat_id: str) -> None:
        """
        Parameters
        -----------
        chat_id : str
            check if there's any community exists
        """
        self._client = MongoSingleton.get_instance().get_client()
        self.chat_id = chat_id

    def check_community_existance(self) -> str | None:
        """
        check if there's any community exist for a chat_id

        Returns
        --------
        community_id : str | None
            if the community exist, return its id
            else, return None
        """
        pass

    def create_community(self) -> str:
        """
        create a community for the chat_id having the community id

        Returns
        ---------
        community_id : str
            the community id that was created
        """
        # TODO: create a random value for the community id
        # and insert it in mongo db
        pass
