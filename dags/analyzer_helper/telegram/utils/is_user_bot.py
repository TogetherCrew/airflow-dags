from github.neo4j_storage.neo4j_connection import Neo4jConnection

class UserBotChecker:
    def __init__(self):
        """
        Initialize the UserBotChecker with the Neo4j connection parameters.
        """
        self.neo4jConnection = Neo4jConnection()
        self.driver = self.neo4jConnection.connect_neo4j()

    def is_user_bot(self, user_id: int) -> bool:
        """
        Check if the specified user ID corresponds to a bot.

        Args:
        user_id (int): The ID of the user to check.

        Returns:
        bool: True if the user is a bot, False otherwise.
        """
        query = """
        MATCH (user:TGUser {id: $user_id})
        RETURN user.is_bot AS is_bot
        """
        with self.driver.session() as session:
            result = session.run(query, user_id=user_id)
            is_bot = result.single()
            if is_bot is not None:
                return is_bot['is_bot']
            else:
                return False
