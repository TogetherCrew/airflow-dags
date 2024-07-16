from datetime import datetime
from analyzer_helper.discourse.utils.convert_date_time_formats import DateTimeFormatConverter
from github.neo4j_storage.neo4j_connection import Neo4jConnection
from typing import Optional


class ExtractRawInfo:
    def __init__(self, forum_endpoint:str):
        """
        Initialize the ExtractRawInfo with the forum endpoint and set up Neo4j connection.
        """
        self.neo4jConnection = Neo4jConnection()
        self.driver = self.neo4jConnection.connect_neo4j()
        self.forum_endpoint = forum_endpoint
        self.converter = DateTimeFormatConverter()

    def close(self):
        """
        Close the Neo4j connection.
        """
        self.driver.close()

    def fetch_post_details(self, created_at: Optional[str] = None, comparison: Optional[str] = None) -> list:
        """
        Fetch details of posts from the Discourse forum.

        :param created_at: Optional datetime string to filter posts created after this date.
        :param comparison: Optional comparison operator, either 'gt' for greater than or 'gte' for greater than or equal to.
        :return: List of dictionaries containing post details.
        """
        where_clause = ""
        if created_at and comparison:
            operator = '>' if comparison == 'gt' else '>='
            where_clause = f"WHERE post.createdAt {operator} $createdAt"

        query = f"""
        MATCH (forum:DiscourseForum {{endpoint: $forum_endpoint}})
        WITH forum
        MATCH (topic:DiscourseTopic {{forumUuid: forum.uuid}})
        MATCH (topic)-[:HAS_POST]->(post:DiscoursePost)
        {where_clause}
        OPTIONAL MATCH (post)<-[:POSTED]-(author:DiscourseUser)
        OPTIONAL MATCH (post)<-[:LIKED]-(likedUser:DiscourseUser)
        OPTIONAL MATCH (post)-[:REPLY_TO]->(repliedPost:DiscoursePost)
        RETURN
          id(post) AS post_id,
          id(author) AS author_id,
          post.createdAt AS created_at,
          author.name AS author_name,
          collect(DISTINCT likedUser.id) AS reactions,
          id(repliedPost) AS replied_post_id,
          id(topic) AS topic_id
        LIMIT 10
        """

        with self.driver.session() as session:
            if created_at and comparison:
                result = session.run(query, forum_endpoint=self.forum_endpoint, createdAt=created_at)
            else:
                result = session.run(query, forum_endpoint=self.forum_endpoint)
            posts = result.data()

        return posts

    def fetch_post_categories(self, post_ids):
        """
        Fetch categories associated with given post IDs.

        :param post_ids: List of post IDs.
        :return: List of dictionaries containing post categories.
        """
        # We are matching topics by post.topicId
        # query = """
        # UNWIND $post_ids AS post_id
        # MATCH (post:DiscoursePost)
        # WHERE id(post) = post_id
        # MATCH (topic:DiscourseTopic)
        # WHERE id(topic) = post.topicId
        # OPTIONAL MATCH (category:DiscourseCategory)-[:HAS_TOPIC]->(topic)
        # RETURN
        #   id(post) AS post_id,
        #   id(category) AS category_id,
        #   category.name AS category_name
        # """
        # We are matching topics by topic.HAS_POST post.id
        query = """
        UNWIND $post_ids AS post_id
        MATCH (post:DiscoursePost)
        WHERE id(post) = post_id
        MATCH (topic:DiscourseTopic)-[:HAS_POST]->(post)
        OPTIONAL MATCH (category:DiscourseCategory)-[:HAS_TOPIC]->(topic)
        RETURN
        id(post) AS post_id,
        id(category) AS category_id,
        category.name AS category_name
        """
        with self.driver.session() as session:
            result = session.run(query, post_ids=post_ids)
            records = [record.data() for record in result]
            print(f"Cathegories fetched: ", records)
            self.driver.close()
            # print(f"Number of records fetched in fetch_post_categories: {len(records)}")
            return records
    
    def get_latest_post_created_at(self, forum_endpoint: str) -> Optional[str]:
        """
        Fetches the created_at timestamp of the latest post from a specified forum.

        Args:
            uri (str): The URI to connect to the Neo4j database.
            user (str): The username for the Neo4j database.
            password (str): The password for the Neo4j database.
            forum_endpoint (str): The endpoint of the Discourse forum.

        Returns:
            Optional[str]: The created_at timestamp of the latest post, or None if no posts are found.
        """
        
        query = """
        MATCH (forum:DiscourseForum {endpoint: $forum_endpoint})
        WITH forum
        MATCH (topic:DiscourseTopic {forumUuid: forum.uuid})
        MATCH (topic)-[:HAS_POST]->(post:DiscoursePost)
        RETURN post.createdAt AS created_at
        ORDER BY post.createdAt DESC
        LIMIT 1
        """
        
        with self.driver.session() as session:
            result = session.run(query, forum_endpoint=forum_endpoint)
            latest_post = result.single()
            
            self.driver.close()
        
        if latest_post:
            return latest_post["created_at"]
        else:
            return None
        
    def combine_posts_with_cathegories(self, post_details, post_categories) -> list:
        """
        Combine post details with their respective categories.

        :param post_details: List of dictionaries containing post details.
        :param post_categories: List of dictionaries containing post categories.
        :return: List of combined dictionaries.
        """
        combined_results = []
        for post in post_details:
            matched = False
            for category in post_categories:
                if post["post_id"] == category["post_id"]:
                    post["category_id"] = category["category_id"]
                    post["category_name"] = category["category_name"]
                    matched = True
                    break
            if not matched:
                post["category_id"] = None
                post["category_name"] = None
            combined_results.append(post)
        return combined_results

    def fetch_raw_data(self, created_at: Optional[str] = None, comparison: Optional[str] = None) -> list:
        """
        Fetch and combine post details and categories.

        :param created_at: Optional datetime string to filter posts created after this date.
        :param comparison: Optional comparison operator, either 'gt' for greater than or 'gte' for greater than or equal to.
        :return: List of combined dictionaries containing post details and categories.
        """
        if created_at and comparison:
            post_details = self.fetch_post_details(created_at, comparison)
        else:
            post_details = self.fetch_post_details()

        post_ids = [post["post_id"] for post in post_details]
        post_categories = self.fetch_post_categories(post_ids)
        return self.combine_posts_with_cathegories(
            post_details, post_categories
        )

    def extract(self, period: datetime, recompute: bool = False) -> list:
        """
        Extract data based on the period and recompute flag.

        :param period: The datetime period to filter posts.
        :param recompute: Flag to indicate if recompute is needed.
        :return: List of combined dictionaries containing post details and categories.
        """
        data = []
        if recompute:
            data = self.fetch_raw_data()
        else:
            latest_activity_date = self.get_latest_post_created_at(
                self.forum_endpoint
            )
            if latest_activity_date is not None:
                period_iso_format = self.converter.to_iso_format(period)
                if latest_activity_date >= period_iso_format:
                    data = self.fetch_post_details(latest_activity_date, "gt")
                else:
                    data = self.fetch_post_details(period_iso_format, "gte")
            else:
                data = self.fetch_post_details()
        return data

# TODO: Testing purposes, remove!
#     @staticmethod
#     def print_combined_data(combined_results):
#         """
#         Print the combined data of posts.

#         :param combined_results: List of combined dictionaries.
#         """
#         for post in combined_results:
#             print(f"Post ID: {post['post_id']}")
#             print(f"Author ID: {post['author_id']}")
#             print(f"Author Name: {post['author_name']}")
#             print(f"Reactions: {post['reactions']}")
#             print(f"Replied Post ID: {post['replied_post_id']}")
#             print(f"Topic ID: {post['topic_id']}")
#             print(f"Category ID: {post.get('category_id', 'N/A')}")
#             print(f"Category Name: {post.get('category_name', 'N/A')}")
#             print("-" * 40)
#         print(f"Total number of combined records: {len(combined_results)}")
        
# extractor = ExtractRawInfo("gov.optimism.io")
# results = extractor.fetch_raw_data()
# print("Results:", results)

