import unittest
from datetime import datetime
from analyzer_helper.discourse.extract_raw_info import ExtractRawInfo
from github.neo4j_storage.neo4j_connection import Neo4jConnection

class TestExtractRawInfo(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.neo4jConnection = Neo4jConnection()
        cls.driver = cls.neo4jConnection.connect_neo4j()
        cls.forum_endpoint = "http://test_forum"
        cls.extractor = ExtractRawInfo(cls.forum_endpoint)

        # Clear existing data
        with cls.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

        # Set up test data
        with cls.driver.session() as session:
            session.run("""
                CREATE (f:DiscourseForum {endpoint: $endpoint, uuid: 'forum-uuid'}),
                       (u1:DiscourseUser {id: 'user1', name: 'User One'}),
                       (u2:DiscourseUser {id: 'user2', name: 'User Two'}),
                       (p1:DiscoursePost {id: '1', content: 'Post 1', topicId: 'topic-uuid'}),
                       (p2:DiscoursePost {id: '2', content: 'Post 2', topicId: 'topic-uuid'}),
                       (t:DiscourseTopic {id: 'topic-uuid', forumUuid: 'forum-uuid'}),
                       (c:DiscourseCategory {id: 'category1', name: 'Category 1'}),
                       (p1)<-[:HAS_POST]-(t),
                       (p2)<-[:HAS_POST]-(t),
                       (p1)<-[:POSTED]-(u1),
                       (p2)<-[:POSTED]-(u2),
                       (p1)<-[:LIKED]-(u2),
                       (p2)<-[:REPLY_TO]-(p1),
                       (c)-[:HAS_TOPIC]->(t)
                """, {'endpoint': cls.forum_endpoint})
        
        # Optional: Print nodes and relationships
        with cls.driver.session() as session:
            result = session.run("MATCH (n)-[r]->() RETURN n, r LIMIT 20")
            for item in result:
                print(item)

    @classmethod
    def tearDownClass(cls):
        with cls.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        cls.extractor.close()
        cls.driver.close()

    def test_fetch_post_details(self):
        post_details = self.extractor.fetch_post_details()
        self.assertEqual(len(post_details), 2)
        self.assertTrue(any(post['author_name'] == 'User One' for post in post_details))
        self.assertTrue(any(post['author_name'] == 'User Two' for post in post_details))

    def test_fetch_post_categories(self):
        post_details = self.extractor.fetch_post_details()
        post_ids = [post["post_id"] for post in post_details]
        post_categories = self.extractor.fetch_post_categories(post_ids)
        self.assertEqual(len(post_categories), 2)
        self.assertTrue(any(category['category_name'] == 'Category 1' for category in post_categories))

    def test_fetch_raw_data(self):
        combined_data = self.extractor.fetch_raw_data()
        self.assertEqual(len(combined_data), 2)
        self.assertTrue(any(post['author_name'] == 'User One' and post['category_name'] == 'Category 1' for post in combined_data))
        self.assertTrue(any(post['author_name'] == 'User Two' and post['category_name'] == 'Category 1' for post in combined_data))
