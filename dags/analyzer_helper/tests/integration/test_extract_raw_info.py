from datetime import datetime
import unittest
from analyzer_helper.discourse.extract_raw_info import ExtractRawInfo
from hivemind_etl_helpers.src.utils.mongo import MongoSingleton
from github.neo4j_storage.neo4j_connection import Neo4jConnection

class TestExtractRawInfo(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.neo4jConnection = Neo4jConnection()
        cls.client = MongoSingleton.get_instance().client
        cls.driver = cls.neo4jConnection.connect_neo4j()
        cls.forum_endpoint = "http://test_forum" 
        cls.platform_id = "platform_db"       
        cls.platform_db = cls.client[cls.platform_id]
        cls.extractor = ExtractRawInfo(cls.forum_endpoint, cls.platform_id)
        cls.rawmemberactivities_collection = cls.platform_db["rawmemberactivities"]

        with cls.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

        with cls.driver.session() as session:
            session.run("""
                CREATE (f:DiscourseForum {endpoint: $endpoint, uuid: 'forum-uuid'}),
                       (u1:DiscourseUser {id: 'user1', name: 'User One'}),
                       (u2:DiscourseUser {id: 'user2', name: 'User Two'}),
                       (p1:DiscoursePost {id: '1', content: 'Post 1', createdAt: '2023-01-01T00:00:00Z', topicId: 'topic-uuid'}),
                       (p2:DiscoursePost {id: '2', content: 'Post 2', createdAt: '2023-01-02T00:00:00Z', topicId: 'topic-uuid'}),
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

    @classmethod
    def tearDownClass(cls):
        with cls.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        cls.extractor.close()
        cls.driver.close()

    def test_fetch_post_details(self):
        post_details = self.extractor.fetch_post_details()
        self.assertEqual(len(post_details), 2)
        author_names = [post['author_name'] for post in post_details]
        self.assertIn('User One', author_names)
        self.assertIn('User Two', author_names)

        for post in post_details:
            self.assertIn('post_id', post)
            self.assertIn('author_id', post)
            self.assertIn('created_at', post)
            self.assertIn('author_name', post)
            self.assertIn('reactions', post)
            self.assertIn('replied_post_id', post)
            self.assertIn('topic_id', post)

    def test_fetch_post_categories(self):
        post_details = self.extractor.fetch_post_details()
        post_ids = [post["post_id"] for post in post_details]
        post_categories = self.extractor.fetch_post_categories(post_ids)
        self.assertEqual(len(post_categories), 2)

        for category in post_categories:
            self.assertIn('post_id', category)
            self.assertIn('category_id', category)

    def test_fetch_raw_data(self):
        combined_data = self.extractor.fetch_raw_data()
        self.assertEqual(len(combined_data), 2)
        author_names_categories = [(post['author_name'], post['category_id']) for post in combined_data]
        self.assertIn(('User One', 'category1'), author_names_categories)
        self.assertIn(('User Two', 'category1'), author_names_categories)

        for post in combined_data:
            self.assertIn('post_id', post)
            self.assertIn('author_id', post)
            self.assertIn('created_at', post)
            self.assertIn('author_name', post)
            self.assertIn('reactions', post)
            self.assertIn('replied_post_id', post)
            self.assertIn('topic_id', post)
            self.assertIn('category_id', post)
            
    def test_extract_with_recompute(self):
        self.rawmemberactivities_collection.delete_many({})

        period = datetime.now()
        data = self.extractor.extract(period, recompute=True)

        # Check if data is fetched from the Neo4j database without date filtering
        self.assertEqual(len(data), 2)
        author_names = [post['author_name'] for post in data]
        self.assertIn('User One', author_names)
        self.assertIn('User Two', author_names)

    def test_extract_without_recompute_no_latest_activity(self):
        self.rawmemberactivities_collection.delete_many({})
        
        result = self.extractor.extract(period=datetime(2023, 1, 1), recompute=False)
        self.assertGreater(len(result), 0)
    
    def test_extract_without_recompute_latest_activity_before_period(self):
        self.rawmemberactivities_collection.delete_many({})
        self.rawmemberactivities_collection.insert_one({'date': '2022-12-31T00:00:00Z'})
        
        result = self.extractor.extract(period=datetime(2023, 1, 1), recompute=False)
        self.assertGreater(len(result), 0)
        self.assertIn('1', [post['post_id'] for post in result])
        self.assertIn('2', [post['post_id'] for post in result])

    def test_extract_without_recompute_latest_activity_after_period(self):
        self.rawmemberactivities_collection.delete_many({})
        self.rawmemberactivities_collection.insert_one({'date': '2023-01-02T00:00:00Z'})
        
        result = self.extractor.extract(period=datetime(2023, 1, 1), recompute=False)
        self.assertGreater(len(result), 0)
        self.assertIn('2', [post['post_id'] for post in result])