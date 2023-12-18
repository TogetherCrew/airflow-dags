import unittest
# from unittest.mock import Mock, patch
from neo4j_storage import save_label_to_neo4j

class TestSaveLabelToNeo4j(unittest.TestCase):

    # @patch('your_module.Neo4jConnection')  # Replace 'your_module' with the actual name of your module
    def test_save_label_to_neo4j(self):

        # Define a sample label
        sample_label = {'id': '123', 'name': 'SampleLabel'}

        # Call the function with the sample label
        response = save_label_to_neo4j(sample_label)

        self.assertIsNone(response, "The response of save_label_to_neo4j should be None")
