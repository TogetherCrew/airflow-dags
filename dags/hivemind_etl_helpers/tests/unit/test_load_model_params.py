import os
import unittest

from tc_hivemind_backend.db.utils.model_hyperparams import load_model_hyperparams


class TestLoadModelHyperparams(unittest.TestCase):
    def setUp(self):
        # Set up environment variables for testing
        os.environ["CHUNK_SIZE"] = "128"
        os.environ["EMBEDDING_DIM"] = "256"

    def tearDown(self):
        # Clean up environment variables after testing
        del os.environ["CHUNK_SIZE"]
        del os.environ["EMBEDDING_DIM"]

    def test_load_model_hyperparams_success(self):
        # Test when environment variables are set correctly
        chunk_size, embedding_dim = load_model_hyperparams()
        self.assertEqual(chunk_size, 128)
        self.assertEqual(embedding_dim, 256)

    def test_load_model_hyperparams_invalid_chunk_size(self):
        # Test when CHUNK_SIZE environment variable is not a valid integer
        os.environ["CHUNK_SIZE"] = "invalid"
        with self.assertRaises(ValueError) as context:
            load_model_hyperparams()
        self.assertEqual(
            str(context.exception), "invalid literal for int() with base 10: 'invalid'"
        )

    def test_load_model_hyperparams_invalid_embedding_dim(self):
        # Test when EMBEDDING_DIM environment variable is not a valid integer
        os.environ["EMBEDDING_DIM"] = "invalid"
        with self.assertRaises(ValueError) as context:
            load_model_hyperparams()
        self.assertEqual(
            str(context.exception), "invalid literal for int() with base 10: 'invalid'"
        )
