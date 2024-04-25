import logging
import unittest
from unittest.mock import Mock, patch

from llama_index.core.schema import Document

from dags.hivemind_etl_helpers.src.db.gdrive.gdrive_loader import GoogleDriveLoader


class TestGoogleDriveLoader(unittest.TestCase):
    def setUp(self):
        self.mock_client_config = {
            "installed": {
                "client_id": "11223.apps.googleusercontent.com",
                "project_id": "test-test",
                "auth_uri": "https://xxx.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "client_secret": "xxx-xxxxxt",
                "redirect_uris": ["http://localhost"],
            }
        }
        self.loader = GoogleDriveLoader(self.mock_client_config)

    def test_load_data_without_input(self):
        loader = GoogleDriveLoader(client_config=None)

        with self.assertRaises(ValueError):
            loader.load_data()

    @patch.object(GoogleDriveLoader, "load_data")
    def test_load_by_folder_id(self, mock_load_data):
        folder_id = ["folder_id_1", "folder_id_2"]
        # mock_data = [{"text": "Document 1"}, {"text": "Document 2"}]
        mock_data = [
            Document(
                id_="folder_id_1.docx",
                metadata={
                    "file_name": "qwertU10p2.docx",
                    "file id": "qwertU10p2",
                    "author": "Jacku",
                    "file name": "Option",
                },
                relationships={},
                text="Option 1: Keep it super casual",
            ),
            Document(
                id_="folder_id_2.docx",
                metadata={
                    "file_name": "qwertU10p3.docx",
                    "file id": "qwertU10p3",
                    "author": "Jacku",
                    "file name": "Option",
                },
                text="Option 1: Keep it super casual",
            ),
        ]
        self.mock_loader = Mock()
        mock_load_data.return_value = mock_data
        loader = GoogleDriveLoader(self.mock_client_config)
        result = loader.load_data(folder_ids=folder_id)

        self.assertEqual(len(result), 2)
        self.assertEqual(result, mock_data)

    @patch.object(GoogleDriveLoader, "load_data")
    def test_load_by_drive_id(self, mock_load_data):
        drive_ids = ["folder_id_1", "folder_id_2"]
        # mock_data = [{"text": "Document 1"}, {"text": "Document 2"}]
        mock_data = [
            Document(
                id_="folder_id_1.docx",
                metadata={
                    "file_name": "qwertU10p2.docx",
                    "file id": "qwertU10p2",
                    "author": "Jacku",
                    "file name": "Option",
                },
                relationships={},
                text="Option 1: Keep it super casual",
            ),
            Document(
                id_="folder_id_2.docx",
                metadata={
                    "file_name": "qwertU10p3.docx",
                    "file id": "qwertU10p3",
                    "author": "Jacku",
                    "file name": "Option",
                },
                text="Option 1: Keep it super casual",
            ),
        ]
        # mock_loader = Mock()
        mock_load_data.return_value = mock_data
        loader = GoogleDriveLoader(self.mock_client_config)
        result = loader.load_data(folder_ids=drive_ids)

        self.assertEqual(len(result), 2)
        self.assertEqual(result, mock_data)

    @patch.object(GoogleDriveLoader, "load_data")
    def test_load_by_file_id(self, mock_load_data):
        mock_loader = Mock()
        file_ids = ["file_id_1", "file_id_2"]
        # mock_data = [{"text": "Document 1"}, {"text": "Document 2"}]
        mock_data = [
            Document(
                id_="file_id_1.docx",
                metadata={
                    "file_name": "qwertU10p2.docx",
                    "file id": "qwertU10p2",
                    "author": "Jacku",
                    "file name": "Option",
                },
                relationships={},
                text="Option 1: Keep it super casual",
            ),
            Document(
                id_="file_id_2.docx",
                metadata={
                    "file_name": "qwertU10p3.docx",
                    "file id": "qwertU10p3",
                    "author": "Jacku",
                    "file name": "Option",
                },
                text="Option 1: Keep it super casual",
            ),
        ]
        # mock_loader = Mock()
        mock_load_data.return_value = mock_data
        loader = GoogleDriveLoader(self.mock_client_config)
        result = loader.load_data(file_ids=file_ids)

        self.assertEqual(len(result), 2)
        self.assertEqual(result, mock_data)

    @patch.object(GoogleDriveLoader, "load_data")
    def test_load_from_folders_exception(self, mock_reader):
        # Arrange
        mock_loader = Mock()
        mock_reader.return_value = mock_loader
        loader = GoogleDriveLoader(client_config=None)
        mock_loader.side_effect = Exception("Test Exception")
        folder_ids = ["folder_id_1", "folder_id_2"]

        # Act
        documents = loader._load_from_folders(mock_loader, folder_ids)
        # print(documents)

        # Assert
        self.assertEqual(len(documents), 0)
