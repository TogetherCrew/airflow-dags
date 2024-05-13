import unittest
from unittest.mock import patch

from hivemind_etl_helpers.src.db.gdrive.gdrive_loader import GoogleDriveLoader
from llama_index.core.schema import Document
from llama_index.readers.google import GoogleDriveReader


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
        loader = GoogleDriveLoader(client_config=self.mock_client_config)

        with self.assertRaises(ValueError):
            loader.load_data()

    @patch.object(GoogleDriveReader, "load_data")
    def test_load_by_folder_id(self, mock_load_data):
        folder_id = ["folder_id_1", "folder_id_2"]
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
        mock_load_data.return_value = mock_data
        loader = GoogleDriveLoader(client_config=self.mock_client_config)
        result = loader.load_data(folder_ids=folder_id)

        self.assertEqual(len(result), 4)
        self.assertEqual(result[0].id_, mock_data[0].id_)

    @patch.object(GoogleDriveReader, "load_data")
    def test_load_by_drive_id(self, mock_load_data):
        drive_ids = ["folder_id_1", "folder_id_2"]
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
        mock_load_data.return_value = mock_data
        loader = GoogleDriveLoader(client_config=self.mock_client_config)
        result = loader.load_data(drive_ids=drive_ids)

        # because we have called `load_data` two times for drives
        # we're expecting the result to be 4
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0].id_, mock_data[0].id_)

    @patch.object(GoogleDriveReader, "load_data")
    def test_load_by_file_id(self, mock_load_data):
        file_ids = ["file_id_1", "file_id_2"]
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
        mock_load_data.return_value = mock_data
        loader = GoogleDriveLoader(self.mock_client_config)
        result = loader.load_data(file_ids=file_ids)

        self.assertEqual(len(result), 2)
        self.assertEqual(result, mock_data)

    @patch.object(GoogleDriveReader, "load_data")
    def test_load_from_folders_exception(self, mock_reader):
        mock_reader.return_value = []
        loader = GoogleDriveLoader(client_config=self.mock_client_config)
        mock_reader.side_effect = Exception("Test Exception")
        folder_ids = ["folder_id_1", "folder_id_2"]

        documents = loader._load_from_folders(folder_ids)
        self.assertEqual(len(documents), 0)

    @patch.object(GoogleDriveReader, "load_data")
    def test_load_from_drives_exception(self, mock_reader):
        mock_reader.return_value = []
        loader = GoogleDriveLoader(client_config=self.mock_client_config)
        mock_reader.side_effect = Exception("Test Exception")
        drives_id = ["folder_id_1", "folder_id_2"]

        documents = loader._load_from_drives(drives_id)
        self.assertEqual(len(documents), 0)

    @patch.object(GoogleDriveReader, "load_data")
    def test_load_from_files_exception(self, mock_reader):
        mock_reader.return_value = []
        loader = GoogleDriveLoader(client_config=self.mock_client_config)
        mock_reader.side_effect = Exception("Test Exception")
        file_id = ["folder_id_1", "folder_id_2"]

        documents = loader._load_from_files(file_id)
        self.assertEqual(len(documents), 0)

    @patch.object(GoogleDriveReader, "load_data")
    def test__load_by_folder_id(self, mock_get):
        folder_ids = ["file_id_1", "file_id_2"]
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
        mock_get.return_value = mock_data
        loader = GoogleDriveLoader(client_config=self.mock_client_config)
        result = loader._load_from_folders(folder_ids=folder_ids)
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0].id_, mock_data[0].id_)

    @patch.object(GoogleDriveReader, "load_data")
    def test__load_by_file_id(self, mock_load_data):
        file_ids = ["file_id_1", "file_id_2"]
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
        mock_load_data.return_value = mock_data
        loader = GoogleDriveLoader(self.mock_client_config)
        result = loader._load_from_files(file_ids=file_ids)

        self.assertEqual(len(result), 2)
        self.assertEqual(result, mock_data)
        self.assertEqual(result[0].id_, mock_data[0].id_)

    @patch.object(GoogleDriveReader, "load_data")
    def test__load_by_drive_id(self, mock_load_data):
        drive_ids = ["folder_id_1", "folder_id_2"]
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

        mock_load_data.return_value = mock_data
        loader = GoogleDriveLoader(client_config=self.mock_client_config)
        result = loader._load_from_drives(drive_ids=drive_ids)
        self.assertEqual(result[0].id_, mock_data[0].id_)

        self.assertEqual(len(result), 4)
