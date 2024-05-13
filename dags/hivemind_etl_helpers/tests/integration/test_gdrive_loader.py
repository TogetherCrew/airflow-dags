import unittest
import os
from unittest.mock import Mock, patch

from hivemind_etl_helpers.src.db.gdrive.gdrive_loader import GoogleDriveLoader
from llama_index.core.schema import Document
from llama_index.readers.google import GoogleDriveReader


class TestGoogleDriveLoader(unittest.TestCase):
    def setUp(self):
        self.refresh_token = "some_specific_token"
        os.environ["GOOGLE_CLIENT_ID"] = "some_client_id"
        os.environ["GOOGLE_CLIENT_SECRET"] = "some_client_secret"

    def test_load_data_without_input(self):
        loader = GoogleDriveLoader(refresh_token=self.refresh_token)

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
        self.mock_loader = Mock()
        mock_load_data.return_value = mock_data
        loader = GoogleDriveLoader(refresh_token=self.refresh_token)
        result = loader.load_data(folder_ids=folder_id)

        self.assertEqual(len(result), 4)
        # self.assertEqual(result, mock_data)

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
        loader = GoogleDriveLoader(refresh_token=self.refresh_token)
        result = loader.load_data(drive_ids=drive_ids)

        self.assertEqual(len(result), 4)
        # self.assertEqual(result, mock_data)

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
        loader = GoogleDriveLoader(self.refresh_token)
        result = loader.load_data(file_ids=file_ids)

        self.assertEqual(len(result), 2)
        self.assertEqual(result, mock_data)

    @patch.object(GoogleDriveReader, "load_data")
    def test_load_from_folders_exception(self, mock_reader):
        mock_loader = Mock()
        mock_reader.return_value = mock_loader
        loader = GoogleDriveLoader(refresh_token=self.refresh_token)
        mock_loader.side_effect = Exception("Test Exception")
        folder_ids = ["folder_id_1", "folder_id_2"]

        documents = loader._load_from_folders(folder_ids)
        self.assertEqual(len(documents), 0)

    @patch.object(GoogleDriveReader, "load_data")
    def test_load_from_drives_exception(self, mock_reader):
        mock_loader = Mock()
        mock_reader.return_value = mock_loader
        loader = GoogleDriveLoader(refresh_token=self.refresh_token)
        mock_loader.side_effect = Exception("Test Exception")
        drives_id = ["folder_id_1", "folder_id_2"]

        documents = loader._load_from_drives(drives_id)
        self.assertEqual(len(documents), 0)

    @patch.object(GoogleDriveReader, "load_data")
    def test_load_from_files_exception(self, mock_reader):
        mock_loader = Mock()
        mock_reader.return_value = mock_loader
        loader = GoogleDriveLoader(refresh_token=self.refresh_token)
        mock_loader.side_effect = Exception("Test Exception")
        file_id = ["folder_id_1", "folder_id_2"]

        documents = loader._load_from_files(file_id)
        self.assertEqual(len(documents), 0)

    @patch.object(GoogleDriveReader, "load_data")
    def test__load_by_folder_id(self, mock_load_data):
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
        self.mock_loader = Mock()
        mock_load_data.return_value = mock_data
        loader = GoogleDriveLoader(refresh_token=self.refresh_token)
        result = loader._load_from_folders(folder_ids=folder_id)
        self.assertEqual(len(result), 4)

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
        loader = GoogleDriveLoader(self.refresh_token)
        result = loader._load_from_files(file_ids=file_ids)

        self.assertEqual(len(result), 2)
        self.assertEqual(result, mock_data)

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
        loader = GoogleDriveLoader(self.refresh_token)
        result = loader._load_from_drives(drive_ids=drive_ids)

        self.assertEqual(len(result), 4)
