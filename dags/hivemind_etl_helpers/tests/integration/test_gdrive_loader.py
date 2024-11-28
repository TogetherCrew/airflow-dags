import os
import unittest
from unittest.mock import patch

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
        mock_load_data.return_value = mock_data
        loader = GoogleDriveLoader(refresh_token=self.refresh_token)
        result = loader.load_data(folder_ids=folder_id)
        self.assertEqual(len(result), 4)
        for i in range(4):
            self.assertEqual(result[i].id_, mock_data[i % 2].id_)

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

        # because we have called `load_data` two times for drives
        # we're expecting the result to be 4
        self.assertEqual(len(result), 4)
        for i in range(4):
            self.assertEqual(result[i].id_, mock_data[i % 2].id_)

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

        expected_results = [
            Document(
                id_="file_id_1.docx",
                metadata={
                    "file_name": "qwertU10p2.docx",
                    "file id": "qwertU10p2",
                    "author": "Jacku",
                    "file name": "Option",
                    "url": "https://drive.google.com/file/d/qwertU10p2/view",
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
                    "url": "https://drive.google.com/file/d/qwertU10p3/view",
                },
                text="Option 1: Keep it super casual",
            ),
        ]
        self.assertEqual(len(result), 2)
        self.assertEqual(result, expected_results)

    @patch.object(GoogleDriveReader, "load_data")
    def test_load_from_folders_exception(self, mock_reader):
        mock_reader.return_value = []
        loader = GoogleDriveLoader(refresh_token=self.refresh_token)
        folder_ids = ["folder_id_1", "folder_id_2"]

        documents = loader._load_from_folders(folder_ids)
        self.assertEqual(len(documents), 0)

    @patch.object(GoogleDriveReader, "load_data")
    def test_load_from_drives_exception(self, mock_reader):
        mock_reader.return_value = []
        loader = GoogleDriveLoader(refresh_token=self.refresh_token)
        drives_id = ["folder_id_1", "folder_id_2"]

        documents = loader._load_from_drives(drives_id)
        self.assertEqual(len(documents), 0)

    @patch.object(GoogleDriveReader, "load_data")
    def test_load_from_files_exception(self, mock_reader):
        mock_reader.return_value = []
        loader = GoogleDriveLoader(refresh_token=self.refresh_token)
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
        loader = GoogleDriveLoader(refresh_token=self.refresh_token)
        result = loader._load_from_folders(folder_ids=folder_ids)
        # for each folder we added the return value of 2 nodes
        # we had two folders
        for i in range(4):
            self.assertEqual(result[i].id_, mock_data[i % 2].id_)

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
        loader = GoogleDriveLoader(self.refresh_token)
        result = loader._load_from_files(file_ids=file_ids)

        self.assertEqual(len(result), 2)
        self.assertEqual(result, mock_data)
        self.assertEqual(result[0].id_, mock_data[0].id_)
        self.assertEqual(result[1].id_, mock_data[1].id_)

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
        # for each folder we added the return value of 2 nodes
        # we had two folders
        for i in range(4):
            self.assertEqual(result[i].id_, mock_data[i % 2].id_)
        self.assertEqual(len(result), 4)

    def test_transform_single_document(self):
        loader = GoogleDriveLoader(refresh_token=self.refresh_token)

        documents = [
            Document(
                doc_id=1,
                text="test",
                metadata={
                    "file id": "file_1",
                    "author": "author_1",
                    "file name": "file_name_1",
                    "mime type": "mime",
                    "created at": "date",
                    "modified at": "modified",
                },
            )
        ]
        transformed_docs = loader._transform_google_documents(documents=documents)

        self.assertEqual(len(transformed_docs), 1)
        self.assertEqual(
            transformed_docs[0].metadata,
            {
                "file id": "file_1",
                "author": "author_1",
                "file name": "file_name_1",
                "mime type": "mime",
                "created at": "date",
                "modified at": "modified",
                "url": "https://drive.google.com/file/d/file_1/view",
            },
        )

    def test_transform_multiple_document(self):
        loader = GoogleDriveLoader(refresh_token=self.refresh_token)

        documents = [
            Document(
                doc_id=1,
                text="test",
                metadata={
                    "file id": "file_1",
                    "author": "author_1",
                    "file name": "file_name_1",
                    "mime type": "mime",
                    "created at": "date",
                    "modified at": "modified",
                },
            ),
            Document(
                doc_id=2,
                text="test",
                metadata={
                    "file id": "file_2",
                    "author": "author_2",
                    "file name": "file_name_2",
                    "mime type": "mime",
                    "created at": "date",
                    "modified at": "modified",
                },
            ),
            Document(
                doc_id=3,
                text="test",
                metadata={
                    "file id": "file_3",
                    "author": "author_3",
                    "file name": "file_name_3",
                    "mime type": "mime",
                    "created at": "date",
                    "modified at": "modified",
                },
            ),
        ]
        transformed_docs = loader._transform_google_documents(documents=documents)

        self.assertEqual(len(transformed_docs), 3)
        self.assertEqual(
            transformed_docs[0].metadata,
            {
                "file id": "file_1",
                "author": "author_1",
                "file name": "file_name_1",
                "mime type": "mime",
                "created at": "date",
                "modified at": "modified",
                "url": "https://drive.google.com/file/d/file_1/view",
            },
        )

        self.assertEqual(
            transformed_docs[1].metadata,
            {
                "file id": "file_2",
                "author": "author_2",
                "file name": "file_name_2",
                "mime type": "mime",
                "created at": "date",
                "modified at": "modified",
                "url": "https://drive.google.com/file/d/file_2/view",
            },
        )

        self.assertEqual(
            transformed_docs[2].metadata,
            {
                "file id": "file_3",
                "author": "author_3",
                "file name": "file_name_3",
                "mime type": "mime",
                "created at": "date",
                "modified at": "modified",
                "url": "https://drive.google.com/file/d/file_3/view",
            },
        )
