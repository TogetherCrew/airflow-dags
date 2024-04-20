import unittest
from unittest.mock import patch

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

    @patch.object(GoogleDriveLoader, "load_data")
    def test_load_by_folder_id(self, mock_load_data):
        folder_id = ["qwertU10p"]
        mock_docs = [
            Document(
                id_="qwertU10p2.docx",
                embedding=None,
                metadata={
                    "file_name": "qwertU10p2.docx",
                    "file id": "qwertU10p2",
                    "author": "Jacku",
                    "file name": "Option",
                    "mime type": "application/vnd.google-apps.document",
                    "created at": "2024-04-16T12:13:31.089Z",
                    "modified at": "2024-04-18T09:19:30.956Z",
                },
                excluded_embed_metadata_keys=[
                    "file_name",
                    "file_type",
                    "file_size",
                    "creation_date",
                    "last_modified_date",
                    "last_accessed_date",
                ],
                excluded_llm_metadata_keys=[
                    "file_name",
                    "file_type",
                    "file_size",
                    "creation_date",
                    "last_modified_date",
                    "last_accessed_date",
                ],
                relationships={},
                text="Option 1: Keep it super casual",
                start_char_idx=None,
                end_char_idx=None,
                text_template="{metadata_str}\n\n{content}",
                metadata_template="{key}: {value}",
                metadata_seperator="\n",
            ),
            Document(
                id_="qwertU10p3.docx",
                embedding=None,
                metadata={
                    "file_name": "qwertU10p3.docx",
                    "file id": "qwertU10p3",
                    "author": "Jacku",
                    "file name": "Option",
                    "mime type": "application/vnd.google-apps.document",
                    "created at": "2024-04-16T12:13:31.089Z",
                    "modified at": "2024-04-18T09:19:30.956Z",
                },
                excluded_embed_metadata_keys=[
                    "file_name",
                    "file_type",
                    "file_size",
                    "creation_date",
                    "last_modified_date",
                    "last_accessed_date",
                ],
                excluded_llm_metadata_keys=[
                    "file_name",
                    "file_type",
                    "file_size",
                    "creation_date",
                    "last_modified_date",
                    "last_accessed_date",
                ],
                relationships={},
                text="Option 1: Keep it super casual",
                start_char_idx=None,
                end_char_idx=None,
                text_template="{metadata_str}\n\n{content}",
                metadata_template="{key}: {value}",
                metadata_seperator="\n",
            ),
        ]
        mock_load_data.return_value = mock_docs

        result = self.loader.load_data(folder_ids=folder_id)
        self.assertEqual(result, mock_docs)

    @patch.object(GoogleDriveLoader, "load_data")
    def test_load_by_file_ids(self, mock_load_data):
        file_ids = ["qwertU10p", "qwertU10p1"]
        mock_docs = [
            Document(
                id_="qwertU10p.docx",
                embedding=None,
                metadata={
                    "file_name": "qwertU10p.docx",
                    "file id": "qwertU10p",
                    "author": "Jacku",
                    "file name": "Option",
                    "mime type": "application/vnd.google-apps.document",
                    "created at": "2024-04-16T12:13:31.089Z",
                    "modified at": "2024-04-18T09:19:30.956Z",
                },
                excluded_embed_metadata_keys=[
                    "file_name",
                    "file_type",
                    "file_size",
                    "creation_date",
                    "last_modified_date",
                    "last_accessed_date",
                ],
                excluded_llm_metadata_keys=[
                    "file_name",
                    "file_type",
                    "file_size",
                    "creation_date",
                    "last_modified_date",
                    "last_accessed_date",
                ],
                relationships={},
                text="Option 1: Keep it super casual",
                start_char_idx=None,
                end_char_idx=None,
                text_template="{metadata_str}\n\n{content}",
                metadata_template="{key}: {value}",
                metadata_seperator="\n",
            ),
            Document(
                id_="qwertU10p1.docx",
                embedding=None,
                metadata={
                    "file_name": "qwertU10p1.docx",
                    "file id": "qwertU10p1",
                    "author": "Jacku",
                    "file name": "Option",
                    "mime type": "application/vnd.google-apps.document",
                    "created at": "2024-04-16T12:13:31.089Z",
                    "modified at": "2024-04-18T09:19:30.956Z",
                },
                excluded_embed_metadata_keys=[
                    "file_name",
                    "file_type",
                    "file_size",
                    "creation_date",
                    "last_modified_date",
                    "last_accessed_date",
                ],
                excluded_llm_metadata_keys=[
                    "file_name",
                    "file_type",
                    "file_size",
                    "creation_date",
                    "last_modified_date",
                    "last_accessed_date",
                ],
                relationships={},
                text="Option 1: Keep it super casual",
                start_char_idx=None,
                end_char_idx=None,
                text_template="{metadata_str}\n\n{content}",
                metadata_template="{key}: {value}",
                metadata_seperator="\n",
            ),
        ]
        mock_load_data.return_value = mock_docs

        result = self.loader.load_data(file_ids=file_ids)

        self.assertEqual(result, mock_docs)

    @patch.object(GoogleDriveLoader, "load_data")
    def test_load_by_drive_ids(self, mock_load_data):
        drive_ids = ["qwertU10p"]
        mock_docs = [
            Document(
                id_="qwertU10p.docx",
                embedding=None,
                metadata={
                    "file_name": "qwertU10p.docx",
                    "file id": "qwertU10p",
                    "author": "Jacku",
                    "file name": "Option",
                    "mime type": "application/vnd.google-apps.document",
                    "created at": "2024-04-16T12:13:31.089Z",
                    "modified at": "2024-04-18T09:19:30.956Z",
                },
                excluded_embed_metadata_keys=[
                    "file_name",
                    "file_type",
                    "file_size",
                    "creation_date",
                    "last_modified_date",
                    "last_accessed_date",
                ],
                excluded_llm_metadata_keys=[
                    "file_name",
                    "file_type",
                    "file_size",
                    "creation_date",
                    "last_modified_date",
                    "last_accessed_date",
                ],
                relationships={},
                text="Option 1: Keep it super casual",
                start_char_idx=None,
                end_char_idx=None,
                text_template="{metadata_str}\n\n{content}",
                metadata_template="{key}: {value}",
                metadata_seperator="\n",
            ),
            Document(
                id_="qwertU10p1.docx",
                embedding=None,
                metadata={
                    "file_name": "qwertU10p1.docx",
                    "file id": "qwertU10p1",
                    "author": "Jacku",
                    "file name": "Option",
                    "mime type": "application/vnd.google-apps.document",
                    "created at": "2024-04-16T12:13:31.089Z",
                    "modified at": "2024-04-18T09:19:30.956Z",
                },
                excluded_embed_metadata_keys=[
                    "file_name",
                    "file_type",
                    "file_size",
                    "creation_date",
                    "last_modified_date",
                    "last_accessed_date",
                ],
                excluded_llm_metadata_keys=[
                    "file_name",
                    "file_type",
                    "file_size",
                    "creation_date",
                    "last_modified_date",
                    "last_accessed_date",
                ],
                relationships={},
                text="Option 1: Keep it super casual",
                start_char_idx=None,
                end_char_idx=None,
                text_template="{metadata_str}\n\n{content}",
                metadata_template="{key}: {value}",
                metadata_seperator="\n",
            ),
        ]
        mock_load_data.return_value = mock_docs

        result = self.loader.load_data(folder_ids=drive_ids)

        self.assertEqual(result, mock_docs)


if __name__ == "__main__":
    unittest.main()
