from typing import List, Optional

from llama_index.core.schema import Document
from llama_index.readers.google import GoogleDriveReader


class GoogleDriveLoader:
    def __init__(self, client_config):
        self.client_config = client_config

    def load_data(
        self,
        drive_ids: Optional[List[str]] = None,
        folder_ids: Optional[List[str]] = None,
        file_ids: Optional[List[str]] = None,
    ) -> List[Document]:
        """Loads documents from Google Drive.

        Args:
            drive_ids: List of the google drive drive ids.
            folder_id: List of the google drive folder ids.
            file_ids: List of google drive file ids.

        Returns:
           List of loaded document objects.
        """

        loader = GoogleDriveReader(self.client_config)

        all_docs = []
        if folder_ids:
            for folder_id in folder_ids:
                all_docs.extend(loader.load_data(folder_id=folder_id))
        elif drive_ids:
            for drive_id in drive_ids:
                for drive_id in drive_id:
                    all_docs.extend(loader.load_data(folder_id=drive_id))
        elif file_ids:
            all_docs = loader.load_data(file_ids=file_ids)
        else:
            raise ValueError("One input at least must be given!")

        return all_docs
