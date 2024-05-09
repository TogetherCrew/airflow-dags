import logging
from typing import List, Optional

from llama_index.core.schema import Document
from llama_index.readers.google import GoogleDriveReader


class GoogleDriveLoader:
    def __init__(self, client_config):
        self.client_config = client_config
        self.loader = GoogleDriveReader(client_config=self.client_config)

    def load_data(
        self,
        drive_ids: Optional[List[str]] = None,
        folder_ids: Optional[List[str]] = None,
        file_ids: Optional[List[str]] = None,
    ) -> List[Document]:
        """Loads documents from Google Drive.

        Args:
            drive_ids: List of the google drive ids.
            folder_id: List of the google folder ids.
            file_ids: List of google file ids.

        Returns:
           List of loaded document objects.
        """

        documents = []
        if folder_ids:
            for folder_id in folder_ids:
                logging.info("Loading documents from folders...")
                documents.extend(self._load_from_folders(folder_id))
        if drive_ids:
            for drive_id in drive_ids:
                print(drive_id)
                logging.info("Loading documents from drives...")
                documents.extend(self._load_from_drives(drive_id))
        if file_ids:
            logging.info("Loading documents from files...")
            documents.extend(self._load_from_files(file_ids))
        if not documents:
            raise ValueError("One input at least must be given!")

        return documents

    def _load_from_folders(self, folder_id: str):
        logging.info(f"Processing folder: {folder_id}")
        try:
            load_folder = self.loader.load_data(folder_id=folder_id)
            return load_folder
        except Exception as e:
            logging.error(
                f"An error occurred while loading from folder: {e}", exc_info=True
            )
            return []

    def _load_from_drives(self, drive_id: str):
        logging.info(f"Processing drive: {drive_id}")
        try:
            load_drive_data = self.loader.load_data(folder_id=drive_id)
            return load_drive_data
        except Exception as e:
            logging.error(
                f"An error occurred while loading from folder: {e}", exc_info=True
            )
            return []

    def _load_from_files(self, file_id: List[str]):
        logging.info(f"Processing file {file_id}")
        try:
            load_file_data = self.loader.load_data(file_ids=file_id)
            return load_file_data
        except Exception as e:
            logging.error(
                f"An error occurred while loading from file: {e}", exc_info=True
            )
            return []
