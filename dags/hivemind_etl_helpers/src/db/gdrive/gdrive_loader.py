import logging
from typing import List, Optional

from llama_index.core.schema import Document
from llama_index.readers.google import GoogleDriveReader


class GoogleDriveLoader:
    def __init__(self, client_config):
        self.client_config = client_config
        self.loader = GoogleDriveReader()

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

        if folder_ids:
            logging.info("Loading documents from folders...")
            return self._load_from_folders(folder_ids)
        elif drive_ids:
            logging.info("Loading documents from drives...")
            return self._load_from_drives(drive_ids)
        elif file_ids:
            logging.info("Loading documents directly...")
            return self._load_from_files(file_ids)
        else:
            raise ValueError("One input at least must be given!")

    def _load_from_folders(self, folder_ids: List[str]):
        folders_data = []
        for folder_id in folder_ids:
            logging.info(f"Processing folder: {folder_id}")
            try:
                folders_data.extend(self.loader.load_data(folder_id=folder_id))
            except Exception as e:
                logging.error(
                    f"An error occurred while loading from folder: {e}", exc_info=True
                )
        return folders_data

    def _load_from_drives(self, drive_ids: List[str]):
        drive_data = []
        for drive_id in drive_ids:
            logging.info(f"Processing drive: {drive_id}")
            try:
                drive_data.extend(self.loader.load_data(folder_id=drive_id))
            except Exception as e:
                logging.error(
                    f"An error occurred while loading from folder: {e}", exc_info=True
                )
        return drive_data

    def _load_from_files(self, file_ids: List[str]):
        file_data = []
        logging.info(f"Processing files: {file_ids}")
        try:
            file_data.extend(self.loader.load_data(file_ids=file_ids))
        except Exception as e:
            logging.error(
                f"An error occurred while loading from file: {e}", exc_info=True
            )
        return file_data
