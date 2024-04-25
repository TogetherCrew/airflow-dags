import logging
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
            drive_ids: List of the google drive ids.
            folder_id: List of the google folder ids.
            file_ids: List of google file ids.

        Returns:
           List of loaded document objects.
        """
        loader = GoogleDriveReader()

        if folder_ids:
            logging.info("Loading documents from folders...")
            return self._load_from_folders(loader, folder_ids)
        elif drive_ids:
            logging.info("Loading documents from drives...")
            return self._load_from_drives(loader, drive_ids)
        elif file_ids:
            logging.info("Loading documents directly...")
            return self._load_from_files(loader, file_ids)
        else:
            raise ValueError("One input at least must be given!")

    def _load_from_folders(self, loader: GoogleDriveReader, folder_ids: List[str]):
        folders_data = []
        for folder_id in folder_ids:
            logging.info(f"Processing folder: {folder_id}")
            try:
                folders_data.extend(loader.load_data(folder_id=folder_id))
            except Exception as e:
                logging.error(
                    f"An error occurred while loading from folder: {e}", exc_info=True
                )
        return folders_data

    def _load_from_drives(self, loader: GoogleDriveReader, drive_ids: List[str]):
        drive_data = []
        for drive_id in drive_ids:
            logging.info(f"Processing drive: {drive_id}")
            try:
                drive_ids.extend(loader.load_data(folder_id=drive_ids))
            except Exception as e:
                logging.error(
                    f"An error occurred while loading from folder: {e}", exc_info=True
                )
        return drive_data

    def _load_from_files(self, loader: GoogleDriveReader, file_ids: List[str]):
        file_data = []
        logging.info(f"Processing file")
        try:
            file_data.extend(loader.load_data(file_ids=file_ids))
        except Exception as e:
            logging.error(
                f"An error occurred while loading from file: {e}", exc_info=True
            )
        return file_data
