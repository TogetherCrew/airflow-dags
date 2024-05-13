import logging
import os
from typing import List, Optional

from dotenv import load_dotenv
from llama_index.core.schema import Document
from llama_index.readers.google import GoogleDriveReader


class GoogleDriveLoader:
    def __init__(self, refresh_token: str):
        self.refresh_token = refresh_token
        client_id, client_secret = self._load_google_drive_creds()

        self.loader = GoogleDriveReader(
            authorized_user_info={
                "refresh_token": refresh_token,
                "client_id": client_id,
                "client_secret": client_secret,
            }
        )

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
            logging.info("Loading documents from folders...")
            documents.extend(self._load_from_folders(folder_ids))
        if drive_ids:
            logging.info("Loading documents from drives...")
            documents.extend(self._load_from_drives(drive_ids))
        if file_ids:
            logging.info("Loading documents directly...")
            documents.extend(self._load_from_files(file_ids))
        if not documents:
            raise ValueError("One input at least must be given!")
        return documents

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
        logging.info(f"Processing file {file_ids}")
        try:
            file_data.extend(self.loader.load_data(file_ids=file_ids))
        except Exception as e:
            logging.error(
                f"An error occurred while loading from file: {e}", exc_info=True
            )
        return file_data

    def _load_google_drive_creds(self) -> tuple[str, str]:
        """
        load google drive credentials

        Returns
        ---------
        client_id : str
            the google API client id
        client_secret : str
            google API client secrets
        """
        load_dotenv()

        client_id = os.getenv("GOOGLE_CLIENT_ID")
        client_secret = os.getenv("GOOGLE_CLIENT_SECRET")

        if client_id is None:
            raise ValueError("`GOOGLE_CLIENT_ID` not found from env variables!")
        if client_secret is None:
            raise ValueError("`GOOGLE_CLIENT_SECRET` not found from env variables!")

        return client_id, client_secret
