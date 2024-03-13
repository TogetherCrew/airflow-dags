from llama_index.core import Document, download_loader


def retrieve_folder_documents(folder_id: str) -> list[Document]:
    """
    retrieve content of a folder

    Parameters
    -----------
    folder_id : str
        the id for the folder we want to extract data from
        Note: the folder should have view access

    Returns
    ---------
    gdrive_docs : list[llama_index.Document]
        a list of retrieve documents
    """
    GoogleDriveReader = download_loader("GoogleDriveReader")
    loader = GoogleDriveReader()
    gdrive_docs = loader.load_data(folder_id=folder_id)

    return gdrive_docs


def retrieve_file_documents(file_ids: list[str]) -> list[Document]:
    """
    retrieve content of multiple files within drive

    Parameters
    -----------
    file_ids : str
        the folder we want to extract data from

    Returns
    ---------
    gdrive_docs : list[llama_index.Document]
        a list of retrieve documents
    """
    GoogleDriveReader = download_loader("GoogleDriveReader")
    loader = GoogleDriveReader()
    gdrive_docs = loader.load_data(file_ids=file_ids)

    return gdrive_docs
