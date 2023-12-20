import os

from dotenv import load_dotenv


def load_model_hyperparams() -> tuple[int, int]:
    """
    load the llm and embedding model hyperparameters (the input parameters)

    Returns
    ---------
    chunk_size : int
        the chunk size to chunk the data
    embedding_dim : int
        the embedding dimension
    """
    load_dotenv()

    chunk_size_str = os.getenv("CHUNK_SIZE")
    chunk_size: int
    if chunk_size_str is None:
        raise ValueError("Chunk size is not given in env")
    else:
        chunk_size = int(chunk_size_str)

    embedding_dim_str = os.getenv("EMBEDDING_DIM")
    embedding_dim: int
    if embedding_dim_str is None:
        raise ValueError("Embedding dimension size is not given in env")
    else:
        embedding_dim = int(embedding_dim_str)

    return chunk_size, embedding_dim
