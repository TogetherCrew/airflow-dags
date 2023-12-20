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

    chunk_size = os.getenv("CHUNK_SIZE")
    if chunk_size is None:
        raise ValueError("Chunk size is not given in env")
    else:
        chunk_size = int(chunk_size)

    embedding_dim = os.getenv("EMBEDDING_DIM")
    if embedding_dim is None:
        raise ValueError("Embedding dimension size is not given in env")
    else:
        embedding_dim = int(embedding_dim)

    return chunk_size, embedding_dim
