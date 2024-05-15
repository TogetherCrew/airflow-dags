import os

from dotenv import load_dotenv


def load_mongo_credentials() -> dict[str, str]:
    """
    load mongo db credentials from .env

    Returns:
    ---------
    mongo_creds : dict[str, Any]
        mongodb credentials
        a dictionary representive of
            `user`: str
            `password` : str
            `host` : str
            `port` : int
    """
    load_dotenv()

    mongo_creds = {}

    mongo_creds["user"] = os.getenv("MONGODB_USER", "")
    mongo_creds["password"] = os.getenv("MONGODB_PASS", "")
    mongo_creds["host"] = os.getenv("MONGODB_HOST", "")
    mongo_creds["port"] = os.getenv("MONGODB_PORT", "")

    return mongo_creds


def load_redis_credentials() -> dict[str, str]:
    """
    load redis db credentials from .env

    Returns:
    ---------
    redis_creds : dict[str, Any]
        redis credentials
        a dictionary representive of
            `password` : str
            `host` : str
            `port` : int
    """
    load_dotenv()

    host = os.getenv("REDIS_HOST")
    port = os.getenv("REDIS_PORT")
    password = os.getenv("REDIS_PASSWORD")

    if host is None:
        raise ValueError("`REDIS_HOST` is not set in env credentials!")
    if port is None:
        raise ValueError("`REDIS_PORT` is not set in env credentials!")
    if password is None:
        raise ValueError("`REDIS_PASSWORD` is not set in env credentials!")

    redis_creds: dict[str, str] = {
        "host": host,
        "port": port,
        "pasword": password,
    }
    return redis_creds


def load_qdrandt_credentials() -> dict[str, str]:
    """
    load qdrant database credentials

    Returns:
    ---------
    qdrant_creds : dict[str, Any]
        redis credentials
        a dictionary representive of
            `api_key` : str
            `host` : str
            `port` : int
    """
    load_dotenv()

    qdrant_creds: dict[str, str] = {}

    host = os.getenv("QDRANT_HOST")
    port = os.getenv("QDRANT_PORT")
    api_key = os.getenv("QDRANT_API_KEY")

    if host is None:
        raise ValueError("`QDRANT_HOST` is not set in env credentials!")
    if port is None:
        raise ValueError("`QDRANT_PORT` is not set in env credentials!")
    if api_key is None:
        raise ValueError("`QDRANT_API_KEY` is not set in env credentials!")

    qdrant_creds = {
        "host": host,
        "port": port,
        "api_key": api_key,
    }
    return qdrant_creds
