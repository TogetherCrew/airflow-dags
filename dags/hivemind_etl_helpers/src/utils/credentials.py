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
        "password": password,
    }
    return redis_creds
