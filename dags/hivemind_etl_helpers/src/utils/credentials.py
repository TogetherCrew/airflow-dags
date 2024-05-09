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

    redis_creds: dict[str, str] = {}

    redis_creds["port"] = os.getenv("REDIS_PORT", "")
    redis_creds["password"] = os.getenv("REDIS_PASSWORD", "")
    redis_creds["host"] = os.getenv("REDIS_HOST", "")

    return redis_creds
