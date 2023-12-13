import psycopg2

from hivemind_etl_helpers.src.utils.credentials import load_postgres_credentials


class PostgresSingleton:
    _instance = None

    def __new__(cls, dbname: str | None, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls, *args, **kwargs)
            cls._instance.connect(dbname)
        return cls._instance

    def connect(self, dbname: str | None):
        creds = load_postgres_credentials()
        self.conn = psycopg2.connect(
            dbname=dbname,
            user=creds["user"],
            password=creds["password"],
            host=creds["host"],
            port=creds["port"],
        )

    def get_connection(self):
        """
        get a connection to database
        """
        return self.conn

    def close_connection(self):
        """
        will close the connection and destroy the class
        """
        self.conn.close()
        self.destroy_instance()

    @classmethod
    def destroy_instance(cls):
        cls._instance = None
