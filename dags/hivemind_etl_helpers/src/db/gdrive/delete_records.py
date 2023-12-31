import logging

from tc_hivemind_backend.db.pg_db_utils import convert_tuple_str
from tc_hivemind_backend.db.postgresql import PostgresSingleton


def delete_records(db_name: str, table_name: str, metadata_file_id: list[str]) -> None:
    """
    delete the records within a table of postgresql based on given file ids.
    Note: We're querying the file ids that are under metadata saved.

    Parameters
    ------------
    db_name : str
        the database to connect to
    metadata_file_id : list[str]
        the file ids which we want to delete
    """
    postgresql = PostgresSingleton(dbname=db_name)
    connection = postgresql.get_connection()

    with connection.cursor() as cursor:
        # preparing for the database query
        query_ids = convert_tuple_str(metadata_file_id)
        try:
            cursor.execute(
                f"""
                DELETE FROM {table_name}
                WHERE metadata_->>'file id' IN {query_ids};
                """
            )
        except Exception as exp:
            logging.error(f"during deleting records, exp: {exp}")
    connection.commit()
    postgresql.close_connection()
