import logging
from datetime import datetime, timezone

import psycopg2
from dateutil import parser
from hivemind_etl_helpers.src.utils.pg_db_utils import convert_tuple_str
from hivemind_etl_helpers.src.utils.postgres import PostgresSingleton


def setup_db(community_id: str) -> None:
    """
    Check if database is not available, then create it

    Parameters
    ------------
    community_id : str
        the guild id to create a database for
    """
    connection: psycopg2.extensions.connection
    postgres = PostgresSingleton(dbname=None)
    # first connecting to no database to check the database availability
    connection = postgres.get_connection()
    connection.autocommit = True
    try:
        cursor = connection.cursor()
        logging.info(f"Creating database community_{community_id}")
        cursor.execute(f"CREATE DATABASE community_{community_id};")
        cursor.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    except psycopg2.errors.DuplicateDatabase:
        logging.info(f"database community_{community_id} previouly created!")
    except Exception as exp:
        logging.error(f"grive database initialization: {exp}")
    finally:
        cursor.close()
        connection.close()
        postgres.close_connection()


def fetch_files_date_field(
    file_ids: list[str],
    community_id: str,
    date_field: str,
    identifier: str,
    table_name: str,
    **kwargs: dict,
) -> dict[str, datetime]:
    """
    using the file ids get their date_field field from meta data

    Parameters
    -----------
    file_ids : list[str]
        the file ids that are saved within database
    community_id : str
        Using to find out which database to connect
    date_field : str
        the date field representing that the file is updated or not
        in gdrive can be `modified_at`, discourse can be `updatedAt` and etc
    identifier : str
        the identifier of a file or post or any other possible id in any platform
    table_name : str
        the table name we want to check data for
    **kwargs : dict[str, Any]
        metadata_condition : dict[str, str]
            the metadata of documents with identifier condition as key
            and condition value as the dict value
            Note: For now we support just one condition. so there should be just one
            key and value
        identifier_type : str
            the type conversion needed to be done in getting the
            data having the identifiers.

    Returns
    ---------
    files_modified_at : dict[str, datetime]
        a dictionary which the keys are the file ids
        and values are the `date_field` date
    """
    condition_identifier: str | None = None
    condition_value: str | None = None
    if "metadata_condition" in kwargs and kwargs["metadata_condition"] is not None:
        condition = kwargs["metadata_condition"]
        condition_identifier = list(condition.keys())[0]
        condition_value = list(condition.values())[0]

    identifier_type = ""
    if "identifier_type" in kwargs:
        identifier_type = kwargs["identifier_type"]  # type: ignore

    # initializing
    results: dict[str, datetime]

    postgres = PostgresSingleton(dbname=f"community_{community_id}")
    connection = postgres.get_connection()
    connection.autocommit = True
    try:
        # preparing for the database query
        ids_string = convert_tuple_str(file_ids)

        with connection.cursor() as cursor:
            query = f"""
                SELECT jsonb_agg(result) as result
                FROM (
                    SELECT DISTINCT
                        jsonb_build_object(
                            metadata_->>'{identifier}', (metadata_->>'{date_field}')::timestamp
                        ) as result
                    FROM
                        data_{table_name}
                    WHERE
                        (metadata_->>'{identifier}'){identifier_type} IN {ids_string}
                """

            if condition_identifier is not None:
                query += f"""
                    AND metadata_->>'{condition_identifier}' = '{condition_value}'
                """
            query += ") AS distinct_results;"

            cursor.execute(query)
            results = cursor.fetchone()
            if results[0] is not None:
                # TODO: check the type of results
                results = postprocess_results(results[0])  # type: ignore
            else:
                results = {}
    except Exception as exp:
        logging.error(
            f"Raised exception while fetching documents {date_field}, exp: {exp}"
        )
        logging.warning("returning empty dict of modified files")
        results = {}
    finally:
        postgres.close_connection()

    return results


def postprocess_results(results: list[dict[str, str]]) -> dict[str, datetime]:
    """
    process a query results to the format we want

    Parameters
    ------------
    results : list[dict[str, str]]
        a list of dictionaries. each dictionary is representing
        an string key and an string value.
        Note: the values must be date in format of
        '%Y-%m-%dT%H:%M:%S' as '2023-09-26T22:29:17'

    Results
    ----------
    results_updated : dict[str, datetime]
        parsing all dicts within the list into one dict with multiple keys and values
    """
    results_updated: dict[str, datetime] = {}

    for r in results:
        for key, value in r.items():
            results_updated[key] = parser.parse(value).replace(tzinfo=timezone.utc)

    return results_updated
