from QueryInfo import QueryInfo
from pathlib import Path
from table_exists import table_exists
from psycopg.sql import SQL, Identifier
from sqli_detection import sqli_detection
from logger import logger
import psycopg
import os


def logs_table_filler(
    cursor: psycopg.Cursor,
    query_info: QueryInfo,
    row_diff: int = 0
) -> None:
    """
    Logs database modifications in a dedicated logs table.

    Args:
        cursor (psycopg.Cursor):
            The database cursor.
        query_info (QueryInfo):
            The object containing SQL query metadata.
        row_diff (int, optional):
            The difference in row count after execution. Defaults to 0.
    """

    if query_info is None:
        logger.info(
            "QueryInfo object is none. No logging action will be done."
        )
        return

    logs_table = os.getenv("LOGS_TABLE")

    table_name = query_info.table_name
    if table_name != logs_table:
        if table_exists(cursor, logs_table):
            log_query = SQL(
                """
                    INSERT INTO {} (
                        table_name,
                        last_modification,
                        modification_type,
                        files_involved,
                        row_diff
                    )
                VALUES (%s, now(), %s, %s, %s)
                """
            ).format(Identifier(logs_table))

            params = (
                table_name,
                query_info.modification_type,
                Path(query_info.files_involved).name
                if query_info.files_involved
                else None,
                row_diff
            )

            sqli_detection(logs_table)
            sqli_detection(table_name)
            sqli_detection(query_info.modification_type)
            if query_info.files_involved is not None:
                sqli_detection(Path(query_info.files_involved).name)

            logger.debug(
                f"Logging action:\n"
                f"{log_query.as_string(cursor)}\n"
                f"Params: {params}"
            )
            cursor.execute(log_query, params)
        else:
            logger.info(
                f"{logs_table} table does not exist, skipping logging."
            )
