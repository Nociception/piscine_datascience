from QueryInfo import QueryInfo
import psycopg
from pathlib import Path
from table_exists import table_exists
import os


def logs_table_filler(
    cursor: psycopg.Cursor,
    query_info: QueryInfo,
    row_diff: int = 0
) -> None:
    """DOCSTRING"""

    if query_info is None:
        print(
            "Error: QueryInfo object is none. "
            "No logging action will be done."
        )
        return

    logs_table = os.getenv("LOGS_TABLE")

    table_name = query_info.table_name
    if table_name != logs_table:
        if table_exists(cursor, logs_table):
            log_query = f"""
            INSERT INTO {logs_table} (
                table_name,
                last_modification,
                modification_type,
                files_involved,
                row_diff
            )
            VALUES (%s, now(), %s, %s, %s)
            """

            params = (
                table_name,
                query_info.modification_type,
                Path(query_info.files_involved).name
                    if query_info.files_involved
                    else None,
                row_diff
            )

            print(f"Logging action:\n{log_query}\nParams: {params}")
            cursor.execute(log_query, params)
        else:
            print(f"{logs_table} table does not exist, skipping logging.")
