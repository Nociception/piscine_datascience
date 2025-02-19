from sqli_detection import sqli_detection
from psycopg.sql import SQL, Identifier
from psycopg_connection_handler import psycopg_connection_handler
from logger import logger
from QueryInfo import QueryInfo

@psycopg_connection_handler()
def sort_table_by_column(
    table_name: str,
    column_name: str,
) -> QueryInfo:
    """DOCSTRING"""

    sqli_detection(table_name)
    sqli_detection(column_name)

    logger.info(f"Reordering `{table_name}` by `{column_name}`...")

    create_sorted_table_query = SQL("""
        CREATE TABLE {} AS
        SELECT *
        FROM {}
        ORDER BY {};
    """).format(
        Identifier(table_name + "_sorted"),
        Identifier(table_name),
        SQL(column_name)
    )

    return QueryInfo(
        sql_query=create_sorted_table_query,
        modification_type=f"SORT BY COLUMN ({column_name})",
        table_name=table_name,
    )
