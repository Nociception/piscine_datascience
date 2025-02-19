from QueryInfo import QueryInfo
from psycopg.sql import SQL, Identifier
from psycopg_connection_handler import psycopg_connection_handler
from logger import logger
from sqli_detection import sqli_detection


@psycopg_connection_handler()
def rename_table(
    old: str,
    new: str
) -> QueryInfo:
    """DOCSTRING"""

    sqli_detection(old)
    sqli_detection(new)

    rename_table_query = SQL("""
        ALTER TABLE {}_sorted RENAME TO {};
        """
    ).format(
        Identifier(old),
        Identifier(new),
    )

    return QueryInfo(
        sql_query=rename_table_query,
        modification_type="RENAME",
        table_name=old
    )
