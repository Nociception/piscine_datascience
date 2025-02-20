from psycopg_connection_handler import psycopg_connection_handler
from QueryInfo import QueryInfo
from psycopg.sql import SQL, Identifier


@psycopg_connection_handler()
def add_columns_table(
    table_name: str,
) -> QueryInfo:
    """DOCSTRING sqli"""

    add_columns_query = SQL("""
        ALTER TABLE {}
        ADD COLUMN 
        ADD COLUMN
        ADD COLUMN
    """).format(Identifier())

    return QueryInfo(
        sql_query=add_columns_query,
        table_name=table_name
    )
