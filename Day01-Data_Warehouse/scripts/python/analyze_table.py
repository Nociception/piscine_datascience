from sqli_detection import sqli_detection
from psycopg_connection_handler import psycopg_connection_handler
from psycopg.sql import SQL, Identifier
from QueryInfo import QueryInfo


@psycopg_connection_handler()
def analyze_table(table_name: str) -> QueryInfo:
    """
    Performs an ANALYZE on the table,
    in order to get the right rows number on adminer.
    """

    sqli_detection(table_name)

    analyze_table_query = SQL("ANALYZE {}").format(
        Identifier(table_name),
    )

    return QueryInfo(
        sql_query=analyze_table_query,
        modification_type="ANALYZE",
        table_name=table_name,
        files_involved=None
    )
