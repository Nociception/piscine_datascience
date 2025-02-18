from sqli_detection import sqli_detection
from psycopg_connection_handler import psycopg_connection_handler
from psycopg.sql import SQL, Identifier, Literal
from QueryInfo import QueryInfo


@psycopg_connection_handler()
def vacuum_table(
    table_name: str,
    full: bool
) -> None:
    """
    Performs an VACUUM (FULL depends on the `full` parameter)
    on the table, in order to get the right rows number on adminer.
    """

    sqli_detection(table_name)
    if not isinstance(full, bool):
        raise TypeError("`full` must be a bool.")

    full = " FULL" if full else ""
    vacuum_query = SQL("VACUUM {} {}").format(
        SQL(full), Identifier(table_name)
    )

    return QueryInfo(
        sql_query=vacuum_query,
        modification_type=f"VACUUM{full}",
        table_name=table_name,
        files_involved=None
    )



