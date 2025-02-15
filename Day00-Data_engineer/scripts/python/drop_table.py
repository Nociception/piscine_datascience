from psycopg_connection_handler import psycopg_connection_handler
from QueryInfo import QueryInfo
from sqli_detection import sqli_detection
from psycopg.sql import SQL, Identifier
from logger import logger
import sys


@psycopg_connection_handler()
def drop_table(
    table_name: str
) -> QueryInfo:
    """
    Drops (deletes) a table from the PostgreSQL database.

    Args:
        table_name (str): The name of the table to drop.

    Returns:
        QueryInfo: An object containing query metadata.
    """

    sqli_detection(table_name)

    drop_query = SQL("DROP TABLE {}").format(
        Identifier(table_name)
    )

    return QueryInfo(
        sql_query=drop_query,
        modification_type="DROP",
        table_name=table_name,
        files_involved=None
    )


def main() -> None:
    """
    Entry point to drop a table using command-line arguments.

    Expects one argument: the name of the table to drop.
    """

    if len(sys.argv) != 2:
        logger.error("Usage: python3 drop_table.py <table_name>")
        sys.exit(1)

    drop_table(table_name=sys.argv[1])


if __name__ == "__main__":
    main()
