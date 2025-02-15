from psycopg.sql import SQL, Identifier
from table_exists import table_exists
from sqli_detection import sqli_detection
import psycopg

def count_rows_table(
    cursor: psycopg.Cursor,
    table_name: str
) -> int:
    """
    Counts the number of rows in a given table.

    Args:
        cursor (psycopg.Cursor): The database cursor.
        table_name (str): The name of the table to count rows from.

    Returns:
        int: The number of rows in the table.

    Raises:
        psycopg.OperationalError: If the table does not exist.
    """

    sqli_detection(table_name)

    if not table_exists(cursor, table_name):
        raise psycopg.OperationalError(
            f"{table_name} table does not exist."
        )

    query = SQL("SELECT COUNT(*) FROM {}").format(Identifier(table_name))
    cursor.execute(query)
    return cursor.fetchone()[0]
