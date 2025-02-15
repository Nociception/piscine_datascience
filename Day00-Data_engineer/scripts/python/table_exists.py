from psycopg.sql import SQL, Literal
from sqli_detection import sqli_detection
import psycopg


def table_exists(
    cursor: psycopg.Cursor,
    table_name: str
) -> bool:
    """
    Checks if a specified table exists in the PostgreSQL database.

    Args:
        cursor (psycopg.Cursor): The database cursor.
        table_name (str): The name of the table to check.

    Returns:
        bool: True if the table exists, False otherwise.
    """

    sqli_detection(table_name)

    query = SQL(
        """
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = {}
        );
        """
    ).format(Literal(table_name))

    cursor.execute(query)

    return cursor.fetchone()[0]
