import psycopg
from psycopg.sql import SQL, Literal
from sqli_detection import sqli_detection

def table_exists(
    cursor: psycopg.Cursor,
    table_name: str
) -> bool:
    """DOCSTRING"""

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
