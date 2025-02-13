import psycopg
from psycopg.sql import SQL, Literal


def table_exists(
    cursor: psycopg.Cursor,
    table_name: str
) -> bool:
    """DOCSTRING"""

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
