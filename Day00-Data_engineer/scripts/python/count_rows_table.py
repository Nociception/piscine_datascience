import psycopg
from psycopg.sql import SQL, Identifier
from table_exists import table_exists


def count_rows_table(
    cursor: psycopg.Cursor,
    table_name: str
) -> int:
    """DOCSTRING"""

    if not table_exists(cursor, table_name):
        raise psycopg.OperationalError(
            f"{table_name} table does not exist."
        )

    query = SQL("SELECT COUNT(*) FROM {}").format(Identifier(table_name))
    cursor.execute(query)
    return cursor.fetchone()[0]
