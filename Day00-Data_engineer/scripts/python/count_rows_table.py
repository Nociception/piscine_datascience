import psycopg
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

    cursor.execute(f"""
        SELECT COUNT(*) FROM {table_name}
        """
    )
    return cursor.fetchone()[0]
