import psycopg


def count_rows_table(
    cursor: psycopg.Cursor,
    table_name: str
) -> int:
    """DOCSTRING"""

    cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
    return int(cursor.fetchone()[0])
