import psycopg

def table_exists(
    cursor: psycopg.Cursor,
    table_name: str
) -> bool:
    """DOCSTRING"""

    cursor.execute(
        f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        );
        """
    )

    return cursor.fetchone()[0]
