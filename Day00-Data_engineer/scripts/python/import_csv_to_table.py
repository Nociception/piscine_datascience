import psycopg


def import_csv_to_table(
    cursor: psycopg.Cursor,
    table_name: str,
    csv_path: str
) -> None:
    """Imports a CSV file into a PostgreSQL table."""

    copy_query = f"""
    COPY {table_name} FROM '{csv_path}'
    DELIMITER ',' CSV HEADER;
    """
    print(f"Importing data from {csv_path} into {table_name}...")
    cursor.execute(copy_query)
