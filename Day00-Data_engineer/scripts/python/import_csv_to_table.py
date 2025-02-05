from psycopg_connection_handler import psycopg_connection_handler

@psycopg_connection_handler(log_action=True)
def import_csv_to_table(
    table_name: str,
    csv_path: str
) -> None:
    """Imports a CSV file into a PostgreSQL table."""

    copy_query = f"""
    COPY {table_name} FROM '{csv_path}'
    DELIMITER ',' CSV HEADER;
    """
    print(f"Importing data from {csv_path} into {table_name}...")
    return copy_query