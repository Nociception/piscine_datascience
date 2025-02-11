from psycopg_connection_handler import psycopg_connection_handler
from QueryInfo import QueryInfo
from pathlib import Path

@psycopg_connection_handler()
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
    
    return QueryInfo(
        sql_query=copy_query,
        modification_type="IMPORT CSV",
        table_name=table_name,
        files_involved=csv_path
    )
