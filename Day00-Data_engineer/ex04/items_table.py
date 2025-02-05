import os
import psycopg
from pathlib import Path
from get_psycopg_connection import get_psycopg_connection


def create_table(
    cursor: psycopg.Cursor,
    table_name: str,
    headers: list[str]
) -> None:
    """Create a table in PostgreSQL based on CSV headers."""

    column_types = [
        "INT",
        "BIGINT",
        "VARCHAR(50)",
        "VARCHAR(50)"
    ]
    columns = ", ".join(
        f"{header} {column_type}"
        for header, column_type in zip(headers, column_types)
    )
    print(f"columns: {columns}")

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} ({columns});
    """
    print(f"create_table_query: {create_table_query}")
    print()
    
    print(f"Creating table {table_name} with columns: {headers}")
    cursor.execute(create_table_query)


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


def main():
    """
    Connects to the PostgreSQL db using environment variables and
    creates tables based on CSV files.
    """

    CONTAINER_CSV_DIR = "/data/item"

    try:
        csv_dir = Path(CONTAINER_CSV_DIR).resolve()
        assert csv_dir.exists(), (
            f"ERROR: CSV directory not found at {csv_dir}"
        )
        print(f"CSV directory resolved to: {csv_dir}")

        csv_files = [
            file.name for file in csv_dir.iterdir()
            if file.is_file() and file.suffix == ".csv"
        ]
        if not csv_files:
            print("No CSV files found in the CSV directory.")
            return
        print(f"CSV files found: {csv_files}")

        connection = get_psycopg_connection()
        cursor = connection.cursor()
        print("Connected to the database successfully.")

        for csv_file in csv_files:
            table_name = os.path.splitext(csv_file)[0]
            csv_path = os.path.join(CONTAINER_CSV_DIR, csv_file)

            with open(
                os.path.join(csv_dir, csv_file),
                "r",
                encoding="utf-8"
            ) as file:
                headers = file.readline().strip().split(",")
                print(f"Headers for {csv_file}: {headers}")

            create_table(cursor, table_name, headers)
            import_csv_to_table(cursor, table_name, csv_path)

        connection.commit()
        print("All CSV files have been imported successfully.")

    except AssertionError as e:
        print(f"{type(e).__name__}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if "connection" in locals():
            connection.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()
