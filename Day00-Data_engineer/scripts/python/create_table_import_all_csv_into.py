import os
from pathlib import Path
from get_psycopg_connection import get_psycopg_connection
from create_table_if_not_exists import create_table_if_not_exists
from import_csv_to_table import import_csv_to_table
from get_all_csv_in_dir import get_all_csv_in_dir


def create_table_import_all_csv_into(
    container_csv_dir: str,
    column_types: list[str]
) -> None:
    """DOCSTRING"""

    csv_dir = Path(container_csv_dir).resolve()

    try:
        connection = get_psycopg_connection()
        cursor = connection.cursor()
        print("Connected to the database successfully.")

        for csv_file in get_all_csv_in_dir(csv_dir):
            table_name = os.path.splitext(csv_file)[0]
            csv_path = os.path.join(container_csv_dir, csv_file)

            with open(
                os.path.join(csv_dir, csv_file),
                "r",
                encoding="utf-8"
            ) as file:
                headers = file.readline().strip().split(",")
                print(f"Headers for {csv_file}: {headers}")

            create_table_if_not_exists(
                cursor,
                table_name,
                headers,
                column_types
            )
            import_csv_to_table(
                cursor,
                table_name,
                csv_path
            )

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
