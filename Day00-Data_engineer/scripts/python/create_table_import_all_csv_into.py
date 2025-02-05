from pathlib import Path
from create_table_if_not_exists import create_table_if_not_exists
from import_csv_to_table import import_csv_to_table
from get_all_csv_in_dir import get_all_csv_in_dir
import os


def create_table_import_all_csv_into(
        container_csv_dir: str,
        column_types: list[str]
) -> None:
    """DOCSTRING"""

    csv_dir = Path(container_csv_dir).resolve()

    for csv_file in get_all_csv_in_dir(csv_dir):
        table_name = os.path.splitext(csv_file)[0]
        csv_path = container_csv_dir + "/" + csv_file

        with open(csv_dir / csv_file, "r", encoding="utf-8") as file:
            headers = file.readline().strip().split(",")
            print(f"Headers for {csv_file}: {headers}")

        create_table_if_not_exists(
            table_name,
            headers,
            column_types
        )
        import_csv_to_table(
            table_name,
            csv_path
        )

    print("All CSV files have been imported successfully.")
