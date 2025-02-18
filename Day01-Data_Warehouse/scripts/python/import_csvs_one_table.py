from pathlib import Path
from import_csv_to_table import import_csv_to_table
from get_all_csv_in_dir import get_all_csv_in_dir
from logger import logger
from create_table import create_table


def import_csvs_one_table(
    container_csv_dir: str,
    column_types: list[str],
    table_name: str
) -> None:
    """DOCSTRING"""

    csv_dir = Path(container_csv_dir).resolve()

    try:
        all_csv_in_dir = get_all_csv_in_dir(csv_dir)
        first_csv_file = all_csv_in_dir[0]

        with open(csv_dir / first_csv_file, "r", encoding="utf-8") as file:
            headers = file.readline().strip().split(",")
            logger.debug(f"Headers for {first_csv_file}: {headers}")
    
        create_table(
            table_name,
            headers,
            column_types
        )




        for csv_file in all_csv_in_dir:
            csv_path = container_csv_dir + "/" + csv_file
            import_csv_to_table(
                table_name,
                csv_path
            )




        logger.info(
        f"All CSV files have been imported"
        f" successfully into {table_name}."
        )

    except AssertionError as e:
        logger.error(e)
