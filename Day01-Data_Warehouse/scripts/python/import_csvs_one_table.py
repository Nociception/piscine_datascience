from pathlib import Path
from import_csv_to_table import import_csv_to_table
from get_all_csv_in_dir import get_all_csv_in_dir
from logger import logger
from create_table import create_table
from analyze_table import analyze_table
from vacuum_table import vacuum_table
import re


def piscineds_csv_sort(csv_list: list[str]) -> list[str]:
    """DOCSTRING"""

    years = dict()
    for csv_file in csv_list:
        year = re.findall(r'\d+', csv_file)[0]
        csv_file_sliced = csv_file[10:-4]
        if year in years:
            years[year].append(csv_file_sliced)
        else:
            years[year] = [csv_file_sliced]

    MONTHS_ORDER = (
        "jan",
        "feb",
        "mar",
        "apr",
        "may",
        "jun",
        "jul",
        "aug",
        "sep",
        "oct",
        "nov",
        "dec"
    )

    for year, months_list in years.items():
        sorted_months_list = []
        for month in MONTHS_ORDER:
            if month in months_list:
                sorted_months_list.append(month)
        years[year] = sorted_months_list

    sorted_csv_list = []
    for year, months in years.items():
        for month in months:
            sorted_csv_list.append(f"data_{year}_{month}.csv")

    return sorted_csv_list


def import_csvs_one_table(
    container_csv_dir: str,
    column_types: list[str],
    table_name: str
) -> None:
    """DOCSTRING"""

    csv_dir = Path(container_csv_dir).resolve()


    try:
        all_csv_in_dir = piscineds_csv_sort(get_all_csv_in_dir(csv_dir))

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

        analyze_table(table_name)

        vacuum_table(table_name, full=True)

    except AssertionError as e:
        logger.error(e)
