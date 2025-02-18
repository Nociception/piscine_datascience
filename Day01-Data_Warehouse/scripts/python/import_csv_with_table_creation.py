# from pathlib import Path
# from create_table import create_table
# from import_csv_to_table import import_csv_to_table
# from get_all_csv_in_dir import get_all_csv_in_dir
# from logger import logger
# import os


# def import_csv_with_table_creation(
#     container_csv_dir: str,
#     column_types: list[str]
# ) -> None:
#     """
#     Automates table creation and CSV data import.

#     - Reads all CSV files in the specified directory.
#     - Extracts headers from each file.
#     - Creates corresponding tables in PostgreSQL.
#     - Imports CSV data into the created tables.

#     Args:
#         container_csv_dir (str): The directory containing CSV files.
#         column_types (list[str]): A list of column data types.
#     """

#     try:
#         csv_dir = Path(container_csv_dir).resolve()

#         for csv_file in get_all_csv_in_dir(csv_dir):
#             table_name = os.path.splitext(csv_file)[0]
#             csv_path = container_csv_dir + "/" + csv_file

#             with open(csv_dir / csv_file, "r", encoding="utf-8") as file:
#                 headers = file.readline().strip().split(",")
#                 logger.debug(f"Headers for {csv_file}: {headers}")

#             create_table(
#                 table_name,
#                 headers,
#                 column_types
#             )
#             import_csv_to_table(
#                 table_name,
#                 csv_path
#             )

#         logger.info("All CSV files have been imported successfully.")

#     except AssertionError as e:
#         logger.error(e)
