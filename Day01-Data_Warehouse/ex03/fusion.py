from import_csv_with_table_creation import import_csv_with_table_creation
# from QueryInfo import QueryInfo
# from psycopg_connection_handler import psycopg_connection_handler
# from psycopg.sql import SQL, Identifier
from deduplicate_item import deduplicate_item
from logger import logger
from analyze_table import analyze_table
from vacuum_table import vacuum_table


def main():
    # """DOCSTRING get table_name with an env variable"""

    TABLE_NAME = "item"
    CONTAINER_CSV_DIR = "/data/" + TABLE_NAME
    COLUMN_TYPES = [
        "INT",
        "BIGINT",
        "VARCHAR(50)",
        "VARCHAR(50)"
    ]

    # import_csv_with_table_creation(
    #     CONTAINER_CSV_DIR,
    #     COLUMN_TYPES
    # )
    
    # deduplicate_item(TABLE_NAME)  #  54043 rows before ANALYZE

    # analyze_table(TABLE_NAME)  # rows number unchanged

    # vacuum_table(TABLE_NAME)  # rows number unchanged

    


if __name__ == "__main__":
    main()
