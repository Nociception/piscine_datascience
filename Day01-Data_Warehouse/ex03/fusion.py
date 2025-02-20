from import_csv_with_table_creation import import_csv_with_table_creation
from deduplicate_item import deduplicate_item
from logger import logger
from analyze_table import analyze_table
from vacuum_table import vacuum_table
import os
# from QueryInfo import QueryInfo
# from psycopg_connection_handler import psycopg_connection_handler
# from psycopg.sql import SQL, Identifier


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

    import_csv_with_table_creation(
        CONTAINER_CSV_DIR,
        COLUMN_TYPES
    )
    
    deduplicate_item(TABLE_NAME)  #  54043 rows before ANALYZE
    # analyze_table(TABLE_NAME)  # rows number unchanged
    # vacuum_table(TABLE_NAME)  # rows number unchanged


    # CUSTOMERS_TABLE = os.getenv("EX01_TABLE")

    # with open("/data/item", "r", encoding="utf-8") as file:
    #     ITEM_HEADERS = file.readline().strip().split(",")

    # add_columns_table(
    #     table_name=CUSTOMERS_TABLE,
    #     headers=ITEM_HEADERS,
    #     new_columns=COLUMN_TYPES[1:]
    # )


if __name__ == "__main__":
    main()
