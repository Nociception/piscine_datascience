from create_table import create_table
from logger import logger
import os


def main():
    """DOCSTRING"""

    TABLE_NAME = os.getenv("LOGS_TABLE")
    HEADERS = [
        "table_name",
        "last_modification",
        "modification_type",
        "files_involved",
        "row_diff"
    ]
    COLUMNS_TYPE = [
        "VARCHAR (50)",
        "TIMESTAMP DEFAULT now()",
        "VARCHAR (50)",
        "VARCHAR (300)",
        "BIGINT"
    ]

    create_table(
        TABLE_NAME,
        HEADERS,
        COLUMNS_TYPE
    )

    logger.info(f"Table {TABLE_NAME} is ready.")


if __name__ == "__main__":
    main()
