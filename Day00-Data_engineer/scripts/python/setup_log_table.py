from create_table_if_not_exists import create_table_if_not_exists
import os


def main():
    """Connects to PostgreSQL and ensures the 'logs' table exists."""

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

    create_table_if_not_exists(
        TABLE_NAME,
        HEADERS,
        COLUMNS_TYPE
    )

    print(f"Table {TABLE_NAME} is ready.")


if __name__ == "__main__":
    main()
