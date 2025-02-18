from import_csvs_one_table import import_csvs_one_table
import os


def main():
    """DOCSTRING"""

    CONTAINER_CSV_DIR = "/data/customer"
    COLUMN_TYPES = [
        "TIMESTAMP WITH TIME ZONE",
        "VARCHAR(50)",
        "INT",
        "NUMERIC(10, 2)",
        "BIGINT",
        "UUID"
    ]
    TABLE_NAME = os.getenv("EX01_TABLE")

    import_csvs_one_table(
        CONTAINER_CSV_DIR,
        COLUMN_TYPES,
        TABLE_NAME
    )


if __name__ == "__main__":
    main()

