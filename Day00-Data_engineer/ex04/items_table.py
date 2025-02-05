from create_table_import_all_csv_into import create_table_import_all_csv_into


def main():
    """
    Connects to the PostgreSQL db using environment variables and
    creates tables based on CSV files.
    """

    CONTAINER_CSV_DIR = "/data/item"
    column_types = [
        "INT",
        "BIGINT",
        "VARCHAR(50)",
        "VARCHAR(50)"
    ]

    create_table_import_all_csv_into(
        CONTAINER_CSV_DIR,
        column_types
    )


if __name__ == "__main__":
    main()
