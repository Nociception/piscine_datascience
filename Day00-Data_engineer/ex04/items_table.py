from import_csv_with_table_creation import import_csv_with_table_creation


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

    import_csv_with_table_creation(
        CONTAINER_CSV_DIR,
        column_types
    )


if __name__ == "__main__":
    main()
