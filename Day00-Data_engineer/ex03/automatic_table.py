from import_csv_with_table_creation import import_csv_with_table_creation


def main():
    """
    Connects to the PostgreSQL db using environment variables and
    creates tables based on CSV files.
    """

    CONTAINER_CSV_DIR = "/data/customer"
    COLUMN_TYPES = [
        "TIMESTAMP WITH TIME ZONE",
        "VARCHAR(50)",
        "INT",
        "NUMERIC(10, 2)",
        "BIGINT",
        "UUID"
    ]

    import_csv_with_table_creation(
        CONTAINER_CSV_DIR,
        COLUMN_TYPES
    )


if __name__ == "__main__":
    main()
