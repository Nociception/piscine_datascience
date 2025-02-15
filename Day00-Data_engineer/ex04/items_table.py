from import_csv_with_table_creation import import_csv_with_table_creation


def main():
    """
    - Defines the path to the CSV directory inside the container.
    - Specifies the column types for the items table.
    - Calls the function to handle table creation and data import.
    """

    CONTAINER_CSV_DIR = "/data/item"
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


if __name__ == "__main__":
    main()
