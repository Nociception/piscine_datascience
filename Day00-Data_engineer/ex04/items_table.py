from import_csv_with_table_creation import import_csv_with_table_creation


"""
Exercice 04 : items table
Turn-in directory : ex04/
Files to turn in : items_table.*
Allowed functions : All
• You have to create the table "items"
with the same columns as in the "item.csv" file
• You have to create at least 3 data types in the table
Below is an example of the expected directory structure:
$> ls -alR
total XX
drwxrwxr-x 2 eagle eagle 4096 Fev 42 20:42 .
drwxrwxr-x 5 eagle eagle 4096 Fev 42 20:42 ..
drwxrwxr-x 2 eagle eagle 4096 Jan 42 20:42 customer
drwxrwxr-x 2 eagle eagle 4096 Jan 42 20:42 items
./customer:
...
./items:
total XX
drwxrwxr-x 2 eagle eagle 4096 Fev 42 20:42 .
drwxrwxr-x 5 eagle eagle 4096 Fev 42 20:42 ..
-rw-rw-r-- 1 eagle eagle XXXX Mar 42 20:42 items.csv
"""


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
