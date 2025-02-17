from import_csv_with_table_creation import import_csv_with_table_creation


"""
Exercice 03 : automatic table
Turn-in directory : ex03/
Files to turn in : automatic_table.*
Allowed functions : All
• We are at the end of February 2022,
you should be able to create tables with data extracted from a CSV.
• Now, in addition, retrieve all the CSV from the `customer`
folder automatically and name the tables according to the CSV's name
but without the file extension, for example : "data_2022_oct"
Below is an example of the expected directory structure:
$> ls -alR
total XX
drwxrwxr-x 2 eagle eagle 4096 Fev 42 20:42 .
drwxrwxr-x 5 eagle eagle 4096 Fev 42 20:42 ..
drwxrwxr-x 2 eagle eagle 4096 Jan 42 20:42 customer
drwxrwxr-x 2 eagle eagle 4096 Jan 42 20:42 items
./customer:
total XX
drwxrwxr-x 2 eagle eagle 4096 Fev 42 20:42 .
drwxrwxr-x 5 eagle eagle 4096 Fev 42 20:42 ..
-rw-rw-r-- 1 eagle eagle XXXX Mar 42 20:42 data_2022_dec.csv
-rw-rw-r-- 1 eagle eagle XXXX Mar 42 20:42 data_2022_nov.csv
-rw-rw-r-- 1 eagle eagle XXXX Mar 42 20:42 data_2022_oct.csv
-rw-rw-r-- 1 eagle eagle XXXX Mar 42 20:42 data_2023_jan.csv
./items:
...
"""


def main():
    """
    - Defines the path to the CSV directory inside the container.
    - Specifies the column types for the table.
    - Calls the function to handle table creation and data import.
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
