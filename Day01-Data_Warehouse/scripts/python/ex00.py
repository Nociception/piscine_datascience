from create_table import create_table
from datetime import datetime
from insert_rows import insert_rows
import os


"""
Exercice 00 : Create postgres DB
Turn-in directory : ex00/
Files to turn in : None
Allowed functions : All
For this exercise you can use directly postgres
if installed in your campus or if you go
through a VM, otherwise you have to use docker compose.
• The username is your student login
• The name of the DB is piscineds
• The password is "mysecretpassword"
We must be able to connect to your posgress database with this command:
psql -U your_login -d piscineds -h localhost -W
mysecretpassword
piscineds=#

Exercice 01 : Show me your DB
Turn-in directory : ex01/
Files to turn in :
Allowed functions : pgadmin, Postico, dbeaver or what you want to see the db
easily
• Find a way to visualize the db easily with a software
• The chosen software must help you to easily find and
manipulate data using its owncorresponding ID
"""


def main():
    """
    Creates a table and inserts predefined rows into it.

    - Reads table name from environment variable.
    - Defines column headers and column types.
    - Creates the table.
    - Inserts a predefined dataset into the table.
    """

    TABLE_NAME = os.getenv("EX00_TABLE")

    HEADERS = [
        "event_time",
        "event_type",
        "product_id",
        "category_id",
        "category_code",
        "brand",
        "price",
        "user_id",
        "user_session",
    ]

    COLUMN_TYPES = [
        "TIMESTAMP NOT NULL",
        "VARCHAR(50)",
        "BIGINT",
        "NUMERIC",
        "VARCHAR(255)",
        "VARCHAR(255)",
        "NUMERIC(10, 2)",
        "BIGINT",
        "VARCHAR(255)"
    ]

    create_table(
        TABLE_NAME,
        HEADERS,
        COLUMN_TYPES
    )

    ROWS = [
        (datetime(2022, 10, 1, 0, 0, 0), 'cart', 5773203, 1487580005134238464, None, 'runail', 2.62, 463240011, '26dd6e6e-4dac-4778-8d2c-92e149dab885'),  # noqa: E501
        (datetime(2022, 10, 1, 0, 0, 3), 'cart', 5773353, 1487580005134238464, None, 'runail', 2.62, 463240011, '26dd6e6e-4dac-4778-8d2c-92e149dab885'),  # noqa: E501
        (datetime(2022, 10, 1, 0, 0, 7), 'cart', 5723490, 1487580005134238464, None, 'runail', 2.62, 463240011, '26dd6e6e-4dac-4778-8d2c-92e149dab885'),  # noqa: E501
        (datetime(2022, 10, 1, 0, 0, 7), 'cart', 5881589, 215119107051219712, None, 'lovely', 13.48, 429681830, '49e8d843-adf3-428b-a2c3-fe8bc6a307c9'),  # noqa: E501
        (datetime(2022, 10, 1, 0, 0, 15), 'cart', 5881449, 148758000513522845952, None, 'lovely', 0.56, 429681830, '49e8d843-adf3-428b-a2c3-fe8bc6a307c9'),  # noqa: E501
        (datetime(2022, 10, 1, 0, 0, 16), 'cart', 5857269, 1487580005134238464, None, 'runail', 2.62, 430174032, '73dea1e7-664e-43f4-8b30-d32b9d5af04f')  # noqa: E501
    ]

    insert_rows(
        TABLE_NAME,
        HEADERS,
        ROWS,
        os.getenv("EX00_PY")
    )


if __name__ == "__main__":
    main()
