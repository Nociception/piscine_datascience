from get_psycopg_connection import get_psycopg_connection
from table_report import table_report
from logger import logger
import psycopg
import os


"""
Exercice 02 : First table
Turn-in directory : ex02/
Files to turn in : table.*
Allowed functions : All
• Create a postgres table using the data from a CSV from the `customer` folder.
Name the tables according to the CSV’s name but without the file extension, for
example : "data_2022_oct"
• The name of the columns must be the same as the one in the CSV files and have
the appropriate type, beware you should have at least 6 different data types
• A DATETIME as the first column is mandatory
Be careful, the typings are not quite the same as under Maria DB
"""


def main() -> None:
    """Executes a report generation for a specific table."""

    connection = None
    try:
        connection, cursor = get_psycopg_connection()

        table_report(cursor, os.getenv("EX02_TABLE"))

    except psycopg.OperationalError as e:
        logger.error(f"Database connection error: {e}")
    except psycopg.ProgrammingError as e:
        logger.error(f"Programming error in SQL query: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        if connection:
            connection.close()
            logger.info("Database connection closed.")


if __name__ == "__main__":
    main()
