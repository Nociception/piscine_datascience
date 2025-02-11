from get_psycopg_connection import get_psycopg_connection
import psycopg
from table_report import table_report
import os


def main() -> None:
    """DOCSTRING"""

    connection = None
    try:
        connection, cursor = get_psycopg_connection()

        table_report(cursor, os.getenv("EX02_TABLE"))


    except psycopg.OperationalError as e:
        print(f"Database connection error: {e}")
    except psycopg.ProgrammingError as e:
        print(f"Programming error in SQL query: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if connection:
            connection.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()
