from get_psycopg_connection import get_psycopg_connection
from table_report import table_report
from logger import logger
import psycopg
import os


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
