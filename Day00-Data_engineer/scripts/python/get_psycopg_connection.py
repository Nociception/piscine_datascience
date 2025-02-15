from logger import logger
import psycopg
import os


def get_psycopg_connection() -> tuple[psycopg.connect, psycopg.Cursor]:
    """
    Establishes and returns a connection to the PostgreSQL database.

    Returns:
        tuple[psycopg.connect, psycopg.Cursor]:
            The database connection and cursor.
    """

    connection = psycopg.connect(
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
    )
    logger.info("Connected to the database successfully.")

    return connection, connection.cursor()
