import psycopg, os


def get_psycopg_connection() -> tuple[psycopg.connect, psycopg.Cursor]:
    """DOCSTRING"""

    connection = psycopg.connect(
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
    )
    print("Connected to the database successfully.")

    return connection, connection.cursor()
