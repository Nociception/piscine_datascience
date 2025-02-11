from get_psycopg_connection import get_psycopg_connection
import psycopg


def ex02_logs() -> None:
    """DOCSTRING"""

    connection = None
    try:
        connection, cursor = get_psycopg_connection()
    
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
