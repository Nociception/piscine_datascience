from get_psycopg_connection import get_psycopg_connection
import psycopg
from table_exists import table_exists


def main() -> None:
    """DOCSTRING"""

    connection = None
    try:
        connection, cursor = get_psycopg_connection()
    
        if not table_exists(cursor, "logs"):
            raise psycopg.OperationalError(
                "logs table does not exist."
            )
        # LOGS EXISTS AFTER THIS STEP

        if not table_exists(cursor, ""):
            raise psycopg.OperationalError(
                "logs table does not exist."
            )


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