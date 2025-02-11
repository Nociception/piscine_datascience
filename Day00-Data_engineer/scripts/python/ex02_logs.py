from get_psycopg_connection import get_psycopg_connection
import psycopg
from table_exists import table_exists


def main() -> None:
    """DOCSTRING"""

    connection = None
    try:
        connection, cursor = get_psycopg_connection()
    
        if table_exists(cursor, "logs"):
            print("LOL")
        else:
            print("MDR")
        # cursor.execute(
        #     """
        #     SELECT EXISTS (
        #         SELECT FROM information_schema.tables 
        #         WHERE table_name = 'logs'
        #     );
        #     """
        # )
        # logs_exists = cursor.fetchone()[0]
        # print(logs_exists)
        # print(type(logs_exists))

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