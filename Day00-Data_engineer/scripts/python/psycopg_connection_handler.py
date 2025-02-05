import psycopg
from functools import wraps
from get_psycopg_connection import get_psycopg_connection

def psycopg_connection_handler(log_action: bool = False):
    """DOCSTRING"""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            connection = None
            try:
                connection = get_psycopg_connection()
                cursor = connection.cursor()
                print("Connected to the database successfully.")

                sql_query = func(*args, **kwargs)

                if sql_query:
                    print(f"Executing query:\n{sql_query}")
                    cursor.execute(sql_query)

                # if log_action:
                #     table_name = kwargs.get('table_name', 'unknown')
                #     log_query = f"""
                #     INSERT INTO 
                #     logs
                #     (table_name, last_modification, modification_type)
                #     VALUES
                #     ('{table_name}', now(), 'INSERT');
                #     """
                #     print(f"Logging action:\n{log_query}")
                #     cursor.execute(log_query)

                connection.commit()
                print("Transaction committed.")
                return sql_query

            except AssertionError as e:
                print(f"AssertionError: {e}")
            except psycopg.Error as e:
                print(f"DatabaseError: {e}")
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
            finally:
                if connection:
                    connection.close()
                    print("Database connection closed.")

        return wrapper
    return decorator
