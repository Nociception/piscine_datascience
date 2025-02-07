from functools import wraps
from get_psycopg_connection import get_psycopg_connection
import psycopg


def psycopg_connection_handler():
    """DOCSTRING"""

    def decorator(func):
        """DOCSTRING"""

        @wraps(func)
        def wrapper(*args, **kwargs):
            """DOCSTRING"""

            connection = None
            try:
                connection = get_psycopg_connection()
                cursor = connection.cursor()
                print("Connected to the database successfully.")

                query_info = func(*args, **kwargs)
                table_name = query_info.table_name

                initial_count = 0
                if query_info.modification_type != "CREATE":
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                    initial_count = cursor.fetchone()[0]
                    print(f"Initial row count: {initial_count}")

                if query_info and query_info.sql_query:
                    print(f"Executing query:\n{query_info.sql_query}")
                    cursor.execute(query_info.sql_query)

                cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                final_count = cursor.fetchone()[0]
                print(f"Final row count: {final_count}")

                row_diff = final_count - initial_count
                print(f"Rows affected: {row_diff}")



                log_query = f"""
                INSERT INTO logs (table_name, last_modification, modification_type, row_diff)
                VALUES ('{table_name}', now(), '{query_info.modification_type}', {row_diff});
                """
                cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'logs'
                );
                """)
                logs_exists = cursor.fetchone()[0]

                if logs_exists:
                    print(f"Logging action:\n{log_query}")
                    cursor.execute(log_query)
                else:
                    print("Logs table does not exist, skipping logging.")



                connection.commit()
                print("Transaction committed.")
                return {
                    'query': query_info.sql_query,
                    'rows_affected': row_diff
                }

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

        return wrapper
    return decorator
