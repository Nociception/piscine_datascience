from functools import wraps
from get_psycopg_connection import get_psycopg_connection
import psycopg
from count_rows_table import count_rows_table
from logs_table_filler import logs_table_filler
from proceed_after_table_report import proceed_after_table_report


def psycopg_connection_handler():
    """DOCSTRING"""

    def decorator(func):
        """DOCSTRING"""

        @wraps(func)
        def wrapper(*args, **kwargs):
            """DOCSTRING"""

            connection = None
            try:
                connection, cursor = get_psycopg_connection()

                query_info = func(*args, **kwargs)
                print(query_info)

                table_name = query_info.table_name

                initial_count = 0
                if query_info.modification_type not in ("CREATE", "DROP"):
                    initial_count = count_rows_table(cursor, table_name)

                if not proceed_after_table_report(
                    cursor,
                    query_info,
                ):
                    return

                if query_info and query_info.sql_query:
                    cursor.execute(query_info.sql_query)
                    print("Query executed.")
                else:
                    raise psycopg.OperationalError(
                        "QueryInfo object, or its query attributes is None."
                    )

                final_count = 0
                if query_info.modification_type not in ("CREATE", "DROP"):
                    final_count = count_rows_table(cursor, table_name)
                row_diff = final_count - initial_count

                logs_table_filler(
                    cursor,
                    query_info,
                    row_diff
                )

                connection.commit()
                print("Transaction committed.")

                # Uncomment the following lines to debug the decorator
                # return {
                #     'query': query_info,
                #     'rows_affected': row_diff
                # }

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
