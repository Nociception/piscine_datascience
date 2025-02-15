from functools import wraps
from get_psycopg_connection import get_psycopg_connection
from count_rows_table import count_rows_table
from logs_table_filler import logs_table_filler
from proceed_after_table_report import proceed_after_table_report
from psycopg.sql import SQL
from logger import logger
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
                connection, cursor = get_psycopg_connection()

                query_info = func(*args, **kwargs)
                logger.debug(query_info)

                table_name = query_info.table_name

                initial_count = 0
                if query_info.modification_type not in ("CREATE", "DROP"):
                    initial_count = count_rows_table(cursor, table_name)

                if not proceed_after_table_report(
                    cursor,
                    query_info,
                ):
                    return

                sql_string = query_info.sql_query
                if isinstance(query_info.sql_query, SQL):
                    sql_string = query_info.sql_query.as_string(cursor)

                if query_info.values:
                    flattened_values = [item for row in query_info.values for item in row]
                    cursor.execute(sql_string, flattened_values)
                else:
                    cursor.execute(sql_string)
                logger.info("Query executed.")


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
                logger.info("Transaction committed.")

                # Uncomment the following lines to debug the decorator
                # return {
                #     'query': query_info,
                #     'rows_affected': row_diff
                # }

            except psycopg.OperationalError as e:
                logger.warning(f"Database connection error: {e}")
            except psycopg.ProgrammingError as e:
                logger.error(f"Programming error in SQL query: {e}")
            except Exception as e:
                logger.error(f"An unexpected error occurred: {e}")
            finally:
                if connection:
                    connection.close()
                    logger.info("Database connection closed.")

        return wrapper
    return decorator
