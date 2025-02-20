from functools import wraps
from get_psycopg_connection import get_psycopg_connection
from count_rows_table import count_rows_table
from logs_table_filler import logs_table_filler
from proceed_after_table_report import proceed_after_table_report
from logger import logger
import psycopg


def psycopg_connection_handler():
    """
    A decorator factory that manages PostgreSQL connections and transactions.

    This decorator ensures that:
    - A database connection is established
        before the decorated function runs.
    - The function executes within
        a controlled transaction.
    - The database connection is properly
        closed after execution.
    - Errors related to the database connection or SQL execution are logged.

    Returns:
        function: A decorator that wraps the function
            to handle database interactions.
    """

    def decorator(func):
        """
        A decorator that wraps a function to handle PostgreSQL connections.

        Args:
            func (function): The function to be decorated.

        Returns:
            function: A wrapper function that manages the database connection.
        """

        @wraps(func)
        def wrapper(*args, **kwargs):
            """
            Wrapper function that establishes a database connection,
                executes the decorated function,
                logs relevant information, and ensures proper cleanup.

            Steps:
            1. Establishes a PostgreSQL connection.
            2. Calls the decorated function, retrieving a `QueryInfo` object.
            3. Logs the SQL query metadata.
            4. Determines the initial row count if applicable.
            5. Checks if execution should proceed (user confirmation step).
            6. Executes the SQL query with appropriate parameters.
            7. Logs the execution result and updates the logs table.
            8. Commits the transaction and closes the connection.
            9. Handles and logs any exceptions raised during execution.

            Args:
                *args: Positional arguments for the decorated function.
                **kwargs: Keyword arguments for the decorated function.

            Raises:
                psycopg.OperationalError:
                    If there is an issue with the database connection.
                psycopg.ProgrammingError:
                    If an SQL syntax error occurs.
                Exception: For any unexpected errors.
            """

            connection = None
            try:
                connection, cursor = get_psycopg_connection()

                # QueryInfo object retrieved
                query_info = func(*args, **kwargs)
                logger.debug(query_info)

                if query_info.modification_type.startswith("VACUUM"):
                    connection.autocommit = True
                    logger.info("Autocommit enabled for VACUUM.")

                initial_count = 0
                if query_info.modification_type not in ("CREATE", "DROP"):
                    initial_count = count_rows_table(
                        cursor, 
                        query_info.table_name)

                if not proceed_after_table_report(
                    cursor,
                    query_info,
                ):
                    return

                # Query execution
                if query_info.values:
                    flattened_values = []
                    for row in query_info.values:
                        for item in row:
                            flattened_values.append(item)
                    cursor.execute(query_info.sql_query, flattened_values)
                else:
                    cursor.execute(query_info.sql_query)
                logger.info("Query executed.")
                if query_info.modification_type == "REMOVE DUPLICATES":
                    explain_analyze_report = cursor.fetchall()
                    logger.info("\nDetails from EXPLAIN ANALYZE:")
                    for row in explain_analyze_report:
                        logger.info(row[0])
                elif query_info.modification_type == "COMPARISON":
                    comparison_result = cursor.fetchall()
                    if comparison_result:
                        logger.info("\nDifferencies found between tables:")
                        for row in comparison_result:
                            logger.info(row)
                    else:
                        logger.info(
                            "\nNo differencies found between the tables."
                        )

                final_count = 0
                if query_info.modification_type not in ("CREATE", "DROP"):
                    final_count = count_rows_table(
                        cursor,
                        query_info.table_name
                    )
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
                logger.error(f"Database connection error: {e}")
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
