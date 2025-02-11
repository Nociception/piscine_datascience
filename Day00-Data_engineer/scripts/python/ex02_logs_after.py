from get_psycopg_connection import get_psycopg_connection
import psycopg
from table_exists import table_exists
import os
from pathlib import Path
from logs_table_filler import logs_table_filler
from QueryInfo import QueryInfo
from count_rows_table import count_rows_table


def main() -> None:
    """DOCSTRING"""

    connection = None
    try:
        connection, cursor = get_psycopg_connection()
    
        logs_table = os.getenv("LOGS_TABLE")
        if not table_exists(cursor, logs_table):
            raise psycopg.OperationalError(
                f"{logs_table} table does not exist."
            )

        ex02_table = os.getenv("EX02_TABLE")
        if not table_exists(cursor, ex02_table):
            raise psycopg.OperationalError(
                f"{ex02_table} table does not exist."
            )

        logs_table_filler(
            cursor,
            QueryInfo(
                "",
                "CREATE",
                ex02_table,
                ""
            )
        )

        nb_rows_ex02_table = count_rows_table(cursor, ex02_table)
        if nb_rows_ex02_table:
            logs_table_filler(
                cursor,
                QueryInfo(
                    sql_query="",
                    modification_type="IMPORT CSV",
                    table_name=ex02_table,
                    files_involved=os.getenv("EX02_CSV_FILE")
                ),
                row_diff=nb_rows_ex02_table
            )
        else:
            print(f"{ex02_table} seems empty.")

        connection.commit()
        print("Transaction committed.")

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