from import_csv_with_table_creation import import_csv_with_table_creation
import psycopg


def analyze_table(
    cursor:psycopg.Cursor,
    table_name: str
) -> None:
    """
    Performs an ANALYZE on the table,
    in order to get the right rows number on adminer.
    """

    analyze_query = f"ANALYZE {table_name};"
    cursor.execute(analyze_query)


def was_vacuumed(
    cursor:psycopg.Cursor,
    table_name: str
) -> bool:
    """
    Checks if the table was vacuumed recently
    by querying pg_stat_user_tables.

    Returns a boolean depending of a recent vacuum:
    True if a recent is logged
    False otherwise

    Raises a SystemError if the query does not produces any result.
    """

    check_recent_vacuum_query = """
    SELECT last_vacuum IS NOT NULL
    FROM pg_stat_user_tables
    WHERE relname = %s;
    """
    cursor.execute(check_recent_vacuum_query, (table_name,))
    # Linked parameters are prefered here, instead of a fstring,
    # in order to prevent SQL injection.
    # Only usable when parameters do not refer to table/column names.
    
    result = cursor.fetchone()
    if result is not None:
        result = result[0]
        print(
            f"Table {table_name} has been vacuumed recently. "
            f"Vacuum skipped."
            if result else
            f"No recent vacuum."
        )
        return bool(result)

    else:
        raise SystemError("SQL query did not produce any result.")


def vacuum_table(
    cursor:psycopg.Cursor,
    table_name: str,
    full: bool
) -> None:
    """
    Performs an VACUUM (FULL depends on the `full` parameter)
    on the table, in order to get the right rows number on adminer.
    """

    if not was_vacuumed(cursor, table_name):
        full = ' FULL ' if full else ' '
        vacuum_query = f"VACUUM{full}{table_name};"
        cursor.execute(vacuum_query)
        print(f"VACUUM{full}command run on the table {table_name}.")
    else:
        print(
            f"VACUUM skipped for table` "
            f"{table_name}`: already vacuumed recently."
        )


def main():
    """
    - Defines the path to the CSV directory inside the container.
    - Specifies the column types for the table.
    - Calls the function to handle table creation and data import.
    """

    CONTAINER_CSV_DIR = "/data/customer"
    COLUMN_TYPES = [
        "TIMESTAMP WITH TIME ZONE",
        "VARCHAR(50)",
        "INT",
        "NUMERIC(10, 2)",
        "BIGINT",
        "UUID"
    ]

    import_csv_with_table_creation(
        CONTAINER_CSV_DIR,
        COLUMN_TYPES
    )


if __name__ == "__main__":
    main()
