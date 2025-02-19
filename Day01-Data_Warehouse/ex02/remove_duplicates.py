import os
import psycopg


def column_exists(
    cursor: psycopg.Cursor,
    table_name: str,
    column_name: str
) -> bool:
    """
    Checks if a column exists in the specified table.

    Returns:
        bool: True if the column exists, False otherwise.
    """

    query = """
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = %s AND column_name = %s
    );
    """
    cursor.execute(query, (table_name, column_name))
    return cursor.fetchone()[0]


def add_index_column(
    cursor: psycopg.Cursor,
    table_name: str,
    column_name: str
) -> None:
    """
    Adds an index column to the specified table.
    The column will automatically assign a unique value for each row.
    """

    if column_exists(cursor, table_name, column_name):
        print(
            f"Column `{column_name}`"
            f" already exists in `{table_name}`. Skipping."
        )
        return

    try:
        print(f"Adding column `{column_name}` to `{table_name}`...")
        add_column_query = f"""
        ALTER TABLE {table_name}
        ADD COLUMN {column_name} SERIAL;
        """
        cursor.execute(add_column_query)
        print(f"Column `{column_name}` added to `{table_name}` successfully.")
    except psycopg.errors.DuplicateColumn:
        print(
            f"Column `{column_name}` already exists in"
            f" `{table_name}`. Skipping."
        )
    except Exception as e:
        print(f"An unexpected error occurred while adding column: {e}")


def sort_table_by_column(
    cursor,
    table_name: str,
    column_name: str,
    index_column_name: str
) -> None:
    """
    Reorders a PostgreSQL table based on a specified column.
    """

    if column_exists(cursor, table_name, index_column_name):
        print(f"Table `{table_name} already sorted.")
        return

    try:
        print(f"Reordering `{table_name}` by `{column_name}`...")

        create_sorted_table_query = f"""
        CREATE TABLE {table_name}_sorted AS
        SELECT *
        FROM {table_name}
        ORDER BY {column_name};
        """
        cursor.execute(create_sorted_table_query)

        drop_table_query = f"DROP TABLE {table_name};"
        cursor.execute(drop_table_query)

        rename_table_query = f"""
        ALTER TABLE {table_name}_sorted RENAME TO {table_name};
        """
        cursor.execute(rename_table_query)

        print(f"Table `{table_name}` successfully reordered by `{column_name}`.")

    except Exception as e:
        print(f"An error occurred while reordering the table: {e}")
        cursor.connection.rollback()



def remove_close_timestamp_duplicates(
    cursor: psycopg.Cursor,
    table_name: str,
    details: bool=False
) -> None:
    """
    Removes duplicates with one second delta tolerance criteria.
    This function is supposed to be called on a time sorted table.

    Uses a SQL command which does:
        a self join on the table
        in order to compare each line with
        all precedent ones: n*(n+1)/2 comparisons
        Order of column comparisons matters:
            it can lead to halves time execution.

    Initial rows number : 20,692,840

    Stats, depending the comparisons order:
        ORDER1:
            t1.product_id = t2.product_id
            AND t1.event_type = t2.event_type
            AND t1.price = t2.price
            AND t1.user_id = t2.user_id
            AND t1.user_session = t2.user_session
            AND ABS(EXTRACT(EPOCH FROM t1.event_time) - EXTRACT(EPOCH FROM t2.event_time)) <= 1
            AND t1.ctid > t2.ctid;
            
            Execution time: 62,909.513 ms
            Number of rows deleted: 1,516,182
            
        ORDER2 (same order as 1st,
            except the last column comparison (index instead of ctid)):
            t1.product_id = t2.product_id
            AND t1.event_type = t2.event_type
            AND t1.price = t2.price
            AND t1.user_id = t2.user_id
            AND t1.user_session = t2.user_session
            AND ABS(EXTRACT(EPOCH FROM t1.event_time) - EXTRACT(EPOCH FROM t2.event_time)) <= 1
            AND t1.index > t2.index;
            
            Execution time: 62,109.124 ms
            Number of rows deleted: 1,516,182

        ORDER3
            t1.index > t2.index
            AND t1.user_id = t2.user_id
            AND ABS(EXTRACT(EPOCH FROM t1.event_time) - EXTRACT(EPOCH FROM t2.event_time)) <= 1
            AND t1.product_id = t2.product_id
            AND t1.event_type = t2.event_type
            AND t1.price = t2.price
            AND t1.user_session = t2.user_session

            Execution time: 32,459.129 ms !!!!!
            Number of rows deleted: 1,516,182
    """

    query = f"""
    EXPLAIN ANALYZE
    DELETE FROM {table_name} t1
    USING {table_name} t2
    WHERE 
        t1.index > t2.index
        AND t1.user_id = t2.user_id
        AND ABS(EXTRACT(EPOCH FROM t1.event_time) - EXTRACT(EPOCH FROM t2.event_time)) <= 1
        AND t1.product_id = t2.product_id
        AND t1.event_type = t2.event_type
        AND t1.price = t2.price
        AND t1.user_session = t2.user_session
    """
    cursor.execute(query)
    print(f"Duplicates with close timestamps removed from `{table_name}`.")
    
    if details:
        explain_analyze_report = cursor.fetchall()
        print("\nDetails from EXPLAIN ANALYZE:")
        for row in explain_analyze_report:
            print(row[0])


def main() -> None:
    """
    Main function:
    - creates a test_table for testing
        the remove_close_timestamp_duplicates function
    - sorts the `table_target` table on time criteria (first column)
    - adds an index column to the `table_target` table
    - removes duplicates with an one second time delta tolerance

    Adjust the `table_target` variable to work whether on the `duplicates_deletion_table_test` table,
    or the `customers` table:
    - 't' targets the `duplicates_deletion_table_test` table
    - something else targets the `customers` table
    """

    TABLE_NAME = "customers"
    TEST_TABLE_NAME = "duplicates_deletion_test"
    INDEX_COLUMN_NAME = "index"

    try:
        env_variables = get_env_variables()
        connection = psycopg.connect(
            user=env_variables["postgres_user"],
            password=env_variables["postgres_password"],
            dbname=env_variables["postgres_db"],
            host=env_variables["postgres_host"],
            port=env_variables["postgres_port"],
        )
        cursor = connection.cursor()
        print("Connected to the database successfully.")
        print()

        sort_table_by_column(
            cursor,
            TEST_TABLE_NAME if table_target == "t" else TABLE_NAME,
            "event_time",
            INDEX_COLUMN_NAME
        )

        if table_target != "t":
            add_index_column(
                cursor,
                TABLE_NAME,
                INDEX_COLUMN_NAME
            )
        
        remove_close_timestamp_duplicates(
            cursor,
            TEST_TABLE_NAME if table_target == "t" else TABLE_NAME,
            details=True
        )

        connection.commit()

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if 'connection' in locals() and connection:
            connection.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()
