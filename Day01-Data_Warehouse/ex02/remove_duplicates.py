import os
import psycopg


def get_env_variables() -> dict[str, str]:
    """Returns the .env variables in a dictionary."""

    env_variables = {
        "postgres_user": os.getenv("POSTGRES_USER"),
        "postgres_password": os.getenv("POSTGRES_PASSWORD"),
        "postgres_db": os.getenv("POSTGRES_DB"),
        "postgres_host": os.getenv("POSTGRES_HOST"),
        "postgres_port": os.getenv("POSTGRES_PORT"),
    }
    assert all(env_variables.values()), (
        f"ERROR: Missing one or more environment variables.\n"
        f"env_variables:\n{env_variables}"
    )

    return env_variables


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
        print(f"Column `{column_name}` already exists in `{table_name}`. Skipping.")
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
        print(f"Column `{column_name}` already exists in `{table_name}`. Skipping.")
    except Exception as e:
        print(f"An unexpected error occurred while adding column: {e}")


def truncate_event_time_to_tens(cursor, table_name: str) -> None:
    """
    Displays the effect of truncating timestamps to the nearest ten seconds.
    """
    query = f"""
    SELECT 
        event_time - INTERVAL '1 second' * (EXTRACT(SECOND FROM event_time) % 10) AS truncated_event_time,
        event_type,
        product_id,
        price,
        user_id,
        user_session
    FROM {table_name}
    LIMIT 10;
    """
    cursor.execute(query)
    result = cursor.fetchall()
    
    print("Truncated event_time to nearest ten seconds:")
    for row in result:
        print(row)


def reorder_table_by_column(
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


def main() -> None:
    """Main function to add an index column to the `customers` table."""

    TABLE_NAME = "customers"
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

        reorder_table_by_column(
            cursor,
            TABLE_NAME,
            "event_time",
            INDEX_COLUMN_NAME
        )

        add_index_column(
            cursor,
            "customers",
            INDEX_COLUMN_NAME
        )

        # truncate_event_time_to_tens(
        #     cursor,
        #     TABLE_NAME            
        # )

        connection.commit()
        print("Index column added successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if 'connection' in locals() and connection:
            connection.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()
