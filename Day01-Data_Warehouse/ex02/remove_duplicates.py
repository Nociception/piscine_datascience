import os
import psycopg
import dotenv
import csv
from pathlib import Path


def get_env_variables() -> dict[str, str]:
    """Returns the .env variables in a dictionary."""
    dotenv_path = Path("../.env")
    assert dotenv_path.exists(), (
        f"ERROR: .env file not found at {dotenv_path.resolve()}"
    )
    print(f".env file found at {dotenv_path.resolve()}")

    dotenv.load_dotenv(dotenv_path=dotenv_path)
    print("Step: Loading environment variables.")
    env_variables = {
        "postgres_user": os.getenv("POSTGRES_USER"),
        "postgres_password": os.getenv("POSTGRES_PASSWORD"),
        "postgres_db": os.getenv("POSTGRES_DB"),
        "postgres_host": os.getenv("POSTGRES_HOST"),
        "postgres_port": os.getenv("POSTGRES_PORT"),
    }
    print(env_variables)
    assert all(env_variables.values()), (
        f"ERROR: Missing one or more environment variables.\n"
        f"env_variables:\n{env_variables}"
    )
    return env_variables


def count_rows(
    cursor: psycopg.Cursor,
    table_name: str
) -> int:
    """Counts the number of rows in a table."""

    query = f"SELECT COUNT(*) FROM {table_name};"
    cursor.execute(query)
    result = cursor.fetchone()
    return result[0]


def calculate_expected_removals(
    cursor: psycopg.Cursor,
    table_name: str
) -> int:
    """Calculate the number of rows expected to be removed."""

    query = f"""
    SELECT 
        SUM(duplicate_count) - COUNT(*) AS expected_removals
    FROM {table_name};
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return result[0] if result else 0


def create_duplicates_table(
    cursor: psycopg.Cursor,
    table_name: str,
    target_table_name: str,
    duplicate_count_column_name: str,
    columns_name: str
) -> None:
    """Creates a table to store duplicates with their count."""

    print(f"Creating the `{table_name}` table...")
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} AS
    SELECT 
        event_time, 
        event_type, 
        product_id, 
        price, 
        user_id, 
        user_session, 
        COUNT(*) AS {duplicate_count_column_name}
    FROM {target_table_name}
    GROUP BY {columns_name}
    HAVING COUNT(*) > 1;
    """
    cursor.execute(create_table_query)
    print("`duplicates` table created.")


def remove_duplicates(
    cursor,
    table_name: str,
    columns_name: str
) -> None:
    """Remove duplicates from the `customers` table."""

    print(f"Removing duplicates from the `{table_name}` table...")
    delete_duplicates_query = f"""
    DELETE FROM {table_name}
    WHERE ctid NOT IN (
        SELECT MIN(ctid)
        FROM {table_name}
        GROUP BY {columns_name}
    );
    """
    cursor.execute(delete_duplicates_query)
    print(f"Duplicates removed from the `{table_name}` table.")


def main():
    """Main function to handle duplicate removal in the `customers` table."""

    ORIGINAL_TABLE_NAME = "customers"
    COLUMNS_NAME = ", ".join(
        [
            "event_time",
            "event_type",
            "product_id",
            "price",
            "user_id",
            "user_session"
        ]
    )
    DUPLICATES_TABLE_NAME = "duplicates"
    DUCPLICATE_COUNT_COLUMN_NAME = "duplicate_count"

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
        
        initial_row_count = count_rows(
            cursor,
            ORIGINAL_TABLE_NAME
        )
        print(
            f"Initial row count in "
            f"{ORIGINAL_TABLE_NAME}: {initial_row_count}"
        )

        create_duplicates_table(
            cursor,
            DUPLICATES_TABLE_NAME,
            ORIGINAL_TABLE_NAME,
            DUCPLICATE_COUNT_COLUMN_NAME,
            COLUMNS_NAME
        )

        # expected_removals = calculate_expected_removals(
        #     cursor,
        #     DUPLICATES_TABLE_NAME
        # )

        # remove_duplicates(
        #     cursor,
        #     ORIGINAL_TABLE_NAME,
        #     COLUMNS_NAME
        # )

        # final_row_count = count_rows(
        #     cursor,
        #     ORIGINAL_TABLE_NAME
        # )
        # print(f"Final row count in `customers`: {final_row_count}")

        # actual_diff = initial_row_count - final_row_count
        # print(f"Actual difference in row count: {actual_diff}")

        # if actual_diff == expected_removals:
        #     print("SUCCESS: The actual difference matches the expected difference.")
        # else:
        #     print("WARNING: The actual difference does not match the expected difference.")

        connection.commit()
        print("Duplicate removal process completed.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if 'connection' in locals() and connection:
            connection.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()