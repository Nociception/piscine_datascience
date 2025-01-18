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


def add_index_column(cursor: psycopg.Cursor, table_name: str, column_name: str) -> None:
    """
    Adds an index column to the specified table.
    The column will automatically assign a unique value for each row.
    """
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


def main():
    """Main function to add an index column to the `customers` table."""

    print("LOL")

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

        add_index_column(cursor, "customers", "row_index")

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
