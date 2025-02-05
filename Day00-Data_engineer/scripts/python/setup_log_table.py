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
        "logs_table": os.getenv("LOGS_TABLE")
    }
    # print(env_variables)
    assert all(env_variables.values()), (
        f"ERROR: Missing one or more environment variables.\n"
        f"env_variables:\n{env_variables}"
    )

    return env_variables


def create_import_log_table(
    cursor: psycopg.Cursor,
    logs_table_name: str
) -> None:
    """Creates the logs table if it does not exist."""

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {logs_table_name} (
        table_name TEXT PRIMARY KEY,
        last_modification TIMESTAMP DEFAULT now(),
        modification_type VARCHAR(50)
    );
    """
    print(f"Ensuring the '{logs_table_name}' table exists...")
    cursor.execute(create_table_query)


def main():
    """Connects to PostgreSQL and ensures the 'logs' table exists."""

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

        create_import_log_table(
            cursor,
            env_variables["logs_table"]
        )

        connection.commit()
        print(f"Table {env_variables['logs_table']} is ready.")

    except AssertionError as e:
        print(f"{type(e).__name__}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if "connection" in locals():
            connection.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()
