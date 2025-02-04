import os
import psycopg
import sys


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
    print(env_variables)
    assert all(env_variables.values()), (
        f"ERROR: Missing one or more environment variables.\n"
        f"env_variables:\n{env_variables}"
    )

    return env_variables


def check_import_log(
    cursor: psycopg.Cursor,
    table_name: str,
    logs_table: str
) -> bool:
    """
    Checks if a table has already been imported
    and asks the user if they want to continue.
    """

    query = f"SELECT * FROM {logs_table} WHERE table_name = %s;"
    cursor.execute(query, (table_name,))
    log_entries = cursor.fetchall()

    if log_entries:
        print(f"The table '{table_name}' has been imported before:")
        for entry in log_entries:
            print(f" - {entry[1]}")

        while True:
            user_input = input(
                "Do you want to proceed with the import? (yes/no) "
            ).strip().lower()
            if user_input in ["yes", "y", ""]:
                return True
            elif user_input in ["no", "n"]:
                return False
            else:
                print("Invalid input. Please type 'yes' or 'no'.")

    print(f"No previous log found concerning '{table_name}'.")
    return True



def main():
    """Connects to PostgreSQL and checks if an import has already been done."""

    if len(sys.argv) != 2:
        print("Usage: python check_log.py <table_name>")
        sys.exit(1)

    table_name = sys.argv[1]

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

        if not check_import_log(
            cursor,
            table_name,
            env_variables["logs_table"]
        ):
            print("Import cancelled by user.")
            sys.exit(0)

    except AssertionError as e:
        print(f"{type(e).__name__}: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)
    finally:
        if "connection" in locals():
            connection.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()
