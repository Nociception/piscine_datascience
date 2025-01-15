import os
import psycopg
from dotenv import load_dotenv
from pathlib import Path


def get_env_variables() -> dict[str, str]:
    """Returns the .env variables in a dictionary."""
    dotenv_path = Path("../.env")
    assert dotenv_path.exists(), (
        f"ERROR: .env file not found at {dotenv_path.resolve()}"
    )
    print(f".env file found at {dotenv_path.resolve()}")

    load_dotenv(dotenv_path=dotenv_path)
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
        "ERROR: Missing one or more environment variables."
    )
    return env_variables


def create_table(cursor, table_name: str, headers: list[str]) -> None:
    """Create a table in PostgreSQL based on CSV headers."""
    columns = ", ".join(f"{header} TEXT" for header in headers)  # Default to TEXT
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
    print(f"Creating table {table_name} with columns: {headers}")
    cursor.execute(create_table_query)


def import_csv_to_table(cursor, table_name: str, csv_path: str) -> None:
    """Import a CSV file into a PostgreSQL table."""
    copy_query = f"""
    COPY {table_name} FROM '{csv_path}'
    DELIMITER ',' CSV HEADER;
    """
    print(f"Importing data from {csv_path} into {table_name}...")
    cursor.execute(copy_query)


def main():
    """
    Connect to the PostgreSQL db using environment variables and
    create tables based on CSV files.
    """

    LOG = 1
    HOST_CSV_DIR = "../subject/customer"  # Path relative to the script's location
    CONTAINER_CSV_DIR = "/data/customer"  # Path mounted inside the container

    try:
        # Step 1: Load environment variables
        env_variables = get_env_variables()

        # Step 2: Resolve the full path to the CSV directory
        csv_dir = Path(HOST_CSV_DIR).resolve()
        assert csv_dir.exists(), f"ERROR: CSV directory not found at {csv_dir}"
        print(f"CSV directory resolved to: {csv_dir}")

        # Step 3: List CSV files in the resolved directory
        csv_files = [
            file.name for file in csv_dir.iterdir() if file.is_file() and file.suffix == ".csv"
        ]
        if not csv_files:
            print("No CSV files found in the CSV directory.")
            return
        if LOG:
            print(f"CSV files found: {csv_files}")

        # Step 4: Connect to the PostgreSQL database
        connection = psycopg.connect(
            user=env_variables["postgres_user"],
            password=env_variables["postgres_password"],
            dbname=env_variables["postgres_db"],
            host=env_variables["postgres_host"],
            port=env_variables["postgres_port"],
        )
        cursor = connection.cursor()
        print("Connected to the database successfully.")

        # Step 5: Process each CSV file
        for csv_file in csv_files:
            table_name = os.path.splitext(csv_file)[0]
            csv_path = f"{CONTAINER_CSV_DIR}/{csv_file}"

            # Read headers from the CSV file
            with open(csv_dir / csv_file, "r", encoding="utf-8") as file:
                headers = file.readline().strip().split(",")
                if LOG:
                    print(f"Headers for {csv_file}: {headers}")

            # Create table and import data
            create_table(cursor, table_name, headers)
            import_csv_to_table(cursor, table_name, csv_path)

        connection.commit()
        print("All CSV files have been imported successfully.")

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
