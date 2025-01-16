import os
import psycopg
import dotenv
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


def create_customers_table(
    cursor: psycopg.Cursor,
    table_name: str
):
    """Creates the `customers` table."""

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        event_time TIMESTAMPTZ,
        event_type VARCHAR(50),
        product_id INT,
        price NUMERIC(10, 2),
        user_id BIGINT,
        user_session UUID
    );
    """
    cursor.execute(create_table_query)
    print(f"Table `{table_name}` created (if not exists).")


def import_csv_to_customers_table(
    cursor: psycopg.Cursor,
    csv_file_path: str,
    table_name: str
):
    """Import data from a CSV file into the `customers` table."""

    copy_query = f"""
    COPY {table_name} (event_time, event_type,
    product_id, price, user_id, user_session)
    FROM '{csv_file_path}'
    DELIMITER ','
    CSV HEADER;
    """
    cursor.execute(copy_query)
    print(f"Data from {csv_file_path} imported into `{table_name}`.")


def main():
    """Main function to join customer data into a single table."""

    HOST_CSV_DIR = "../subject/customer"
    CONTAINER_CSV_DIR = "/data/customer"
    TABLE_NAME = "customers"

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

        csv_dir = Path(HOST_CSV_DIR).resolve()
        assert csv_dir.exists(), (
            f"ERROR: CSV directory not found at {csv_dir}"
        )
        print(f"CSV directory resolved to: {csv_dir}")

        csv_files = [
            file.name for file in csv_dir.iterdir()
            if file.is_file() and file.suffix == ".csv"
        ]
        if not csv_files:
            print("No CSV files found in the CSV directory.")
            return
        print(f"CSV files found: {csv_files}")

        create_customers_table(cursor, TABLE_NAME)

        for csv_file in csv_files:
            import_csv_to_customers_table(
                cursor,
                os.path.join(CONTAINER_CSV_DIR, csv_file),
                TABLE_NAME
            )

        connection.commit()
        print("All CSV files have been imported into the `customers` table.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if 'connection' in locals() and connection:
            connection.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()
