import os
import psycopg
from pathlib import Path


def get_env_variables() -> dict[str, str]:
    """Returns the .env variables in a dictionary."""

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


def get_connection_cursor(
    env_variables: dict[str, str],
    autocommit: bool
) -> tuple[psycopg.Connection, psycopg.Cursor]:
    """
    Provides a psycopg connection cursor,
    according to the environment varibles provided.
    Autocommit depending on the `autocommit` parameter.
    """

    connection = psycopg.connect(
        user=env_variables["postgres_user"],
        password=env_variables["postgres_password"],
        dbname=env_variables["postgres_db"],
        host=env_variables["postgres_host"],
        port=env_variables["postgres_port"],
        autocommit=autocommit
    )
    cursor = connection.cursor()
    print(
        f"Connected to the database"
        f"`{env_variables['postgres_db']}` successfully."
    )
    
    return connection, cursor


def create_customers_table(
    cursor: psycopg.Cursor,
    table_name: str
) -> bool:
    """
    Creates the `customers` table if does not exist.
    Returns True if the table exists but is empty,
    indicating taht data import is needed.
    Returns False if the table already exists and is populated.
    """

    check_table_query = f"""
    SELECT EXISTS (
        SELECT FROM information_schema.tables
        WHERE table_name = '{table_name}'
    );
    """
    cursor.execute(check_table_query)
    table_exists = cursor.fetchone()[0]

    if not table_exists:
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
        print(f"Table `{table_name}` created.")

    count_query = f"SELECT COUNT(*) FROM {table_name};"
    cursor.execute(count_query)
    row_count = cursor.fetchone()[0]

    import_needed = not bool(row_count)
    print(
            f"Table `{table_name}` exists but is empty."
            f" Import is needed."
            if import_needed else
            f"Table `{table_name}` exists and is already populated."
    )
    return import_needed


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
    """Main function to join customer data into a single table."""

    CONTAINER_CSV_DIR = "/data/customer"
    TABLE_NAME = "customers"

    try:
        env_variables = get_env_variables()

        try :
            connection, cursor = get_connection_cursor(
                env_variables,
                autocommit=False
            )

            csv_dir = Path(CONTAINER_CSV_DIR).resolve()
            assert csv_dir.exists(), (
                f"ERROR: CSV directory not found at {csv_dir}"
            )
            print(f"CSV directory resolved to: {csv_dir}")

            csv_files = [
                file.name for file in csv_dir.iterdir()
                if file.is_file() and file.suffix == ".csv"
            ]
            if not csv_files:
                print(f"No CSV files found in the CSV directory {csv_dir}.")
                return
            print(f"CSV files found: {csv_files}")

            import_needed = create_customers_table(cursor, TABLE_NAME)

            if import_needed:
                for csv_file in csv_files:
                    import_csv_to_customers_table(
                        cursor,
                        os.path.join(CONTAINER_CSV_DIR, csv_file),
                        TABLE_NAME
                    )

                analyze_table(cursor,TABLE_NAME)

                print(
                    f"All CSV files have been imported into"
                    f" the `{TABLE_NAME}` table.")

            connection.commit()

        except Exception as e:
            print(f"An error occurred: {e}")

        finally:
            if 'connection' in locals() and connection:
                connection.close()
                print("Database connection closed.")

        try:
            connection, cursor = get_connection_cursor(
                env_variables,
                autocommit=True
            )

            vacuum_table(cursor, TABLE_NAME, full=True)

        except Exception as e:
            print(f"An error occurred: {e}")

        finally:
            if 'connection' in locals() and connection:
                connection.close()
                print("Database connection closed.")

    except Exception as e:
        print(f"An error occurred: {e}")
        

if __name__ == "__main__":
    main()
