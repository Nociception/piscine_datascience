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


def create_item_table(
    cursor: psycopg.Cursor,
    table_name: str
) -> bool:
    """
    Creates the `item` table if does not exist.
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
            product_id INT,
            category_id NUMERIC(20,0),
            category_code VARCHAR(50),
            brand VARCHAR(20)
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


def import_csv_into_table(
    cursor: psycopg.Cursor,
    csv_file_path: str,
    table_name: str
):
    """Import data from a CSV file into the `customers` table."""

    copy_query = f"""
    COPY {table_name} (product_id, category_id,
    category_code, brand)
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
        # raise SystemError("SQL query did not produce any result.")
        return False


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


def deduplicate_items(
    cursor: psycopg.Cursor,
    table_name: str
) -> None:
    """Creates a table `{table_name}_deduplicated` with unique `product_id`."""

    try:
        print(f"Creating `{table_name}_deduplicated` table...")

        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name}_deduplicated AS
        SELECT
            product_id,
            MAX(category_id) AS category_id,
            MAX(category_code) AS category_code,
            MAX(brand) AS brand
        FROM {table_name}
        GROUP BY product_id;
        """
        cursor.execute(query)
        print(f"Table `{table_name}_deduplicated` created successfully.")

        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        total_original = cursor.fetchone()[0]

        cursor.execute(f"SELECT COUNT(*) FROM {table_name}_deduplicated;")
        total_deduplicated = cursor.fetchone()[0]

        print(f"Rows before deduplication: {total_original}")
        print(f"Rows after deduplication: {total_deduplicated}")

    except Exception as e:
        print(f"An error occurred: {e}")


def create_test_table(
    cursor: psycopg.Cursor,
    test_table_name: str
) -> None:
    """
    Creates and populates a test table with XXXX tests cases:
    """

    table_creation_query = f"""
        DROP TABLE IF EXISTS {test_table_name};

        CREATE TABLE {test_table_name} (
            product_id INT,
            category_id NUMERIC(20,0),
            category_code VARCHAR(50),
            brand VARCHAR(20)
        );
    """
    cursor.execute(table_creation_query)

    sample_data = [
        (5712790, None, None, None),
        (5712790, 2002, None, None),
        (5712790, None, "3,3", None),
        (5712790, None, None, "4,4"),  # 4
        # (5712790, "2,2", "3,3", "4,4"),

        (5764655, 1487580005411062528, None, None),
        (5764655, 1487580005411062528, "6,3", None),
        (5764655, 1487580005411062528, None, "7,4"),  # 7
        # (5764655, 1487580005411062528, "6,3", "7,4"),

        (4958, None, "lol8,3", None),
        (4958, 9002, "lol8,3", None),
        (4958, None, "lol8,3", "10,4"),  # 10
        # (4958, "9,2", "lol8,3", "10,4"),

        (5848413, None, None, "freedecor"),
        (5848413, 12002, None, "freedecor"),
        (5848413, None, "13,3", "freedecor"),  # 13
        # (5848413, 12002, "13,3", "freedecor"),

        (5629988, 1487580009311764480, "14,3", None),
        (5629988, 1487580009311764480, None, "15,4"),  # 15
        # (5629988, 1487580009311764480, "14,3", "15,4"),
        
        
    #         5798924 | 1783999068867920640 |                         | zinger
    # 5695827 | 1487580010821713920 |                         | ingarden
    # 5590822 | 1487580006300255232 |                         | strong
        
        
        
        
    ]

    cursor.executemany(
        f"INSERT INTO {test_table_name} (product_id, category_id, category_code, brand) VALUES (%s, %s, %s, %s);",
        sample_data
    )


def main():
    """Main function to load item data into a postgres table."""

    CONTAINER_CSV_DIR = "/data/item"
    TABLE_NAME = "item"
    TEST_TABLE_NAME = "ex03_test"

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
            if not csv_files[0]:
                print(f"No CSV files found in the CSV directory {csv_dir}.")
                return
            print(f"CSV files found: {csv_files[0]}")

            import_needed = create_item_table(cursor, TABLE_NAME)

            if import_needed:
                import_csv_into_table(
                    cursor,
                    os.path.join(CONTAINER_CSV_DIR, csv_files[0]),
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

        try:
            connection, cursor = get_connection_cursor(
                env_variables,
                autocommit=True
            )

            target_table = "t"

            input_table_name = TEST_TABLE_NAME if target_table == "t" else TABLE_NAME

            create_test_table(cursor, TEST_TABLE_NAME)
            deduplicate_items(cursor, input_table_name)

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
