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


def remove_duplicates(
    cursor: psycopg.Cursor,
    table_name: str,
    index_column_name: str
) -> None:
    """
    Removes duplicates from the specified table.
    Keeps the first occurrence of each duplicate group.
    """

    query = f"""
    WITH ranked_rows AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY
                       event_type,
                       product_id,
                       price,
                       user_id,
                       user_session,
                       DATE_TRUNC('second', event_time)
                   ORDER BY event_time
               ) AS row_num
        FROM {table_name}
    )
    DELETE FROM {table_name}
    WHERE {index_column_name} IN (
        SELECT {index_column_name}
        FROM ranked_rows
        WHERE row_num > 1
    );
    """
    print(f"Removing duplicates from `{table_name}`...")
    cursor.execute(query)
    print("Duplicates removed.")


def create_test_table(cursor):
    """DOCSTRING"""

    cursor.execute("""
        DROP TABLE IF EXISTS test_partition;

        CREATE TABLE test_partition (
            event_time TIMESTAMPTZ,
            event_type VARCHAR(50),
            product_id INT,
            price NUMERIC(10, 2),
            user_id BIGINT,
            user_session UUID
        );
    """)

    sample_data = [
        ("2022-11-01 00:03:14+00", "view", 5888548, 3.97, 429913900, "2f0bff3c-252f-4fe6-afcd-5d8a6a92839a"),
        ("2022-11-01 00:03:38+00", "cart", 5864286, 20.16, 565876667, "cf5d7069-7465-4ec5-a9be-c911bc1b9f95"),
        ("2022-11-01 00:03:39+00", "view", 5883844, 25.71, 445896396, "b9052fb9-7da8-4df8-a1da-eae051cd8e9c"),
        ("2022-11-01 00:03:44+00", "cart", 5674484, 15.71, 562076640, "09fafd6c-6c99-46b1-834f-33527f4de241"),
        ("2022-11-01 00:03:46+00", "remove_from_cart", 5851312, 6.33, 566239468, "3ad69360-e328-454a-b1f8-0e0901e1e344"),
        ("2022-11-01 00:03:46+00", "view", 5856190, 15.71, 562076640, "09fafd6c-6c99-46b1-834f-33527f4de241"),
        ("2022-11-01 00:03:54+00", "view", 5856189, 15.71, 562076640, "09fafd6c-6c99-46b1-834f-33527f4de241"),
        ("2022-11-01 00:04:00+00", "cart", 5815662, 0.92, 485066868, "9eeee910-722d-495e-8335-c1e3efb00089"),
        ("2022-11-01 00:04:04+00", "view", 5759382, 4.44, 556138645, "57ed222e-a54a-4907-9944-5a875c2d7f4f"),
        ("2022-11-01 00:04:21+00", "view", 5829502, 13.75, 445896396, "b9052fb9-7da8-4df8-a1da-eae051cd8e9c"),
        ("2022-11-01 00:04:29+00", "view", 5864480, 29.21, 565876667, "cf5d7069-7465-4ec5-a9be-c911bc1b9f95"),
        ("2022-11-01 00:04:33+00", "purchase", 24380, 5.24, 564451209, "861ab2f1-b2e5-886f-a93b-5b067eff081f"),
        ("2022-11-01 00:04:33+00", "purchase", 26765, 7.16, 564451209, "861ab2f1-b2e5-886f-a93b-5b067eff081f"),
        ("2022-11-01 00:04:33+00", "purchase", 5573498, 4.29, 564451209, "861ab2f1-b2e5-886f-a93b-5b067eff081f"),
        ("2022-11-01 00:04:33+00", "purchase", 5586154, 3.17, 564451209, "861ab2f1-b2e5-886f-a93b-5b067eff081f"),
        ("2022-11-01 00:04:33+00", "purchase", 5739918, 8.73, 564451209, "861ab2f1-b2e5-886f-a93b-5b067eff081f"),
        ("2022-11-01 00:04:33+00", "purchase", 5766980, 1.98, 564451209, "861ab2f1-b2e5-886f-a93b-5b067eff081f"),
        ("2022-11-01 00:04:33+00", "purchase", 5767494, 2.14, 564451209, "861ab2f1-b2e5-886f-a93b-5b067eff081f"),
        ("2022-11-01 00:04:33+00", "purchase", 5775813, 1.98, 564451209, "861ab2f1-b2e5-886f-a93b-5b067eff081f"),
        ("2022-11-01 00:04:33+00", "purchase", 5775814, 1.98, 564451209, "861ab2f1-b2e5-886f-a93b-5b067eff081f"),
        ("2022-11-01 00:04:33+00", "purchase", 5881443, 7.78, 564451209, "861ab2f1-b2e5-886f-a93b-5b067eff081f"),
        ("2022-11-01 00:04:33+00", "purchase", 5884031, 3.97, 564451209, "861ab2f1-b2e5-886f-a93b-5b067eff081f"),
        ("2022-11-01 00:04:33+00", "purchase", 9169, 4.76, 564451209, "861ab2f1-b2e5-886f-a93b-5b067eff081f"),
        ("2022-11-01 00:04:46+00", "cart", 5549786, 3.00, 566239468, "3ad69360-e328-454a-b1f8-0e0901e1e344"),
        ("2022-11-01 00:04:48+00", "view", 5763438, 9.37, 542350069, "e7890f88-28e5-4104-a4c8-f72e8c114f63"),
        ("2022-11-01 00:04:51+00", "view", 5861591, 2.22, 534488348, "25360ead-b337-47b1-a61d-e684a8491179"),
        ("2022-11-01 00:04:51+00", "view", 5763440, 9.37, 542350069, "1f2f220e-8988-4553-808a-524217633522"),
        ("2022-11-01 00:04:59+00", "remove_from_cart", 5838202, 3.49, 514649199, "dd20e3bf-021f-4633-a9c7-18b6158b2006"),
        ("2022-11-01 00:04:59+00", "view", 5870458, 10.32, 445896396, "b9052fb9-7da8-4df8-a1da-eae051cd8e9c"),
        ("2022-11-01 00:05:00+00", "remove_from_cart", 5838192, 3.49, 514649199, "dd20e3bf-021f-4633-a9c7-18b6158b2006"),
        ("2022-11-01 00:05:02+00", "view", 5896424, 32.54, 562076640, "09fafd6c-6c99-46b1-834f-33527f4de241"),
        ("2022-11-01 00:05:06+00", "view", 5838169, 3.49, 514649199, "dd20e3bf-021f-4633-a9c7-18b6158b2006"),
        ("2022-11-01 00:05:17+00", "view", 5625247, 98.40, 424460109, "1f21a6e7-5d75-4c77-adcc-1d47d3b16e15"),
        ("2022-11-01 00:05:30+00", "view", 5885630, 31.35, 556138645, "57ed222e-a54a-4907-9944-5a875c2d7f4f"),
        ("2022-11-01 00:05:44+00", "remove_from_cart", 5838169, 3.49, 514649199, "dd20e3bf-021f-4633-a9c7-18b6158b2006"),
        ("2022-11-01 00:05:49+00", "view", 5864544, 19.05, 565876667, "cf5d7069-7465-4ec5-a9be-c911bc1b9f95"),
        ("2022-11-01 00:05:54+00", "remove_from_cart", 5838150, 3.49, 514649199, "dd20e3bf-021f-4633-a9c7-18b6158b2006"),
        ("2022-11-01 00:05:55+00", "view", 5857967, 6.44, 391027139, "8b820c4f-4cee-4407-9f6a-dc0f926959bb"),
        ("2022-11-01 00:05:58+00", "view", 38005, 10.13, 566273978, "c2d3b5fa-445a-432e-bfa8-bea9652e3cfe"),
        ("2022-11-01 00:06:11+00", "view", 5549786, 3.00, 566239468, "3ad69360-e328-454a-b1f8-0e0901e1e344"),
        ("2022-11-01 00:06:22+00", "remove_from_cart", 5826980, 3.95, 556138645, "57ed222e-a54a-4907-9944-5a875c2d7f4f"),
        ("2022-11-01 00:06:22+00", "remove_from_cart", 5826980, 3.95, 556138645, "57ed222e-a54a-4907-9944-5a875c2d7f4f"),
        ("2022-11-01 00:06:24+00", "remove_from_cart", 5766772, 4.13, 556138645, "57ed222e-a54a-4907-9944-5a875c2d7f4f"),
        ("2022-11-01 00:06:34+00", "view", 5863097, 0.63, 429904458, "33bd95b0-7be4-4b05-8e7a-96225ad6873b"),
        ("2022-11-01 00:06:34+00", "view", 5864545, 19.05, 565876667, "cf5d7069-7465-4ec5-a9be-c911bc1b9f95"),
        ("2022-11-01 00:06:36+00", "view", 5893540, 7.44, 562817002, "32e737b9-7439-40b0-9011-33a0f757cdf8"),
        ("2022-11-01 00:06:42+00", "cart", 5770294, 1.59, 514649199, "dd20e3bf-021f-4633-a9c7-18b6158b2006"),
        ("2022-11-01 00:06:55+00", "view", 5747404, 6.33, 554428484, "64ccbe91-91f6-4671-b276-857dfa0e99aa"),
        ("2022-11-01 00:13:19+00", "remove_from_cart", 5749150, 0.22, 202438687, "8dc848f5-bac3-44d7-9414-75d4e599abaf"),
        ("2022-11-01 00:13:20+00", "remove_from_cart", 5749150, 0.22, 202438687, "8dc848f5-bac3-44d7-9414-75d4e599abaf"),
    ]

    cursor.executemany(
        "INSERT INTO test_partition (event_time, event_type, product_id, price, user_id, user_session) VALUES (%s, %s, %s, %s, %s, %s);",
        sample_data
    )


def visualize_partition_without_row_number(cursor):
    """
    Visualizes the effect of PARTITION BY by simulating it with GROUP BY
    without using ROW_NUMBER().
    """
    query = """
    SELECT 
        DATE_TRUNC('second', event_time) AS truncated_event_time,
        event_type,
        product_id,
        price,
        user_id,
        user_session,
        COUNT(*) AS group_size
    FROM test_partition
    GROUP BY 
        DATE_TRUNC('second', event_time),
        event_type,
        product_id,
        price,
        user_id,
        user_session
    ORDER BY truncated_event_time;
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    print("Results with GROUP BY (simulating PARTITION BY):")
    print("---------------------------------------------------")
    for row in results:
        print(row)


def remove_close_timestamp_duplicates(cursor, table_name):
    """
    DOCSTRING
    """

    query = f"""
    DELETE FROM {table_name} t1
    USING {table_name} t2
    WHERE 
        t1.product_id = t2.product_id
        AND t1.event_type = t2.event_type
        AND t1.price = t2.price
        AND t1.user_id = t2.user_id
        AND t1.user_session = t2.user_session
        AND ABS(EXTRACT(EPOCH FROM t1.event_time) - EXTRACT(EPOCH FROM t2.event_time)) <= 1
        AND t1.ctid > t2.ctid;
    """
    cursor.execute(query)
    print(f"Duplicates with close timestamps removed from `{table_name}`.")


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

        create_test_table(cursor)
        
        table = "c"
        
        reorder_table_by_column(
            cursor,
            "test_partition" if table == "t" else TABLE_NAME,
            "event_time",
            INDEX_COLUMN_NAME
        )

        add_index_column(
            cursor,
            "test_partition" if table == "t" else TABLE_NAME,
            INDEX_COLUMN_NAME
        )
        
        remove_close_timestamp_duplicates(
            cursor,
            "test_partition" if table == "t" else TABLE_NAME,
        )



        # remove_duplicates(
        #     cursor,
        #     TABLE_NAME,
        #     INDEX_COLUMN_NAME
        # )
        
        
        # visualize_partition_without_row_number(cursor)
        
        

        connection.commit()

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if 'connection' in locals() and connection:
            connection.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()
