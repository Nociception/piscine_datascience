from sort_table_by_column import sort_table_by_column
import os
import psycopg


# def column_exists(
#     cursor: psycopg.Cursor,
#     table_name: str,
#     column_name: str
# ) -> bool:
#     """
#     Checks if a column exists in the specified table.

#     Returns:
#         bool: True if the column exists, False otherwise.
#     """

#     query = """
#     SELECT EXISTS (
#         SELECT 1
#         FROM information_schema.columns
#         WHERE table_name = %s AND column_name = %s
#     );
#     """
#     cursor.execute(query, (table_name, column_name))
#     return cursor.fetchone()[0]


# def add_index_column(
#     cursor: psycopg.Cursor,
#     table_name: str,
#     column_name: str
# ) -> None:
#     """
#     Adds an index column to the specified table.
#     The column will automatically assign a unique value for each row.
#     """

#     if column_exists(cursor, table_name, column_name):
#         print(
#             f"Column `{column_name}`"
#             f" already exists in `{table_name}`. Skipping."
#         )
#         return

#     try:
#         print(f"Adding column `{column_name}` to `{table_name}`...")
#         add_column_query = f"""
#         ALTER TABLE {table_name}
#         ADD COLUMN {column_name} SERIAL;
#         """
#         cursor.execute(add_column_query)
#         print(f"Column `{column_name}` added to `{table_name}` successfully.")
#     except psycopg.errors.DuplicateColumn:
#         print(
#             f"Column `{column_name}` already exists in"
#             f" `{table_name}`. Skipping."
#         )
#     except Exception as e:
#         print(f"An unexpected error occurred while adding column: {e}")


# def remove_close_timestamp_duplicates(
#     cursor: psycopg.Cursor,
#     table_name: str,
#     details: bool=False
# ) -> None:
#     """
#     Removes duplicates with one second delta tolerance criteria.
#     This function is supposed to be called on a time sorted table.

#     Uses a SQL command which does:
#         a self join on the table
#         in order to compare each line with
#         all precedent ones: n*(n+1)/2 comparisons
#         Order of column comparisons matters:
#             it can lead to halves time execution.

#     Initial rows number : 20,692,840

#     Stats, depending the comparisons order:
#         ORDER1:
#             t1.product_id = t2.product_id
#             AND t1.event_type = t2.event_type
#             AND t1.price = t2.price
#             AND t1.user_id = t2.user_id
#             AND t1.user_session = t2.user_session
#             AND ABS(EXTRACT(EPOCH FROM t1.event_time) - EXTRACT(EPOCH FROM t2.event_time)) <= 1
#             AND t1.ctid > t2.ctid;
            
#             Execution time: 62,909.513 ms
#             Number of rows deleted: 1,516,182
            
#         ORDER2 (same order as 1st,
#             except the last column comparison (index instead of ctid)):
#             t1.product_id = t2.product_id
#             AND t1.event_type = t2.event_type
#             AND t1.price = t2.price
#             AND t1.user_id = t2.user_id
#             AND t1.user_session = t2.user_session
#             AND ABS(EXTRACT(EPOCH FROM t1.event_time) - EXTRACT(EPOCH FROM t2.event_time)) <= 1
#             AND t1.index > t2.index;
            
#             Execution time: 62,109.124 ms
#             Number of rows deleted: 1,516,182

#         ORDER3
#             t1.index > t2.index
#             AND t1.user_id = t2.user_id
#             AND ABS(EXTRACT(EPOCH FROM t1.event_time) - EXTRACT(EPOCH FROM t2.event_time)) <= 1
#             AND t1.product_id = t2.product_id
#             AND t1.event_type = t2.event_type
#             AND t1.price = t2.price
#             AND t1.user_session = t2.user_session

#             Execution time: 32,459.129 ms !!!!!
#             Number of rows deleted: 1,516,182
#     """

#     query = f"""
#     EXPLAIN ANALYZE
#     DELETE FROM {table_name} t1
#     USING {table_name} t2
#     WHERE 
#         t1.index > t2.index
#         AND t1.user_id = t2.user_id
#         AND ABS(EXTRACT(EPOCH FROM t1.event_time) - EXTRACT(EPOCH FROM t2.event_time)) <= 1
#         AND t1.product_id = t2.product_id
#         AND t1.event_type = t2.event_type
#         AND t1.price = t2.price
#         AND t1.user_session = t2.user_session
#     """
#     cursor.execute(query)
#     print(f"Duplicates with close timestamps removed from `{table_name}`.")
    
#     if details:
#         explain_analyze_report = cursor.fetchall()
#         print("\nDetails from EXPLAIN ANALYZE:")
#         for row in explain_analyze_report:
#             print(row[0])


def main() -> None:
    """DOCSTRING"""

    TABLE_NAME = os.getenv("EX01_TABLE")
    INDEX_COLUMN_NAME = "index"

    sort_table_by_column(
        TABLE_NAME,
        "event_time",
    )

    # drop_table_query = f"DROP TABLE {table_name};"



    # rename_table_query = f"""
    # ALTER TABLE {table_name}_sorted RENAME TO {table_name};
    # """
    # cursor.execute(rename_table_query)

    # print(f"Table `{table_name}` successfully reordered by `{column_name}`.")



    # if table_target != "t":
    #     add_index_column(
    #         cursor,
    #         TABLE_NAME,
    #         INDEX_COLUMN_NAME
    #     )
    
    # remove_close_timestamp_duplicates(
    #     cursor,
    #     TEST_TABLE_NAME if table_target == "t" else TABLE_NAME,
    #     details=True
    # )




    # except Exception as e:
    #     print(f"An error occurred while reordering the table: {e}")
    #     cursor.connection.rollback()


if __name__ == "__main__":
    main()
