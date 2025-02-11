from QueryInfo import QueryInfo
import psycopg

def logs_table_filler(
    cursor: psycopg.Cursor,
    query_info: QueryInfo,
    row_diff: int = 0
) -> None:
    """DOCSTRING"""

    if query_info is None:
        print(
            "Error: QueryInfo object is none. "
            "No logging action will be done."
        )
        return

    table_name = query_info.table_name
    if table_name != "logs":
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'logs'
            );
            """
        )
        logs_exists = cursor.fetchone()[0]
        if logs_exists:

            log_query = f"""
            INSERT INTO logs (
                table_name,
                last_modification,
                modification_type,
                files_involved,
                row_diff
            )
            VALUES (%s, now(), %s, %s, %s)
            """

            params = (
                table_name,
                query_info.modification_type,
                query_info.files_involved
                    if query_info.files_involved
                    else None,
                row_diff
            )

            print(f"Logging action:\n{log_query}\nParams: {params}")
            cursor.execute(log_query, params)
        else:
            print("Logs table does not exist, skipping logging.")


# VALUES (
#                 '{table_name}',
#                 now(),
#                 '{query_info.modification_type}',
#                 '{query_info.files_involved}',
#                 {row_diff}
#             );