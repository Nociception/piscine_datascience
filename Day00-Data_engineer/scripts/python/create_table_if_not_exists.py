import psycopg

def create_table_if_not_exists(
    cursor: psycopg.Cursor,
    table_name: str,
    headers: list[str],
    column_types: list[str],
    logs_table: bool=False
) -> None:
    """
    Create a table (if it does not exist) in PostgreSQL,
    according to the args.
    """

    columns = ", ".join(
        f"{header} {column_type}"
        for header, column_type in zip(headers, column_types)
    )
    print(f"columns: {columns}")

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} ({columns});
    """
    print(f"create_table_query: {create_table_query}")
    print()
    
    print(f"Creating table {table_name} with columns: {headers}")
    cursor.execute(create_table_query)

    if logs_table:
        pass
