from psycopg_connection_handler import psycopg_connection_handler
from QueryInfo import QueryInfo

@psycopg_connection_handler()
def create_table_if_not_exists(
    table_name: str,
    headers: list[str],
    column_types: list[str],
) -> str:
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
    
    return QueryInfo(
        sql_query=create_table_query,
        modification_type="CREATE",
        table_name=table_name
    )