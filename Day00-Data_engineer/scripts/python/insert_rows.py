from psycopg_connection_handler import psycopg_connection_handler
from QueryInfo import QueryInfo
from datetime import datetime
import os


def format_value(value):
    """DOCSTRING"""

    if value is None:
        return "NULL"
    if isinstance(value, str):
        return f"'{value}'"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, datetime):
        return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
    raise ValueError(f"Unsupported data type: {type(value)}")


@psycopg_connection_handler()
def insert_rows(
    table_name: str,
    headers: list[str],
    rows: list[tuple],
) -> QueryInfo:
    """DOCSTRING"""

    columns = ", ".join(headers)
    formatted_values = ", ".join(
        f"({', '.join(format_value(value) for value in row)})"
        for row in rows
    )

    insert_query = f"""
        INSERT INTO {table_name} ({columns})
        VALUES {formatted_values};
    """

    return QueryInfo(
        sql_query=insert_query,
        modification_type="INSERT",
        table_name=table_name,
        files_involved=os.getenv("EX01_PY")
    )
