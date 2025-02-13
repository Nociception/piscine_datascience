from psycopg_connection_handler import psycopg_connection_handler
from QueryInfo import QueryInfo
from psycopg.sql import SQL, Identifier, Placeholder


@psycopg_connection_handler()
def insert_rows(
    table_name: str,
    headers: list[str],
    rows: list[tuple],
    files_involved: str | None = None
) -> QueryInfo:
    """DOCSTRING"""

    columns_sql = [Identifier(col) for col in headers]

    placeholders_list = []
    for _ in rows:
        row_placeholders = [Placeholder() for _ in headers]
        placeholders_list.append(
            SQL("({})").format(SQL(", ").join(row_placeholders))
        )

    insert_query = SQL(
        "INSERT INTO {} ({}) VALUES {}"
    ).format(
        Identifier(table_name),
        SQL(", ").join(columns_sql),
        SQL(", ").join(placeholders_list)
    )

    return QueryInfo(
        sql_query=insert_query,
        modification_type="INSERT",
        table_name=table_name,
        files_involved=files_involved,
        values=rows
    )
