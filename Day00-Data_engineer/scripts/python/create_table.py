from psycopg_connection_handler import psycopg_connection_handler
from QueryInfo import QueryInfo
from psycopg.sql import SQL, Identifier
from sqli_detection import sqli_detection


@psycopg_connection_handler()
def create_table(
    table_name: str,
    headers: list[str],
    column_types: list[str],
) -> QueryInfo:
    """DOCSTRING"""

    sqli_detection(table_name)
    for elt in headers:
        sqli_detection(elt)
    for elt in column_types:
        sqli_detection(elt)


    column_definitions = []
    for header, column_type in zip(headers, column_types):
        column_definitions.append(SQL("{} {}").format(
            Identifier(header), SQL(column_type))
        )
    columns_sql = SQL(", ").join(column_definitions)

    create_table_query = SQL("CREATE TABLE {} ({})").format(
        Identifier(table_name), columns_sql
    )

    return QueryInfo(
        sql_query=create_table_query,
        modification_type="CREATE",
        table_name=table_name,
        files_involved=None
    )
