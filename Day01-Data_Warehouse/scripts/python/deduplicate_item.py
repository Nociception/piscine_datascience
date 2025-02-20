from psycopg_connection_handler import psycopg_connection_handler
from QueryInfo import QueryInfo
from psycopg.sql import SQL, Identifier


@psycopg_connection_handler()
def deduplicate_item(table_name: str) -> QueryInfo:
    """DOCSTRING SQLI"""

    new_table_name = table_name + "_deduplicated"

    deduplicate_query = SQL("""
        CREATE TABLE IF NOT EXISTS {} AS
        SELECT 
            product_id,
            MAX(category_id) FILTER (WHERE category_id IS NOT NULL) AS category_id,
            MAX(category_code) FILTER (WHERE category_code IS NOT NULL) AS category_code,
            MAX(brand) FILTER (WHERE brand IS NOT NULL) AS brand
        FROM {}
        GROUP BY product_id;
    """).format(
        Identifier(new_table_name),
        Identifier(table_name)
    )

    return QueryInfo(
        sql_query = deduplicate_query,
        modification_type = "DEDUPLICATE",
        table_name = table_name
    )
