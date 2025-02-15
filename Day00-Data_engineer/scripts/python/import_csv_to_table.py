from logger import logger
from psycopg_connection_handler import psycopg_connection_handler
from QueryInfo import QueryInfo
from psycopg.sql import SQL, Identifier, Literal
from sqli_detection import sqli_detection


@psycopg_connection_handler()
def import_csv_to_table(
    table_name: str,
    csv_path: str
) -> QueryInfo:
    """
    Imports a CSV file into a specified PostgreSQL table
    using the COPY command.

    Args:
        table_name (str):
            The name of the table where data will be imported.
        csv_path (str): The full path to the CSV file.

    Returns:
        QueryInfo: An object containing query metadata.
    """

    sqli_detection(table_name)
    sqli_detection(csv_path)

    copy_query = SQL("""
        COPY {} FROM {}
        DELIMITER ',' CSV HEADER;
    """).format(Identifier(table_name), Literal(csv_path))

    logger.info(f"Importing data from {csv_path} into {table_name}...")
    
    return QueryInfo(
        sql_query=copy_query,
        modification_type="IMPORT CSV",
        table_name=table_name,
        files_involved=csv_path
    )
