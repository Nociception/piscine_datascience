import psycopg
from QueryInfo import QueryInfo
from table_report import table_report
from logger import logger


def proceed_after_table_report(
    cursor: psycopg.Cursor,
    query_info: QueryInfo,
) -> bool:
    """DOCSTRING"""

    if query_info.modification_type in ["CREATE", "DROP"]:
        return True

    logger.info(query_info)
    table_report(cursor, query_info.table_name)

    while True:
        user_input = input(
            "Do you want to proceed with the import? (yes/no) "
        ).strip().lower()
        if user_input in ["yes", "y", ""]:
            return True
        elif user_input in ["no", "n"]:
            return False
        else:
            print("Invalid input. Please type 'yes' or 'no'.")
