import psycopg
from QueryInfo import QueryInfo
from table_report import table_report

def proceed_after_table_report(
    cursor: psycopg.Cursor,
    query_info: QueryInfo,
) -> bool:
    """DOCSTRING"""

    if query_info.modification_type in ["CREATE", "DROP"]:
        return True
    
    print("\nSTART: proceed_after_table_report")


    print(query_info)
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


    # print("END: proceed_after_table_report\n")

    # return True