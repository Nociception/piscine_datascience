from psycopg_connection_handler import psycopg_connection_handler
from table_exists import table_exists
from get_psycopg_connection import get_psycopg_connection
from QueryInfo import QueryInfo
import sys


@psycopg_connection_handler()
def drop_table(
    table_name: str
) -> QueryInfo:
    """DOCSTRING"""

    return QueryInfo(
        sql_query=f"""DROP TABLE {table_name};""",
        modification_type="DROP",
        table_name=table_name,
        files_involved=None
    )


def main() -> None:
    """DOCSTRING"""

    if len(sys.argv) != 2:
        print("Usage: python drop_table.py <table_name>")
        sys.exit(1)

    drop_table(table_name=sys.argv[1])


if __name__ == "__main__":
    main()
