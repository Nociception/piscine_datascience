from psycopg_connection_handler import psycopg_connection_handler
from QueryInfo import QueryInfo
from psycopg.sql import SQL, Identifier
import sys


@psycopg_connection_handler()
def drop_table(
    table_name: str
) -> QueryInfo:
    """DOCSTRING"""

    drop_query = SQL("DROP TABLE {}").format(
        Identifier(table_name)
    )

    return QueryInfo(
        sql_query=drop_query,
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
