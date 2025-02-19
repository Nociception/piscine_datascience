from table_exists import table_exists
from psycopg.sql import SQL, Identifier
from sqli_detection import sqli_detection
import psycopg
import os


def ellipse(
    s: str,
    threshold: int
) -> str:
    """
    Truncates a string if it exceeds a given length.

    Args:
        s (str): The input string.
        threshold (int): The maximum allowed length.

    Returns:
        str: The truncated string with '...' appended if necessary.
    """

    return s if len(s) < threshold else s[:threshold - 3] + '...'


def table_report(
    cursor: psycopg.Cursor,
    table_name: str,
) -> None:
    """
    Displays log entries related to a specific table.

    Args:
        cursor (psycopg.Cursor): The database cursor.
        table_name (str): The table for which logs should be retrieved.

    Raises:
        psycopg.OperationalError: If the logs table does not exist.
    """

    logs_table = os.getenv("LOGS_TABLE")
    if not table_exists(cursor, logs_table):
        raise psycopg.OperationalError(
            f"{logs_table} table does not exist."
        )

    sqli_detection(table_name)
    sqli_detection(logs_table)

    query = SQL(
        "SELECT * FROM {} WHERE table_name = %s;"
    ).format(Identifier(logs_table))
    cursor.execute(query, (table_name,))

    log_entries = cursor.fetchall()
    if log_entries:
        print(f"\nLogs for table '{table_name}':")

        widths = {
            'Table': 20,
            'Date': 25,
            'Action': 40,
            'File': 25,
            'Row Diff': 10
        }
        print(" | ".join(
            f"{title:<{width}}"
            for title, width in widths.items()
            )
        )

        print("-" * (sum(k for k in widths.values()) + 3 * (len(widths) - 1)))
        for entry in log_entries:
            row = (
                entry[0],
                entry[1].strftime("%Y-%m-%d %H:%M:%S"),
                entry[2],
                entry[3] if entry[3] else "N/A",
                str(entry[4]) if entry[4] else '0'
            )
            print(" | ".join(
                    f"{ellipse(row[k], width):<{width}.{width}}"
                    for k, width in enumerate(widths.values())
                )
            )
        print()

    else:
        print(f"\nNo previous log found concerning '{table_name}'.\n")
