import psycopg
import os
from table_exists import table_exists


def ellipse(
    s: str,
    threshold: int
) -> str:
    """DOCSTRING"""

    return s if len(s) < threshold else s[:threshold - 3] + '...'


def table_report(
    cursor: psycopg.Cursor,
    table_name: str,
) -> None:
    """DOCSTRING"""

    logs_table = os.getenv("LOGS_TABLE")
    if not table_exists(cursor, logs_table):
        raise psycopg.OperationalError(
            f"{logs_table} table does not exist."
        )

    query = f"SELECT * FROM {os.getenv('LOGS_TABLE')} WHERE table_name = %s;"
    cursor.execute(query, (table_name,))

    log_entries = cursor.fetchall()
    if log_entries:
        print(f"\nLogs for table '{table_name}':")

        widths = {
            'Table': 15,
            'Date': 25,
            'Action': 12,
            'File': 25,
            'Row Diff': 8
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
