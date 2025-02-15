from get_psycopg_connection import get_psycopg_connection
from table_report import table_report
from table_exists import table_exists
import psycopg, os


def ellipse(
    s: str,
    threshold: int
) -> str:
    """DOCSTRING"""

    return s if len(s) < threshold else s[:threshold - 3] + '...'


def vulnerable_table_report(
    cursor: psycopg.Cursor,
    table_name: str,
) -> None:
    """DOCSTRING"""
    
    logs_table = os.getenv("LOGS_TABLE")
    if not table_exists(cursor, logs_table):
        raise psycopg.OperationalError(
            f"{logs_table} table does not exist."
        )

    query = f"""
        SELECT * FROM {os.getenv('LOGS_TABLE')}
        WHERE table_name = '{table_name}';
    """
    cursor.execute(query)

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


if __name__ == "__main__":

    print(
        "This sqli scenario is supposed to be launched"
        " after logs table is already populated.\n"
        "In other words: you should launch it after every make ex* !\n"
    )

    try:
        connection, cursor = get_psycopg_connection()

        print("With vulnerable_table_report:")
        print("Normal query: vulnerable_table_report(\"item\")")
        print("Result:")
        vulnerable_table_report(cursor, "item")

        print("SQLi attempt: vulnerable_table_report(\"item\' OR \'1\'=\'1\")")
        print("Result:")
        vulnerable_table_report(cursor, "item' OR '1'='1")


        print("\nWith table_report:")
        print("Normal query: table_report(\"item\")")
        print("Result:")
        table_report(cursor, "item")

        print("SQLi attempt: table_report(\"item\' OR \'1\'=\'1\")")
        print("Result:")
        table_report(cursor, "item' OR '1'='1")

    except psycopg.OperationalError as e:
        print(f"Database connection error: {e}")
    except psycopg.ProgrammingError as e:
        print(f"Programming error in SQL query: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if connection:
            connection.close()
            print("Database connection closed.")
