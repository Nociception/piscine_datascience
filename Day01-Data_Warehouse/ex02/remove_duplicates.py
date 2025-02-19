from remove_close_timestamp_duplicates import remove_close_timestamp_duplicates
from vacuum_table import vacuum_table
import os


def main() -> None:
    """DOCSTRING"""

    TABLE_NAME = os.getenv("EX01_TABLE")
    TIMEWINDOW = 1  # seconds

    remove_close_timestamp_duplicates(
        TABLE_NAME,
        time_window=TIMEWINDOW
    )

    vacuum_table(TABLE_NAME)

if __name__ == "__main__":
    main()
