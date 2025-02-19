from remove_close_timestamp_duplicates import remove_close_timestamp_duplicates
import os


def main() -> None:
    """DOCSTRING"""

    TABLE_NAME = os.getenv("EX01_TABLE")
    TIMEWINDOW = 1  # seconds

    remove_close_timestamp_duplicates(
        TABLE_NAME,
        time_window=TIMEWINDOW
    )

if __name__ == "__main__":
    main()
