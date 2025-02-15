from logger import logger


def get_all_csv_in_dir(csv_dir: str) -> None | list[str]:
    """DOCSTRING"""

    assert csv_dir.exists(), (
        f"ERROR: CSV directory not found at {csv_dir}"
    )
    logger.info(f"CSV directory resolved to: {csv_dir}")

    csv_files = [
        file.name for file in csv_dir.iterdir()
        if file.is_file() and file.suffix == ".csv"
    ]
    if not csv_files:
        logger.error("No CSV files found in the CSV directory.")
        return None
    
    logger.info(f"CSV files found: {csv_files}")
    return csv_files
