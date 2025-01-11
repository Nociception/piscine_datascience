import os
import psycopg
from dotenv import load_dotenv
from pathlib import Path
import csv
from datetime import datetime
from itertools import islice


def is_float(value: str) -> bool:
    """
    Checks if a string can be converted into a float.
    """

    try:
        float(value)
        return True
    except ValueError:
        return False


def is_datetime(value: str) -> bool:
    """
    Checks if a string matches with a datetime format.
    """

    formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]
    for fmt in formats:
        try:
            datetime.strptime(value, fmt)
            return True
        except ValueError:
            continue
    return False


def infer_column_types(sample_rows, num_samples=100):
    """
    Infers column data type according to a sample of lines.
    """

    types = list()
    for col_index in range(len(sample_rows[0])):
        col_samples = [row[col_index] for row in sample_rows[:num_samples]]
        if all(item.isdigit() for item in col_samples if item):
            types.append("BIGINT")
        elif all(is_float(item) for item in col_samples if item):
            types.append("NUMERIC(10, 2)")
        elif all(is_datetime(item) for item in col_samples if item):
            types.append("TIMESTAMP")
        else:
            types.append("VARCHAR(255)")

    return types


def main():
    """
    Connect to the PostgreSQL db using environment variables and
    create tables based on CSV files.
    """

    dotenv_path = Path("../.env")
    if not dotenv_path.exists():
        print(f"ERROR: .env file not found at {dotenv_path.resolve()}")
    else:
        print(f".env file found at {dotenv_path.resolve()}")
        

    load_dotenv(dotenv_path="../.env")
    print("Step: Loading environment variables.")
    env_variables = {
        "postgres_user": os.getenv("POSTGRES_USER"),
        "postgres_password": os.getenv("POSTGRES_PASSWORD"),
        "postgres_db": os.getenv("POSTGRES_DB"),
        "postgres_host": os.getenv("POSTGRES_HOST"),
        "postgres_port": os.getenv("POSTGRES_PORT"),
    }
    
    print(env_variables)
    
    if not all(env_variables.values()):
        print("ERROR: Missing one or more environment variables.")
        return

    try:
        connection = psycopg.connect(
            user=env_variables["postgres_user"],
            password=env_variables["postgres_password"],
            dbname=env_variables["postgres_db"],
            host=env_variables["postgres_host"],
            port=env_variables["postgres_port"],
        )
        cursor = connection.cursor()
        print("Connected to the database successfully.")

        dir_path = "../subject/customer"
        extension_targeted = ".csv"
        csv_files = [
            file for file in os.listdir(dir_path)
            if os.path.isfile(os.path.join(dir_path, file))
            and file.endswith(extension_targeted)
        ]
        print(f"csv_files: {csv_files}")

        if not csv_files:
            print(f"ERROR: No CSV files found int {dir_path}.")
            return

        csv_file = csv_files[0]
        table_name = os.path.splitext(csv_file)[0]
        # Returns a tuple (filename_without_extension, extension)

        csv_path = os.path.join(dir_path, csv_file)
        
        # Note: variables defined in the with are reachable after the with bloc
        # at the same with indent level.
        with open(csv_path, newline="", encoding="utf-8") as file:
            reader = csv.reader(file)
            headers = next(reader)
            # As reader is an iterator, next is callable with it
            # and allows to get each time the next value within.

            sample_rows = list(islice(reader, 10))
            column_types = infer_column_types(sample_rows)
        
        print(f"sample_rows : ")
        for row in sample_rows:
            print(row)
        print(f"column_types : {column_types}")
        

    except Exception as e:
        print(f"Exception occured: {e}")
        pass
    
    finally:
        if 'connection' in locals():
            connection.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()