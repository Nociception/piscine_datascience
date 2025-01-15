import os
import psycopg
from dotenv import load_dotenv
from pathlib import Path
import csv
import docker


def get_env_variables() -> dict[str, str]:
    """Returns the .env varibles in a dictionnary."""

    dotenv_path = Path("../.env")
    assert dotenv_path.exists(), (
        f"ERROR: .env file not found at {dotenv_path.resolve()}"
    )
    print(f".env file found at {dotenv_path.resolve()}")

    load_dotenv(dotenv_path="../.env")
    print("Step: Loading environment variables.")
    env_variables = {
        "postgres_user": os.getenv("POSTGRES_USER"),
        "postgres_password": os.getenv("POSTGRES_PASSWORD"),
        "postgres_db": os.getenv("POSTGRES_DB"),
        "postgres_host": os.getenv("POSTGRES_HOST"),
        "postgres_port": os.getenv("POSTGRES_PORT"),
        "postgres_container_name": os.getenv("POSTGRES_CONTAINER_NAME"),        
    }
    print(env_variables)
    assert all(env_variables.values()), (
        "ERROR: Missing one or more environment variables."
    )

    return env_variables


def copy_files_to_container(
    container_name: str,
    source_dir: str,
    target_dir: str
) -> None:
    """DOCSTRING"""

    client = docker.from_env()
    container = client.containers.get(container_name)

    for root, _, files in os.walk(source_dir):
        for file in files:
            source_path = os.path.join(root, file)
            relative_path = os.path.relpath(source_path, source_dir)
            target_path = os.path.join(target_dir, relative_path)
            print(f"Copying {source_path} to {container_name}: {target_path}")


def main():
    """
    Connects to the PostgreSQL db using environment variables and
    creates tables based on CSV files.
    """

    LOG = 1

    DIR_PATH = "../subject/customer"
    TARGET_DIR = "/data/customer"
    

    try:
        env_variables = get_env_variables()
        
        copy_files_to_container(
            env_variables["postgres_container_name"],
            DIR_PATH,
            TARGET_DIR
        )

###################
        if 1:
            extension_targeted = ".csv"
            csv_files = [
                file for file in os.listdir(DIR_PATH)
                if os.path.isfile(os.path.join(DIR_PATH, file))
                and file.endswith(extension_targeted)
            ]
            if LOG:
                print(f"csv_files:\n{csv_files}")
            if not csv_files:
                print(f"ERROR: No CSV files found int {DIR_PATH}.")
                return

            table_names = [
                os.path.splitext(csv_filename)[0] for csv_filename in csv_files
            ]
            if LOG:
                print(f"table_names:\n{table_names}")
            if not table_names:
                print(f"ERROR: No CSV files found int {DIR_PATH}.")
                return

            csv_paths = [
                os.path.join(DIR_PATH, csv_file) for csv_file in csv_files
            ]
            if LOG:
                print(f"csv_paths:\n{csv_paths}")
            if not csv_paths:
                print(f"ERROR: No CSV files found int {DIR_PATH}.")
                return
###################

        connection = psycopg.connect(
            user=env_variables["postgres_user"],
            password=env_variables["postgres_password"],
            dbname=env_variables["postgres_db"],
            host=env_variables["postgres_host"],
            port=env_variables["postgres_port"],
        )
        cursor = connection.cursor()
        print("Connected to the database successfully.")





        
    except AssertionError as e:
        print(f"{type(e).__name__}: {e}")
    except Exception as e:
        print(f"An unexpected error occured: {e}")
    
    finally:
        if 'connection' in locals():
            connection.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()