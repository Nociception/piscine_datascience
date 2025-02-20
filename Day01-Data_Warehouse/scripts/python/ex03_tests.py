from create_table import create_table
from insert_rows import insert_rows
from QueryInfo import QueryInfo
from psycopg.sql import SQL, Identifier
from logger import logger
from deduplicate_item import deduplicate_item
from psycopg_connection_handler import psycopg_connection_handler
from datetime import datetime
from add_columns_table import add_columns_table
import os


def create_populate_test_table(
    table_name: str,
    headers: list[str],
    column_types: list[str]
) -> None:
    """DOCSTRING"""

    create_table(
        table_name=table_name,
        headers=headers,
        column_types = column_types
    )

    SAMPLE_DATA = [
        (5712790, None, None, None),
        (5712790, 2002, None, None),
        (5712790, None, "3,3", None),
        (5712790, None, None, "4,4"),  # 4
        # (5712790, 2002, "3,3", "4,4"),

        (5764655, 1487580005411062528, None, None),
        (5764655, 1487580005411062528, "6,3", None),
        (5764655, 1487580005411062528, None, "7,4"),  # 7
        # (5764655, 1487580005411062528, "6,3", "7,4"),

        (4958, None, "lol8,3", None),
        (4958, 9002, "lol8,3", None),
        (4958, None, "lol8,3", "10,4"),  # 10
        # (4958, 9002, "lol8,3", "10,4"),

        (5848413, None, None, "freedecor"),
        (5848413, 12002, None, "freedecor"),
        (5848413, None, "13,3", "freedecor"),  # 13
        # (5848413, 12002, "13,3", "freedecor"),

        (5629988, 1487580009311764480, "14,3", None),
        (5629988, 1487580009311764480, None, "15,4"),  # 15
        # (5629988, 1487580009311764480, "14,3", "15,4"),
        
        (5706778, 1487580005268456192, None, "beautix"),
        # (5706778, 1487580005268456192, None, "beautix"),
        
        (5838935, 1487580005713052416, None, "ingarden"),
        (5838935, 1487580005713052416, None, "ingarden"),
        # (5838935, 1487580005713052416, None, "ingarden"),

        (5808300, 1487580005511725824, None, None),
        # (5808300, 1487580005511725824, None, None),

        (5687131, 1487580008187692032, None, None),
        (5687131, 1487580008187692032, None, None),
        # (5687131, 1487580008187692032, None, None),

        (5746848, 2193074740686488320, "furniture.bathroom.bath", None),
        (5746848, 2193074740686488320, "furniture.bathroom.bath", None),
        # (5746848, 2193074740686488320, "furniture.bathroom.bath", None),

        (5885594, 1487580006350586880, "appliances.environment.vacuum", "polarus"),
        (5885594, 1487580006350586880, "appliances.environment.vacuum", "polarus"),
        # (5885594, 1487580006350586880, "appliances.environment.vacuum", "polarus"),

        (5692279, 1487580004857414400, None, "lianail"),
        (5692279, 1487580004857414400, "accessories.bag", None),
        # (5692279, 1487580004857414400, "accessories.bag", "lianail"),
    ]

    insert_rows(
        table_name = table_name,
        headers=headers,
        rows=SAMPLE_DATA,
    )


def create_expected_table(
    table_name: str,
    headers: list[str],
    column_types: list[str]
) -> None:
    """DOCSTRING"""

    create_table(
        table_name=table_name,
        headers=headers,
        column_types=column_types
    )

    EXPECTED_ROWS = [
        (5712790, 2002, "3,3", "4,4"),
        (5764655, 1487580005411062528, "6,3", "7,4"),
        (4958, 9002, "lol8,3", "10,4"),
        (5848413, 12002, "13,3", "freedecor"),
        (5629988, 1487580009311764480, "14,3", "15,4"),
        (5706778, 1487580005268456192, None, "beautix"),
        (5838935, 1487580005713052416, None, "ingarden"),
        (5808300, 1487580005511725824, None, None),
        (5687131, 1487580008187692032, None, None),
        (5746848, 2193074740686488320, "furniture.bathroom.bath", None),
        (5885594, 1487580006350586880, "appliances.environment.vacuum", "polarus"),
        (5692279, 1487580004857414400, "accessories.bag", "lianail"),
    ]

    insert_rows(
        table_name=table_name,
        headers=headers,
        rows=EXPECTED_ROWS
    )


def create_different_table(
    table_name: str,
    headers: list[str],
    column_types: list[str]
) -> None:
    """DOCSTRING"""

    create_table(
        table_name=table_name,
        headers=headers,
        column_types=column_types
    )

    DIFFERENT_ROWS = [
        (5712790, 2002, "3,3", "4,4"),
        (5764655, 1487580005411062528, "6,3", "7,4"),
        (4958, 9002, "lol8,3", "10,4"),
        (5848413, 12002, "13,3", "freedecor"),
        (5629988, 1487580009311764480, "14,3", "15,4"),
        (5706778, 1487580005268456192, None, "beautix"),
        (5838935, 1487580005713052416, None, "ingarden"),
        (5808300, 1487580005511725824, None, None),
        (5687131, 1487580008187692032, None, None),
        (5687131, 1487580008187692031, None, None),  # different line
        (5746848, 2193074740686488320, "furniture.bathroom.bath", None),
        (5746847, 2193074740686488329, "furniture.bathroom.bath", None),  # different line
        (5885594, 1487580006350586880, "appliances.environment.vacuum", "polarus"),
        (5885593, 1487580006350586889, "appliances.environment.vacuun", "polarus"),  # different line
        (5692279, 1487580004857414400, "accessories.bag", "lianail"),
        (5892279, 1487580054857414400, "accessorie.bag", "lianaif"),  # different line
    ]

    insert_rows(
        table_name=table_name,
        headers=headers,
        rows=DIFFERENT_ROWS
    )


@psycopg_connection_handler()
def compare_two_tables(
    t1: str,
    t2: str
) -> QueryInfo:
    """DOCSTRING sqli"""

    comparison_query = SQL("""
        (
            SELECT * FROM {table1}
            EXCEPT ALL
            SELECT * FROM {table2}
        )
        UNION ALL
        (
            SELECT * FROM {table2}
            EXCEPT ALL
            SELECT * FROM {table1}
        )
    """).format(
        table1=Identifier(t1),
        table2=Identifier(t2)
    )

    return QueryInfo(
        sql_query=comparison_query,
        modification_type=f"COMPARISON ({t1} and {t2})",
        table_name=t1
    )


def create_populate_customer_test_table() -> None:
    """DOCSTRING"""

    CUSTOMERS_TABLE_TEST = os.getenv("EX01_TABLE") + "_test"

    with open("/data/customer/data_2022_oct.csv", "r", encoding="utf-8") as file:
        CUSTOMERS_HEADERS = file.readline().strip().split(",")

    COLUMN_TYPES = [
        "TIMESTAMP WITH TIME ZONE",
        "VARCHAR(50)",
        "INT",
        "NUMERIC(10, 2)",
        "BIGINT",
        "UUID"
    ]

    create_table(
        table_name=CUSTOMERS_TABLE_TEST,
        headers=CUSTOMERS_HEADERS,
        column_types=COLUMN_TYPES
    )

    ROWS = [
        ("2022-10-01 00:00:00+00", "cart", 5712790, 2.62, 463240011, "26dd6e6e-4dac-4778-8d2c-92e149dab885"),  #
        ("2022-10-01 00:00:03+00", "cart", 5764655, 2.62, 463240011, "26dd6e6e-4dac-4778-8d2c-92e149dab885"),  #
        ("2022-10-01 00:00:07+00", "cart", 4958, 13.48, 429681830, "49e8d843-adf3-428b-a2c3-fe8bc6a307c9"),  #
        ("2022-10-01 00:00:07+00", "cart", 5848413, 2.62, 463240011, "26dd6e6e-4dac-4778-8d2c-92e149dab885"),  #
        ("2022-10-01 00:00:15+00", "cart", 5629988, 0.56, 429681830, "49e8d843-adf3-428b-a2c3-fe8bc6a307c9"),  #
        ("2022-10-01 00:00:16+00", "cart", 5706778, 2.62, 430174032, "73dea1e7-664e-43f4-8b30-d32b9d5af04f"),  #
        ("2022-10-01 00:00:19+00", "cart", 5838935, 4.75, 377667011, "81326ac6-daa4-4f0a-b488-fd0956a78733"),  #
        ("2022-10-01 00:00:24+00", "cart", 5838935, 0.56, 467916806, "2f5b5546-b8cb-9ee7-7ecd-84276f8ef486"),  #
        ("2022-10-01 00:00:25+00", "cart", 5808300, 1.27, 385985999, "d30965e8-1101-44ab-b45d-cc1bb9fae694"),  #
        ("2022-10-01 00:00:26+00", "view", 5687131, 1.59, 474232307, "445f2b74-5e4c-427e-b7fa-6e0a28b156fe"),  #
        ("2022-10-01 00:00:28+00", "view", 5746848, 5.54, 555446068, "4257671a-efc8-4e58-96c2-3ab457916d78"),  #
        ("2022-10-01 00:00:28+00", "remove_from_cart", 5885594, 0.95, 429681830, "49e8d843-adf3-428b-a2c3-fe8bc6a307c9"),  #
        ("2022-10-01 00:00:30+00", "remove_from_cart", 5692279, 0.60, 429681830, "49e8d843-adf3-428b-a2c3-fe8bc6a307c9"),
        ("2022-10-01 00:00:30+00", "remove_from_cart", 5809103, 0.60, 429681830, "49e8d843-adf3-428b-a2c3-fe8bc6a307c9"),
        ("2022-10-01 00:00:32+00", "remove_from_cart", 5779403, 12.22, 429681830, "49e8d843-adf3-428b-a2c3-fe8bc6a307c9"),
        ("2022-10-01 00:00:33+00", "remove_from_cart", 5779403, 12.22, 429681830, "49e8d843-adf3-428b-a2c3-fe8bc6a307c9"),
        ("2022-10-01 00:00:34+00", "cart", 5670337, 2.38, 546705258, "3b5c65c0-bb1c-453b-b340-4ebf973a3136"),
        ("2022-10-01 00:00:42+00", "cart", 5836522, 0.40, 429681830, "49e8d843-adf3-428b-a2c3-fe8bc6a307c9"),
        ("2022-10-01 00:00:43+00", "cart", 5836522, 0.40, 429681830, "49e8d843-adf3-428b-a2c3-fe8bc6a307c9"),
        ("2022-10-01 00:00:48+00", "view", 5819638, 21.75, 546705258, "3b5c65c0-bb1c-453b-b340-4ebf973a3136"),
        ("2022-10-01 00:00:48+00", "cart", 5859414, 2.37, 555442940, "618f3d7d-2939-47ea-8f1d-07a4f97d0fe2"),
        ("2022-10-01 00:00:53+00", "view", 5856191, 24.44, 507355498, "944c7e9b-40bd-4112-a05b-81e73f37e0c0"),
        ("2022-10-01 00:00:55+00", "cart", 5859413, 2.37, 555442940, "618f3d7d-2939-47ea-8f1d-07a4f97d0fe2"),
        ("2022-10-01 00:00:56+00", "remove_from_cart", 5881589, 13.48, 429681830, "49e8d843-adf3-428b-a2c3-fe8bc6a307c9"),
        ("2022-10-01 00:01:01+00", "cart", 5723518, 2.62, 430174032, "c2bbd970-a5ad-42dd-a59b-f44276330b02"),
        ("2022-10-01 00:01:02+00", "remove_from_cart", 5848908, 1.90, 429681830, "49e8d843-adf3-428b-a2c3-fe8bc6a307c9"),
        ("2022-10-01 00:01:03+00", "cart", 5677366, 7.30, 524009100, "8bbff347-0be1-470e-8860-d9a75db965b2"),
        ("2022-10-01 00:01:03+00", "cart", 5859411, 2.37, 555442940, "618f3d7d-2939-47ea-8f1d-07a4f97d0fe2"),
        ("2022-10-01 00:01:03+00", "remove_from_cart", 5729011, 0.79, 429681830, "49e8d843-adf3-428b-a2c3-fe8bc6a307c9"),
        ("2022-10-01 00:01:05+00", "remove_from_cart", 5858981, 0.79, 429681830, "49e8d843-adf3-428b-a2c3-fe8bc6a307c9"),
    ]

    ROWS = [
        (datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S+00"), *row[1:])
        for row in ROWS
    ]

    insert_rows(
        table_name=CUSTOMERS_TABLE_TEST,
        headers=CUSTOMERS_HEADERS,
        rows=ROWS
    )


def main():
    """DOCSTRING"""

    TEST_HEADERS = [
        "product_id",
        "category_id",
        "category_code",
        "brand",
    ]
    TEST_COLUMN_TYPES = [
        "INT",
        "NUMERIC(20, 0)",
        "VARCHAR(50)",
        "VARCHAR(20)",
    ]

    if 0:
        TEST_TABLE_NAME = "test"

        create_populate_test_table(
            table_name=TEST_TABLE_NAME,
            headers=TEST_HEADERS,
            column_types=TEST_COLUMN_TYPES
        )

        deduplicate_item(TEST_TABLE_NAME)


        EXPECTED_TABLE_NAME = "expected_table"
        create_expected_table(
            table_name=EXPECTED_TABLE_NAME,
            headers=TEST_HEADERS,
            column_types=TEST_COLUMN_TYPES
        )

        compare_two_tables(TEST_TABLE_NAME+"_deduplicated", EXPECTED_TABLE_NAME)

        DIFFERENT_TABLE_NAME = "different_table"
        create_different_table(
            table_name=DIFFERENT_TABLE_NAME,
            headers=TEST_HEADERS,
            column_types=TEST_COLUMN_TYPES
        )

        compare_two_tables(TEST_TABLE_NAME+"_deduplicated", DIFFERENT_TABLE_NAME)

###########################

    if 1:
        create_populate_customer_test_table()

        with open("/data/item", "r", encoding="utf-8") as file:
            ITEM_HEADERS = file.readline().strip().split(",")

        add_columns_table(
            table_name=CUSTOMERS_TABLE_TEST,
            headers=ITEM_HEADERS[1:],
            new_columns=COLUMN_TYPES[1:]
        )




if __name__ == "__main__":
    main()
