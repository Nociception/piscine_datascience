from create_table import create_table
from insert_rows import insert_rows
from QueryInfo import QueryInfo
from psycopg.sql import SQL, Identifier
from logger import logger
from deduplicate_item import deduplicate_item
from psycopg_connection_handler import psycopg_connection_handler


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
        files_involved=None
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


def main():
    TEST_TABLE_NAME = "test"
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

    logger.critical("1")

    create_populate_test_table(
        table_name=TEST_TABLE_NAME,
        headers=TEST_HEADERS,
        column_types=TEST_COLUMN_TYPES
    )

    logger.critical("2")

    deduplicate_item(TEST_TABLE_NAME)

    logger.critical("3")


    EXPECTED_TABLE_NAME = "expected_table"
    create_expected_table(
        table_name=EXPECTED_TABLE_NAME,
        headers=TEST_HEADERS,
        column_types=TEST_COLUMN_TYPES
    )

    logger.critical("4")

    compare_two_tables(TEST_TABLE_NAME+"_deduplicated", EXPECTED_TABLE_NAME)

    DIFFERENT_TABLE_NAME = "different_table"
    create_different_table(
        table_name=DIFFERENT_TABLE_NAME,
        headers=TEST_HEADERS,
        column_types=TEST_COLUMN_TYPES
    )

    compare_two_tables(TEST_TABLE_NAME+"_deduplicated", DIFFERENT_TABLE_NAME)


if __name__ == "__main__":
    main()
