from create_table import create_table
from insert_rows import insert_rows
from datetime import datetime
from remove_close_timestamp_duplicates import remove_close_timestamp_duplicates


def create_ex02_test_table(
    table_name: str,
    headers: list[str],
    column_types: list[str]
):
    """DOCSTRING"""

    create_table(
        table_name=table_name,
        headers=headers,
        column_types=column_types
    )


def populate_ex02_test_table(
    table_name: str,
    headers: list[str],
):
    """DOCSTRING"""

    ROWS = [
        ("2022-11-01 00:03:14+00", "view", 5888548, 3.97, 429913900, "2f0bff3c-252f-4fe6-afcd-5d8a6a92839a", 0),  # noqa: E501
        ("2022-11-01 00:03:38+00", "cart", 5864286, 20.16, 565876667, "cf5d7069-7465-4ec5-a9be-c911bc1b9f95", 1),  # noqa: E501
        ("2022-11-01 00:03:39+00", "view", 5883844, 25.71, 445896396, "b9052fb9-7da8-4df8-a1da-eae051cd8e9c", 2),  # noqa: E501
        ("2022-11-01 00:03:44+00", "cart", 5674484, 15.71, 562076640, "09fafd6c-6c99-46b1-834f-33527f4de241", 3),  # noqa: E501
        ("2022-11-01 00:03:46+00", "remove_from_cart", 5851312, 6.33, 566239468, "3ad69360-e328-454a-b1f8-0e0901e1e344", 4),  # noqa: E501
        ("2022-11-01 00:03:46+00", "view", 5856190, 15.71, 562076640, "09fafd6c-6c99-46b1-834f-33527f4de241", 5),  # noqa: E501
        ("2022-11-01 00:03:54+00", "view", 5856189, 15.71, 562076640, "09fafd6c-6c99-46b1-834f-33527f4de241", 6),  # noqa: E501
        ("2022-11-01 00:04:00+00", "cart", 5815662, 0.92, 485066868, "9eeee910-722d-495e-8335-c1e3efb00089", 7),  # noqa: E501
        ("2022-11-01 00:04:04+00", "view", 5759382, 4.44, 556138645, "57ed222e-a54a-4907-9944-5a875c2d7f4f", 8),  # noqa: E501
        ("2022-11-01 00:04:21+00", "view", 5829502, 13.75, 445896396, "b9052fb9-7da8-4df8-a1da-eae051cd8e9c", 9),  # noqa: E501
        ("2022-11-01 00:04:29+00", "view", 5864480, 29.21, 565876667, "cf5d7069-7465-4ec5-a9be-c911bc1b9f95", 10),  # noqa: E501
        ("2022-11-01 00:04:33+00", "purchase", 24380, 5.24, 564451209, "861ab2f1-b2e5-886f-a93b-5b067eff081f", 11),  # noqa: E501
        ("2022-11-01 00:04:33+00", "purchase", 26765, 7.16, 564451209, "861ab2f1-b2e5-886f-a93b-5b067eff081f", 12),  # noqa: E501
        ("2022-11-01 00:04:33+00", "purchase", 5573498, 4.29, 564451209, "861ab2f1-b2e5-886f-a93b-5b067eff081f", 13),  # noqa: E501
        ("2022-11-01 00:04:33+00", "purchase", 5586154, 3.17, 564451209, "861ab2f1-b2e5-886f-a93b-5b067eff081f", 14),  # noqa: E501
        ("2022-11-01 00:04:33+00", "purchase", 5739918, 8.73, 564451209, "861ab2f1-b2e5-886f-a93b-5b067eff081f", 15),  # noqa: E501
        ("2022-11-01 00:04:33+00", "purchase", 5766980, 1.98, 564451209, "861ab2f1-b2e5-886f-a93b-5b067eff081f", 16),  # noqa: E501
        ("2022-11-01 00:04:33+00", "purchase", 5767494, 2.14, 564451209, "861ab2f1-b2e5-886f-a93b-5b067eff081f", 17),  # noqa: E501
        ("2022-11-01 00:04:33+00", "purchase", 5775813, 1.98, 564451209, "861ab2f1-b2e5-886f-a93b-5b067eff081f", 18),  # noqa: E501
        ("2022-11-01 00:04:33+00", "purchase", 5775814, 1.98, 564451209, "861ab2f1-b2e5-886f-a93b-5b067eff081f", 19),  # noqa: E501
        ("2022-11-01 00:04:33+00", "purchase", 5881443, 7.78, 564451209, "861ab2f1-b2e5-886f-a93b-5b067eff081f", 20),  # noqa: E501
        ("2022-11-01 00:04:33+00", "purchase", 5884031, 3.97, 564451209, "861ab2f1-b2e5-886f-a93b-5b067eff081f", 21),  # noqa: E501
        ("2022-11-01 00:04:33+00", "purchase", 9169, 4.76, 564451209, "861ab2f1-b2e5-886f-a93b-5b067eff081f", 22),  # noqa: E501
        ("2022-11-01 00:04:46+00", "cart", 5549786, 3.00, 566239468, "3ad69360-e328-454a-b1f8-0e0901e1e344", 23),  # noqa: E501
        ("2022-11-01 00:04:48+00", "view", 5763438, 9.37, 542350069, "e7890f88-28e5-4104-a4c8-f72e8c114f63", 24),  # noqa: E501
        ("2022-11-01 00:04:48+00", "view", 5763438, 9.37, 542350069, "e7890f88-28e5-4104-a4c8-f72e8c114f64", 25),  # noqa: E501
        ("2022-11-01 00:04:48+00", "view", 5763438, 9.37, 542350069, "e7890f88-28e5-4104-a4c8-f72e8c114f65", 26),  # noqa: E501
        ("2022-11-01 00:04:48+00", "view", 5763438, 9.37, 542350069, "e7890f88-28e5-4104-a4c8-f72e8c114f66", 27),  # noqa: E501
        ("2022-11-01 00:04:48+00", "view", 5763438, 9.37, 542350069, "e7890f88-28e5-4104-a4c8-f72e8c114f63", 28),  # noqa: E501  # SLA the 24th
        ("2022-11-01 00:04:51+00", "view", 5861591, 2.22, 534488348, "25360ead-b337-47b1-a61d-e684a8491179", 29),  # noqa: E501
        ("2022-11-01 00:04:51+00", "view", 5763440, 9.37, 542350069, "1f2f220e-8988-4553-808a-524217633522", 30),  # noqa: E501
        ("2022-11-01 00:04:59+00", "remove_from_cart", 5838202, 3.49, 514649199, "dd20e3bf-021f-4633-a9c7-18b6158b2006", 31),  # noqa: E501
        ("2022-11-01 00:04:59+00", "view", 5870458, 10.32, 445896396, "b9052fb9-7da8-4df8-a1da-eae051cd8e9c", 32),  # noqa: E501
        ("2022-11-01 00:05:00+00", "remove_from_cart", 5838192, 3.49, 514649199, "dd20e3bf-021f-4633-a9c7-18b6158b2006", 33),  # noqa: E501
        ("2022-11-01 00:05:02+00", "view", 5896424, 32.54, 562076640, "09fafd6c-6c99-46b1-834f-33527f4de241", 34),  # noqa: E501
        ("2022-11-01 00:05:06+00", "view", 5838169, 3.49, 514649199, "dd20e3bf-021f-4633-a9c7-18b6158b2006", 35),  # noqa: E501
        ("2022-11-01 00:05:17+00", "view", 5625247, 98.40, 424460109, "1f21a6e7-5d75-4c77-adcc-1d47d3b16e15", 36),  # noqa: E501
        ("2022-11-01 00:05:30+00", "view", 5885630, 31.35, 556138645, "57ed222e-a54a-4907-9944-5a875c2d7f4f", 37),  # noqa: E501
        ("2022-11-01 00:05:44+00", "remove_from_cart", 5838169, 3.49, 514649199, "dd20e3bf-021f-4633-a9c7-18b6158b2006", 38),  # noqa: E501
        ("2022-11-01 00:05:44+00", "remove_from_cart", 5838169, 3.49, 514649199, "dd20e3bf-021f-4633-a9c7-18b6158b2005", 39),  # noqa: E501
        ("2022-11-01 00:05:44+00", "remove_from_cart", 5838169, 3.49, 514649199, "dd20e3bf-021f-4633-a9c7-18b6158b2004", 40),  # noqa: E501
        ("2022-11-01 00:05:44+00", "remove_from_cart", 5838169, 3.49, 514649199, "dd20e3bf-021f-4633-a9c7-18b6158b2003", 41),  # noqa: E501
        ("2022-11-01 00:05:44+00", "remove_from_cart", 5838169, 3.49, 514649199, "dd20e3bf-021f-4633-a9c7-18b6158b2002", 42),  # noqa: E501
        ("2022-11-01 00:05:45+00", "remove_from_cart", 5838169, 3.49, 514649199, "dd20e3bf-021f-4633-a9c7-18b6158b2006", 43),  # noqa: E501 # SLA the 38, with TD1
        ("2022-11-01 00:05:49+00", "view", 5864544, 19.05, 565876667, "cf5d7069-7465-4ec5-a9be-c911bc1b9f95", 44),  # noqa: E501
        ("2022-11-01 00:05:54+00", "remove_from_cart", 5838150, 3.49, 514649199, "dd20e3bf-021f-4633-a9c7-18b6158b2006", 45),  # noqa: E501
        ("2022-11-01 00:05:55+00", "view", 5857967, 6.44, 391027139, "8b820c4f-4cee-4407-9f6a-dc0f926959bb", 46),  # noqa: E501
        ("2022-11-01 00:05:58+00", "view", 38005, 10.13, 566273978, "c2d3b5fa-445a-432e-bfa8-bea9652e3cfe", 47),  # noqa: E501
        ("2022-11-01 00:06:11+00", "view", 5549786, 3.00, 566239468, "3ad69360-e328-454a-b1f8-0e0901e1e344", 48),  # noqa: E501
        ("2022-11-01 00:06:22+00", "remove_from_cart", 5826980, 3.95, 556138645, "57ed222e-a54a-4907-9944-5a875c2d7f4f", 49),  # noqa: E501
        ("2022-11-01 00:06:22+00", "remove_from_cart", 5826980, 3.95, 556138645, "57ed222e-a54a-4907-9944-5a875c2d7f4f", 50),  # noqa: E501  # SLA above
        ("2022-11-01 00:06:24+00", "remove_from_cart", 5766772, 4.13, 556138645, "57ed222e-a54a-4907-9944-5a875c2d7f4f", 51),  # noqa: E501
        ("2022-11-01 00:06:34+00", "view", 5863097, 0.63, 429904458, "33bd95b0-7be4-4b05-8e7a-96225ad6873b", 52),  # noqa: E501
        ("2022-11-01 00:06:34+00", "view", 5864545, 19.05, 565876667, "cf5d7069-7465-4ec5-a9be-c911bc1b9f95", 53),  # noqa: E501
        ("2022-11-01 00:06:36+00", "view", 5893540, 7.44, 562817002, "32e737b9-7439-40b0-9011-33a0f757cdf8", 54),  # noqa: E501
        ("2022-11-01 00:06:42+00", "cart", 5770294, 1.59, 514649199, "dd20e3bf-021f-4633-a9c7-18b6158b2006", 55),  # noqa: E501
        ("2022-11-01 00:06:42+00", "cart", 5770294, 1.59, 514649199, "dd20e3bf-021f-4633-a9c7-18b6158b2006", 56),  # noqa: E501  # SLA above
        ("2022-11-01 00:06:42+00", "cart", 5770294, 1.59, 514649199, "dd20e3bf-021f-4633-a9c7-18b6158b2006", 57),  # noqa: E501  # SLA above (the second one in a row)
        ("2022-11-01 00:06:55+00", "view", 5747404, 6.33, 554428484, "64ccbe91-91f6-4671-b276-857dfa0e99aa", 58),  # noqa: E501
        ("2022-11-01 00:13:19+00", "remove_from_cart", 5749150, 0.22, 202438687, "8dc848f5-bac3-44d7-9414-75d4e599abaf", 59),  # noqa: E501
        ("2022-11-01 00:13:20+00", "remove_from_cart", 5749150, 0.22, 202438687, "8dc848f5-bac3-44d7-9414-75d4e599abaf", 60),  # noqa: E501  # SLA above TD1
    ]

    ROWS = [
        (datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S+00"), *row[1:])
        for row in ROWS
    ]

    insert_rows(
        table_name=table_name,
        headers=headers,
        rows=ROWS,
        files_involved="ex02_tests"
    )


def main():
    """
    Creates and populates a test table with five cases:
    - lines 24 and 28:
        are the same
        separated by 3 lines with the same timestamp,
        but different on the other columns (user session)
    - lines 38 and 43:
        are the same, except the 1 sec timestamp delta
        separated by 4 lines with the same timestamp as the 38th,
        but different on the other columns (user session)
    - lines 56 and 57:
        are the same
    - lines 59 and 60:
        are the same, except the 1 sec timestamp delta

    Test table already time sorted.

    Legends for the comments:
    - SLA: Same line as
    - TD1: time delta 1 second

    After the duplicates deletion,
    the following must be deleted: 28, 43, 50, 56, 57, 60
    61 rows before, 55 rows after.
    """

    EX02_TEST_TABLE = "ex02_test_table"

    HEADERS = [
        "event_time",
        "event_type",
        "product_id",
        "price",
        "user_id",
        "user_session",
        "index"
    ]

    COLUMN_TYPES = [
        "TIMESTAMPTZ",
        "VARCHAR(50)",
        "INT",
        "NUMERIC(10, 2)",
        "BIGINT",
        "UUID",
        "INT"
    ]

    create_ex02_test_table(
        table_name = EX02_TEST_TABLE,
        headers = HEADERS,
        column_types = COLUMN_TYPES
    )

    populate_ex02_test_table(
        table_name = EX02_TEST_TABLE,
        headers = HEADERS,
    )
    
    remove_close_timestamp_duplicates(table_name=EX02_TEST_TABLE)


if __name__ == "__main__":
    main()