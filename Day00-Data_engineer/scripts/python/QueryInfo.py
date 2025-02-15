from dataclasses import dataclass
from psycopg.sql import SQL
from typing import Optional


@dataclass
class QueryInfo:
    """
    Data class representing an SQL query with metadata.

    Attributes:
    sql_query (SQL):
        The SQL query to be executed.
    modification_type (str):
        Type of modification (INSERT, DELETE, CREATE, DROP, etc.).
    table_name (str):
        The affected table name.
    files_involved (Optional[str]):
        The file associated with the modification (if applicable).
    values (Optional[list[tuple]]):
        The values used in parameterized queries.
    """

    sql_query: SQL
    modification_type: str
    table_name: str
    files_involved: Optional[str] = None
    values: Optional[list[tuple]] = None
