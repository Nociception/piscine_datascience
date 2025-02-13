from dataclasses import dataclass
from psycopg.sql import SQL
from typing import Optional


@dataclass
class QueryInfo:
    sql_query: SQL
    modification_type: str
    table_name: str
    files_involved: Optional[str] = None
    values: Optional[list[tuple]] = None
