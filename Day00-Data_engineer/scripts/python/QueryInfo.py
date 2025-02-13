from dataclasses import dataclass
from psycopg.sql import SQL


@dataclass
class QueryInfo:
    sql_query: SQL
    modification_type: str
    table_name: str
    files_involved: None | str
