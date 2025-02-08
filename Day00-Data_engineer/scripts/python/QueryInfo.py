from dataclasses import dataclass

@dataclass
class QueryInfo:
    sql_query: str
    modification_type: str
    table_name: str
    files_involved: str
