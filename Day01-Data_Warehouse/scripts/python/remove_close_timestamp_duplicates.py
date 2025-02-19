from psycopg_connection_handler import psycopg_connection_handler
from logger import logger
from psycopg.sql import SQL, Identifier, Literal
from QueryInfo import QueryInfo
from sqli_detection import sqli_detection


@psycopg_connection_handler()
def remove_close_timestamp_duplicates(
    table_name: str,
    time_window: int=1
) -> QueryInfo:
    """
    Removes duplicates with 1 second delta tolerance criteria,
    on the event_time column.

    Uses a SQL command which does:
        a self join on the table
        in order to compare each line with
        all precedent ones: n*(n+1)/2 comparisons
        Order of column comparisons matters:
            it can lead to halves time execution.

    Initial rows number : 20,692,840

    Stats, depending the comparisons order:
        ORDER1:
            t1.product_id = t2.product_id
            AND t1.event_type = t2.event_type
            AND t1.price = t2.price
            AND t1.user_id = t2.user_id
            AND t1.user_session = t2.user_session
            AND ABS(EXTRACT(EPOCH FROM t1.event_time) - EXTRACT(EPOCH FROM t2.event_time)) <= 1
            AND t1.ctid > t2.ctid;
            
            Execution time: 62,909.513 ms
            Number of rows deleted: 1,516,182
            
        ORDER2 (same order as 1st,
            except the last column comparison (index instead of ctid)):
            t1.product_id = t2.product_id
            AND t1.event_type = t2.event_type
            AND t1.price = t2.price
            AND t1.user_id = t2.user_id
            AND t1.user_session = t2.user_session
            AND ABS(EXTRACT(EPOCH FROM t1.event_time) - EXTRACT(EPOCH FROM t2.event_time)) <= 1
            AND t1.index > t2.index;
            
            Execution time: 62,109.124 ms
            Number of rows deleted: 1,516,182

        ORDER3
            remove_duplicates_query = SQL(\"\"\"
                EXPLAIN ANALYZE
                DELETE FROM {table} t1
                USING {table} t2
                WHERE 
                    t1.ctid > t2.ctid
                    AND t1.user_id = t2.user_id
                    AND ABS(EXTRACT(EPOCH FROM t1.event_time) - EXTRACT(EPOCH FROM t2.event_time)) <= {time_window}
                    AND t1.product_id = t2.product_id
                    AND t1.event_type = t2.event_type
                    AND t1.price = t2.price
                    AND t1.user_session = t2.user_session
            \"\"\").format(
                table=Identifier(table_name),
                time_window=Literal(time_window)
            )

            Execution time: 31492.048 ms !!!!!!!!
            Number of rows deleted: 1,516,182
    """

    sqli_detection(table_name)
    if not isinstance(time_window, int):
        raise ValueError(
            "Wrong argument."
        )

    remove_duplicates_query = SQL("""
        EXPLAIN ANALYZE
        DELETE FROM {table} t1
        USING {table} t2
        WHERE 
            t1.ctid > t2.ctid
            AND t1.user_id = t2.user_id
            AND ABS(EXTRACT(EPOCH FROM t1.event_time) - EXTRACT(EPOCH FROM t2.event_time)) <= {time_window}
            AND t1.product_id = t2.product_id
            AND t1.event_type = t2.event_type
            AND t1.price = t2.price
            AND t1.user_session = t2.user_session
    """).format(
        table=Identifier(table_name),
        time_window=Literal(time_window)
    )
    
    return QueryInfo(
        sql_query=remove_duplicates_query,
        modification_type="REMOVE DUPLICATES",
        table_name=table_name
    )
