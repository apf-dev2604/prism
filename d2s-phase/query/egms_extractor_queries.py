#!/usr/bin/env python3
"""EGMS MySQL extractor SQL text. Queries and identifiers remain unchanged."""


def build_select_sql(cfg, columns, quote_identifier):
    db = quote_identifier(cfg.db)
    table = quote_identifier(cfg.table)
    date_col = quote_identifier(cfg.date_column)
    selected_columns = ",\n            ".join(quote_identifier(col) for col in columns)

    # Date-window filter only. No LIMIT. No keyset condition.
    return f"""
        SELECT
            {selected_columns}
        FROM {db}.{table}
        WHERE {date_col} >= %s
          AND {date_col} < %s
    """


def build_count_sql(cfg, quote_identifier):
    db = quote_identifier(cfg.db)
    table = quote_identifier(cfg.table)
    date_col = quote_identifier(cfg.date_column)
    return f"""
        SELECT COUNT(*) AS cnt
        FROM {db}.{table}
        WHERE {date_col} >= %s
          AND {date_col} < %s
    """

