"""Execute analytical SQL queries and display the results."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

import config
from utils import ensure_directories, get_logger

LOGGER = get_logger("run_queries")

QUERY_FILES = {
    "total_revenue_per_user": config.SQL_DIR / "total_revenue_per_user.sql",
    "top_selling_products": config.SQL_DIR / "top_selling_products.sql",
    "monthly_sales_summary": config.SQL_DIR / "monthly_sales_summary.sql",
    "payment_method_distribution": config.SQL_DIR / "payment_method_distribution.sql",
}


def read_sql(path: Path) -> str:
    """Read SQL text from disk."""
    if not path.exists():
        raise FileNotFoundError(f"SQL file missing: {path}")
    return path.read_text(encoding="utf-8")


def run_query(connection: sqlite3.Connection, sql_text: str) -> pd.DataFrame:
    """Execute SQL and return results as a dataframe."""
    return pd.read_sql_query(sql_text, connection)


def display_result(name: str, df: pd.DataFrame) -> None:
    """Pretty print query output."""
    LOGGER.info("Result: %s", name)
    if df.empty:
        LOGGER.warning("No rows returned for %s", name)
    else:
        LOGGER.info("\n%s", df.to_markdown(index=False))


def main() -> None:
    """Entry point for loading SQL files and running them sequentially."""
    ensure_directories()
    if not config.DB_FILE.exists():
        raise FileNotFoundError(
            f"Database missing at {config.DB_FILE}. Run ingest_data.py first."
        )

    with sqlite3.connect(config.DB_FILE) as conn:
        for name, sql_path in QUERY_FILES.items():
            sql_text = read_sql(sql_path)
            LOGGER.info("Executing query: %s", name)
            df = run_query(conn, sql_text)
            display_result(name, df)


if __name__ == "__main__":
    main()

