"""Generate a summary report for the synthetic data workflow."""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

import pandas as pd

import config
from utils import ensure_directories, get_logger

LOGGER = get_logger("report")

CSV_FILES = [
    ("users", config.DATA_DIR / "users.csv"),
    ("products", config.DATA_DIR / "products.csv"),
    ("orders", config.DATA_DIR / "orders.csv"),
    ("order_items", config.DATA_DIR / "order_items.csv"),
    ("payments", config.DATA_DIR / "payments.csv"),
]

SQL_SNIPPETS = {
    "total_revenue_per_user": config.SQL_DIR / "total_revenue_per_user.sql",
    "top_selling_products": config.SQL_DIR / "top_selling_products.sql",
}


def summarize_csvs(files: Iterable[tuple[str, Path]]) -> pd.DataFrame:
    """Collect row/column counts per CSV."""
    stats = []
    for name, path in files:
        if not path.exists():
            LOGGER.warning("CSV missing for report: %s", path)
            continue
        df = pd.read_csv(path)
        stats.append(
            {
                "dataset": name,
                "rows": len(df),
                "columns": len(df.columns),
                "sample_columns": ", ".join(df.columns[:5]),
            }
        )
    return pd.DataFrame(stats)


def summarize_tables(connection: sqlite3.Connection) -> pd.DataFrame:
    """Return table row counts from SQLite."""
    tables = [name for name, _ in CSV_FILES]
    rows = []
    for table in tables:
        count = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        rows.append({"table": table, "row_count": count})
    return pd.DataFrame(rows)


def fetch_sql_preview(
    connection: sqlite3.Connection, queries: dict[str, Path]
) -> dict[str, pd.DataFrame]:
    """Execute a subset of SQL analytics to highlight KPIs."""
    results = {}
    for name, path in queries.items():
        if not path.exists():
            LOGGER.warning("SQL snippet missing: %s", path)
            continue
        sql_text = path.read_text(encoding="utf-8")
        df = pd.read_sql_query(sql_text, connection)
        results[name] = df
    return results


def main() -> None:
    """Entry point for generating the textual report."""
    ensure_directories()

    LOGGER.info("=== CSV DATASETS ===")
    csv_stats = summarize_csvs(CSV_FILES)
    if not csv_stats.empty:
        LOGGER.info("\n%s", csv_stats.to_markdown(index=False))

    if not config.DB_FILE.exists():
        LOGGER.error("Database not found at %s. Run ingest_data.py first.", config.DB_FILE)
        return

    with sqlite3.connect(config.DB_FILE) as conn:
        LOGGER.info("=== TABLE COUNTS ===")
        table_stats = summarize_tables(conn)
        LOGGER.info("\n%s", table_stats.to_markdown(index=False))

        LOGGER.info("=== KPI SNAPSHOTS ===")
        snapshots = fetch_sql_preview(conn, SQL_SNIPPETS)
        for name, df in snapshots.items():
            pretty = df.head(10)
            LOGGER.info("[%s]\n%s", name, pretty.to_markdown(index=False))

    LOGGER.info("Workflow summary complete.")


if __name__ == "__main__":
    main()

