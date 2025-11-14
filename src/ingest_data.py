"""Ingest synthetic CSV datasets into a SQLite database with validation."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

import config
from utils import ensure_directories, get_logger

LOGGER = get_logger("ingest_data")

CSV_TABLE_MAP = {
    "users.csv": "users",
    "products.csv": "products",
    "orders.csv": "orders",
    "order_items.csv": "order_items",
    "payments.csv": "payments",
}


def reset_database(db_path: Path) -> None:
    """Delete the existing database file to ensure a clean build."""
    if db_path.exists():
        LOGGER.info("Removing existing database at %s", db_path)
        db_path.unlink()


def create_schema(connection: sqlite3.Connection) -> None:
    """Create normalized tables with constraints."""
    schema_sql = """
    PRAGMA foreign_keys = ON;

    CREATE TABLE users (
        user_id INTEGER PRIMARY KEY,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        phone TEXT,
        signup_date TEXT,
        loyalty_status TEXT,
        country TEXT
    );

    CREATE TABLE products (
        product_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        category TEXT NOT NULL,
        price REAL NOT NULL,
        stock_qty INTEGER NOT NULL,
        created_at TEXT
    );

    CREATE TABLE orders (
        order_id INTEGER PRIMARY KEY,
        user_id INTEGER NOT NULL REFERENCES users(user_id),
        order_date TEXT NOT NULL,
        status TEXT NOT NULL,
        payment_method TEXT NOT NULL,
        shipping_address TEXT,
        total_amount REAL NOT NULL
    );

    CREATE TABLE order_items (
        order_item_id INTEGER PRIMARY KEY,
        order_id INTEGER NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
        product_id INTEGER NOT NULL REFERENCES products(product_id),
        quantity INTEGER NOT NULL,
        unit_price REAL NOT NULL,
        line_total REAL NOT NULL
    );

    CREATE TABLE payments (
        payment_id INTEGER PRIMARY KEY,
        order_id INTEGER NOT NULL REFERENCES orders(order_id),
        payment_method TEXT NOT NULL,
        amount REAL NOT NULL,
        payment_status TEXT NOT NULL,
        payment_date TEXT NOT NULL,
        transaction_id TEXT UNIQUE NOT NULL
    );
    """
    connection.executescript(schema_sql)
    LOGGER.info("Database schema created successfully.")


def load_csv(connection: sqlite3.Connection, csv_path: Path, table_name: str) -> int:
    """Load a CSV file into the specified table."""
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file missing: {csv_path}")
    df = pd.read_csv(csv_path)
    df.to_sql(table_name, connection, if_exists="append", index=False)
    LOGGER.info("Loaded %s rows into %s", len(df), table_name)
    return len(df)


def validate_counts(connection: sqlite3.Connection) -> dict[str, int]:
    """Return row counts per table for verification."""
    results: dict[str, int] = {}
    for table in CSV_TABLE_MAP.values():
        count = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        results[table] = count
        LOGGER.info("Table %s has %s rows.", table, count)
    return results


def main() -> None:
    """Entry point for rebuilding and loading the SQLite database."""
    ensure_directories()
    reset_database(config.DB_FILE)

    LOGGER.info("Connecting to SQLite at %s", config.DB_FILE)
    with sqlite3.connect(config.DB_FILE) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")
        create_schema(conn)

        for csv_name, table_name in CSV_TABLE_MAP.items():
            csv_path = config.DATA_DIR / csv_name
            load_csv(conn, csv_path, table_name)

        validate_counts(conn)

    LOGGER.info("Data ingestion into SQLite completed successfully.")


if __name__ == "__main__":
    main()

