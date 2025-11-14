"""Utility helpers for logging and filesystem operations."""
from __future__ import annotations

import logging
from pathlib import Path

import config


def ensure_directories() -> None:
    """Ensure all required directories exist."""
    for directory in (config.DATA_DIR, config.DB_DIR, config.SQL_DIR):
        Path(directory).mkdir(parents=True, exist_ok=True)


def get_logger(name: str) -> logging.Logger:
    """Configure and return a module-specific logger."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    return logging.getLogger(name)
