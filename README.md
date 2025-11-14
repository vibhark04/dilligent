# Synthetic E-Commerce Data Pipeline

This project demonstrates an end-to-end agentic SDLC workflow for generating, ingesting, and analyzing synthetic e-commerce data using Cursor IDE.

## Project Structure
- `data/` – auto-generated CSV datasets.
- `src/` – Python modules for data generation, ingestion, analytics, and reporting.
- `db/` – SQLite database file (`ecom.db`).
- `sql/` – Analytical SQL scripts.
- `README.md` – documentation.

## Workflow Overview
1. **Generate synthetic data** with `src/generate_data.py` using Faker and pandas. The script produces `users.csv`, `products.csv`, `orders.csv`, `order_items.csv`, and `payments.csv` with referential integrity.
2. **Ingest into SQLite** via `src/ingest_data.py`, which rebuilds the schema from scratch, loads CSVs, enforces foreign keys, and validates row counts.
3. **Run analytics** with SQL files inside `sql/` executed through `src/run_queries.py`. Each SQL statement answers a specific business question (revenue per user, top products, monthly sales, payment distribution).
4. **Summarize** the entire process using `src/report.py`, which inspects CSV stats, database row counts, and highlights analytics results to confirm the pipeline health.

## Getting Started
```bash
python -m venv .venv
.\.venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

## Commands
- `python src/generate_data.py` – regenerate CSV datasets (idempotent).
- `python src/ingest_data.py` – rebuild database and load data from CSV.
- `python src/run_queries.py` – execute SQL analytics and pretty-print results.
- `python src/report.py` – produce a workflow summary (CSV counts, DB counts, KPI snippets).

## Agentic SDLC Notes
- **Plan** – `README.md` plus `src/config.py` capture requirements and tunable parameters.
- **Build** – modular scripts inside `src/` create data, database, and analytics artifacts.
- **Verify** – ingestion validates row counts; SQL runner logs execution status; report script summarizes KPIs.
- **Operate** – logging across scripts and GitHub-ready structure enable quick troubleshooting and deployment.

## GitHub Preparation
This repo is initialized with `git init` and includes a `.gitignore` covering virtual environments, cache files, the SQLite database, and generated CSVs.
