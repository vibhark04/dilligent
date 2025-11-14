"""Global configuration constants for the synthetic e-commerce pipeline."""
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DB_DIR = BASE_DIR / "db"
SQL_DIR = BASE_DIR / "sql"
DB_FILE = DB_DIR / "ecom.db"

# Data generation settings
RANDOM_SEED = 42
NUM_USERS = 200
NUM_PRODUCTS = 80
NUM_ORDERS = 600
MAX_ITEMS_PER_ORDER = 5
PAYMENT_METHODS = ["card", "upi", "cod", "net_banking"]
ORDER_STATUSES = ["pending", "shipped", "delivered", "cancelled"]
PAYMENT_STATUSES = ["initiated", "completed", "failed", "refunded"]
