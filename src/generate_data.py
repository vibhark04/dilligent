"""Generate synthetic e-commerce datasets using Faker and pandas."""
from __future__ import annotations

import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

import config
from utils import ensure_directories, get_logger

LOGGER = get_logger("generate_data")
faker = Faker()


def seed_everything() -> None:
    """Seed randomness across Faker and Python's random module."""
    Faker.seed(config.RANDOM_SEED)
    random.seed(config.RANDOM_SEED)


def generate_users(num_users: int) -> pd.DataFrame:
    """Create a dataframe of synthetic users."""
    loyalty_levels = ["bronze", "silver", "gold", "platinum"]
    rows = []
    for user_id in range(1, num_users + 1):
        profile = {
            "user_id": user_id,
            "first_name": faker.first_name(),
            "last_name": faker.last_name(),
            "email": faker.unique.email(),
            "phone": faker.unique.phone_number(),
            "signup_date": faker.date_between(start_date="-2y", end_date="today"),
            "loyalty_status": random.choices(loyalty_levels, weights=[0.4, 0.3, 0.2, 0.1])[0],
            "country": faker.country(),
        }
        rows.append(profile)
    return pd.DataFrame(rows)


def generate_products(num_products: int) -> pd.DataFrame:
    """Create a dataframe of synthetic products."""
    categories = ["electronics", "fashion", "home", "beauty", "sports", "books"]
    rows = []
    for product_id in range(1, num_products + 1):
        price = round(random.uniform(10, 800), 2)
        rows.append(
            {
                "product_id": product_id,
                "name": f"{faker.word().title()} {faker.word().title()}",
                "category": random.choice(categories),
                "price": price,
                "stock_qty": random.randint(10, 500),
                "created_at": faker.date_between(start_date="-3y", end_date="today"),
            }
        )
    return pd.DataFrame(rows)


def generate_orders(num_orders: int, users_df: pd.DataFrame) -> pd.DataFrame:
    """Create orders referencing existing users."""
    rows = []
    for order_id in range(1, num_orders + 1):
        user = users_df.sample(1).iloc[0]
        order_dt = faker.date_time_between(start_date="-1y", end_date="now")
        status = random.choices(
            config.ORDER_STATUSES,
            weights=[0.2, 0.4, 0.35, 0.05],
        )[0]
        payment_method = random.choice(config.PAYMENT_METHODS)
        rows.append(
            {
                "order_id": order_id,
                "user_id": int(user.user_id),
                "order_date": order_dt,
                "status": status,
                "payment_method": payment_method,
                "shipping_address": faker.address().replace("\n", ", "),
                "total_amount": 0.0,  # updated after generating order items
            }
        )
    return pd.DataFrame(rows)


def generate_order_items(
    orders_df: pd.DataFrame, products_df: pd.DataFrame
) -> pd.DataFrame:
    """Create order items referencing orders and products."""
    rows = []
    order_item_id = 1
    product_ids = products_df["product_id"].tolist()
    product_prices = dict(
        zip(products_df["product_id"].tolist(), products_df["price"].tolist())
    )
    for _, order in orders_df.iterrows():
        num_items = random.randint(1, config.MAX_ITEMS_PER_ORDER)
        selected_products = random.sample(product_ids, k=num_items)
        for product_id in selected_products:
            quantity = random.randint(1, 4)
            unit_price = float(product_prices[product_id])
            line_total = round(unit_price * quantity, 2)
            rows.append(
                {
                    "order_item_id": order_item_id,
                    "order_id": int(order.order_id),
                    "product_id": int(product_id),
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "line_total": line_total,
                }
            )
            order_item_id += 1
    order_items_df = pd.DataFrame(rows)
    totals = order_items_df.groupby("order_id")["line_total"].sum()
    orders_df.loc[:, "total_amount"] = orders_df["order_id"].map(totals).fillna(0.0)
    orders_df["total_amount"] = orders_df["total_amount"].round(2)
    return order_items_df


def generate_payments(orders_df: pd.DataFrame) -> pd.DataFrame:
    """Create payment records tied to orders."""
    rows = []
    for order in orders_df.itertuples(index=False):
        status = (
            "refunded"
            if order.status == "cancelled"
            else random.choices(
                config.PAYMENT_STATUSES,
                weights=[0.1, 0.75, 0.1, 0.05],
            )[0]
        )
        payment_date = (
            order.order_date + timedelta(minutes=random.randint(10, 240))
            if isinstance(order.order_date, datetime)
            else faker.date_time_between(start_date="-1y", end_date="now")
        )
        rows.append(
            {
                "payment_id": order.order_id,
                "order_id": int(order.order_id),
                "payment_method": order.payment_method,
                "amount": float(order.total_amount),
                "payment_status": status,
                "payment_date": payment_date,
                "transaction_id": faker.unique.bothify(text="PAY#######"),
            }
        )
    return pd.DataFrame(rows)


def write_csv(df: pd.DataFrame, filename: str) -> Path:
    """Persist dataframe to the data directory."""
    output_path = config.DATA_DIR / filename
    df.to_csv(output_path, index=False)
    LOGGER.info("Wrote %s rows to %s", len(df), output_path)
    return output_path


def main() -> None:
    """Entry point for generating all datasets."""
    ensure_directories()
    seed_everything()

    LOGGER.info("Generating synthetic datasets ...")
    users_df = generate_users(config.NUM_USERS)
    products_df = generate_products(config.NUM_PRODUCTS)
    orders_df = generate_orders(config.NUM_ORDERS, users_df)
    order_items_df = generate_order_items(orders_df, products_df)
    payments_df = generate_payments(orders_df)

    write_csv(users_df, "users.csv")
    write_csv(products_df, "products.csv")
    write_csv(orders_df, "orders.csv")
    write_csv(order_items_df, "order_items.csv")
    write_csv(payments_df, "payments.csv")

    LOGGER.info("Synthetic data generation complete.")


if __name__ == "__main__":
    main()

