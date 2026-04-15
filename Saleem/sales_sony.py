"""
Generate 100 Sony India retail sales rows and load into MySQL
database retail_banking, table sony_sales.

Connection defaults match MySQL Workbench Local MySQL80 (localhost:3306, root).
Override with MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD (password required if root has one).
"""

from __future__ import annotations

import os
import random
from datetime import date, timedelta
from decimal import Decimal

try:
    import pymysql
except ImportError as e:  # pragma: no cover
    raise SystemExit("Install PyMySQL: pip install pymysql") from e

BRAND = "Sony"

CATEGORIES = ("TV", "phones", "home_theatres", "other_gadgets")

# Major India branch cities (incl. common spellings you mentioned)
BRANCH_CITIES = (
    "Hyderabad",
    "Chennai",
    "Bengaluru",
    "Kolkata",
    "Mumbai",
    "Delhi",
    "Pune",
    "Ahmedabad",
    "Jaipur",
    "Kochi",
    "Indore",
    "Lucknow",
    "Nagpur",
    "Surat",
    "Visakhapatnam",
    "Chandigarh",
    "Bhopal",
    "Patna",
    "Thiruvananthapuram",
    "Coimbatore",
)

INVENTORY_TYPES = (
    "floor_stock",
    "warehouse",
    "in_transit",
    "serialized",
    "consignment",
    "BOPIS_reserved",
)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS sony_sales (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    brand VARCHAR(32) NOT NULL DEFAULT 'Sony',
    category VARCHAR(32) NOT NULL,
    branch_city VARCHAR(64) NOT NULL,
    transaction_date DATE NOT NULL,
    units_sold INT UNSIGNED NOT NULL,
    unit_price DECIMAL(12, 2) NOT NULL,
    margin_pct DECIMAL(5, 2) NOT NULL COMMENT 'Gross margin % on revenue',
    profit DECIMAL(14, 2) NOT NULL COMMENT 'units * unit_price * margin_pct / 100',
    inventory_type VARCHAR(32) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_sony_sales_txn_date (transaction_date),
    KEY idx_sony_sales_city_cat (branch_city, category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def _price_range(category: str) -> tuple[Decimal, Decimal]:
    """Rough INR price bands by category."""
    bands = {
        "TV": (Decimal("24990.00"), Decimal("289990.00")),
        "phones": (Decimal("14990.00"), Decimal("134990.00")),
        "home_theatres": (Decimal("8990.00"), Decimal("89990.00")),
        "other_gadgets": (Decimal("1990.00"), Decimal("45990.00")),
    }
    return bands[category]


def generate_rows(count: int, seed: int = 42) -> list[tuple]:
    rng = random.Random(seed)
    today = date.today()
    rows: list[tuple] = []
    for _ in range(count):
        category = rng.choice(CATEGORIES)
        low, high = _price_range(category)
        unit_price = Decimal(rng.uniform(float(low), float(high))).quantize(Decimal("0.01"))
        units = rng.randint(1, 25)
        margin_pct = Decimal(rng.uniform(8.0, 32.5)).quantize(Decimal("0.01"))
        revenue = unit_price * units
        profit = (revenue * margin_pct / Decimal("100")).quantize(Decimal("0.01"))
        txn_day = today - timedelta(days=rng.randint(0, 365))
        rows.append(
            (
                BRAND,
                category,
                rng.choice(BRANCH_CITIES),
                txn_day,
                units,
                unit_price,
                margin_pct,
                profit,
                rng.choice(INVENTORY_TYPES),
            )
        )
    return rows


def get_connection():
    # Match MySQL Workbench "Local instance MySQL80": Standard TCP/IP, localhost, 3306, root.
    # Omit database= so CREATE DATABASE / USE retail_banking work when the DB is new.
    # Password: set MYSQL_PASSWORD (PyCharm Run → Environment variables), same value as Workbench vault.
    return pymysql.connect(
        host=os.environ.get("MYSQL_HOST", "localhost"),
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        user=os.environ.get("MYSQL_USER", "root"),
        password=os.environ.get("MYSQL_PASSWORD", ""),
        charset="utf8mb4",
        autocommit=False,
        connect_timeout=10,
    )


def ensure_schema(cur) -> None:
    cur.execute(
        "CREATE DATABASE IF NOT EXISTS retail_banking "
        "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
    )
    cur.execute("USE retail_banking")


def main(row_count: int = 100) -> None:
    rows = generate_rows(row_count)
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            ensure_schema(cur)
            cur.execute(CREATE_TABLE_SQL)
            cur.execute("DELETE FROM sony_sales")
            insert_sql = (
                "INSERT INTO sony_sales "
                "(brand, category, branch_city, transaction_date, units_sold, "
                "unit_price, margin_pct, profit, inventory_type) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            )
            cur.executemany(insert_sql, rows)
        conn.commit()
        print(f"Loaded {row_count} rows into retail_banking.sony_sales.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main(100)
