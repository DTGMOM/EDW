"""
Generate 300 additional Sony India retail sales rows and INSERT into MySQL
database retail_banking, table sony_sales — without deleting existing rows.

Connection defaults: localhost:3306, root.
Override with MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD env vars.
"""

from __future__ import annotations

import os
import random
from datetime import date, timedelta
from decimal import Decimal

try:
    import pymysql
except ImportError as e:
    raise SystemExit("Install PyMySQL: pip install pymysql") from e

BRAND = "Sony"

CATEGORIES = ("TV", "phones", "home_theatres", "other_gadgets")

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


def _price_range(category: str) -> tuple[Decimal, Decimal]:
    """Rough INR price bands by category."""
    bands = {
        "TV":             (Decimal("24990.00"), Decimal("289990.00")),
        "phones":         (Decimal("14990.00"), Decimal("134990.00")),
        "home_theatres":  (Decimal("8990.00"),  Decimal("89990.00")),
        "other_gadgets":  (Decimal("1990.00"),  Decimal("45990.00")),
    }
    return bands[category]


def generate_rows(count: int, seed: int = 99) -> list[tuple]:
    """Generate `count` rows with a different seed than sales_sony.py (42)."""
    rng = random.Random(seed)
    today = date.today()
    rows: list[tuple] = []
    for _ in range(count):
        category  = rng.choice(CATEGORIES)
        low, high = _price_range(category)
        unit_price = Decimal(rng.uniform(float(low), float(high))).quantize(Decimal("0.01"))
        units      = rng.randint(1, 25)
        margin_pct = Decimal(rng.uniform(8.0, 32.5)).quantize(Decimal("0.01"))
        revenue    = unit_price * units
        profit     = (revenue * margin_pct / Decimal("100")).quantize(Decimal("0.01"))
        txn_day    = today - timedelta(days=rng.randint(0, 365))
        rows.append((
            BRAND,
            category,
            rng.choice(BRANCH_CITIES),
            txn_day,
            units,
            unit_price,
            margin_pct,
            profit,
            rng.choice(INVENTORY_TYPES),
        ))
    return rows


def get_connection():
    return pymysql.connect(
        host=os.environ.get("MYSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        user=os.environ.get("MYSQL_USER", "root"),
        password=os.environ.get("MYSQL_PASSWORD", "Admin123"),
        database="retail_banking",
        charset="utf8mb4",
        autocommit=False,
        connect_timeout=10,
    )


def main(row_count: int = 300) -> None:
    rows = generate_rows(row_count)
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Check existing row count before insert
            cur.execute("SELECT COUNT(*) FROM sony_sales")
            before = cur.fetchone()[0]

            insert_sql = (
                "INSERT INTO sony_sales "
                "(brand, category, branch_city, transaction_date, units_sold, "
                "unit_price, margin_pct, profit, inventory_type) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            )
            cur.executemany(insert_sql, rows)

            # Check row count after insert
            cur.execute("SELECT COUNT(*) FROM sony_sales")
            after = cur.fetchone()[0]

        conn.commit()
        print(f"Rows before insert : {before}")
        print(f"Rows inserted      : {row_count}")
        print(f"Rows after insert  : {after}")
        print(f"Successfully added {row_count} rows into retail_banking.sony_sales.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main(300)
