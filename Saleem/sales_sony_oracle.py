#!/usr/bin/env python3
"""
Copy rows id 1..350 from MySQL retail_banking.sony_sales into Oracle TEST_USER.SALES.

If Oracle SALES only has ID, missing columns are added (ALTER TABLE) to mirror sony_sales.

Env (optional overrides):
  MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD
  ORACLE_HOST, ORACLE_PORT, ORACLE_SERVICE, ORACLE_USER, ORACLE_PASSWORD

Usage:
  python sales_sony_oracle.py
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from typing import Any, Sequence

try:
    import pymysql
except ImportError as e:
    raise SystemExit("Install PyMySQL: pip install pymysql") from e

try:
    import oracledb
except ImportError as e:
    raise SystemExit("Install oracledb: pip install oracledb") from e

# ─── Row range (inclusive) ───────────────────────────────────────────────────
START_ID = 1
END_ID = 350

# ─── MySQL ───────────────────────────────────────────────────────────────────
MYSQL_CFG = dict(
    host=os.environ.get("MYSQL_HOST", "localhost"),
    port=int(os.environ.get("MYSQL_PORT", "3306")),
    user=os.environ.get("MYSQL_USER", "root"),
    password=os.environ.get("MYSQL_PASSWORD", ""),
    database="retail_banking",
    charset="utf8mb4",
)

# ─── Oracle ─────────────────────────────────────────────────────────────────
ORACLE_CFG = dict(
    host=os.environ.get("ORACLE_HOST", "localhost"),
    port=int(os.environ.get("ORACLE_PORT", "1521")),
    service_name=os.environ.get("ORACLE_SERVICE", "XEPDB1"),
    user=os.environ.get("ORACLE_USER", "test_user"),
    password=os.environ.get("ORACLE_PASSWORD", ""),
)

# Columns to add if missing (Oracle). ID is assumed to exist.
_ORACLE_EXTRA_COLS: tuple[tuple[str, str], ...] = (
    ("BRAND", "VARCHAR2(32 CHAR)"),
    ("CATEGORY", "VARCHAR2(32 CHAR)"),
    ("BRANCH_CITY", "VARCHAR2(64 CHAR)"),
    ("TRANSACTION_DATE", "DATE"),
    ("UNITS_SOLD", "NUMBER(10,0)"),
    ("UNIT_PRICE", "NUMBER(12,2)"),
    ("MARGIN_PCT", "NUMBER(5,2)"),
    ("PROFIT", "NUMBER(14,2)"),
    ("INVENTORY_TYPE", "VARCHAR2(32 CHAR)"),
    ("CREATED_AT", "TIMESTAMP(6)"),
)

_INSERT_COLS = (
    "id",
    "brand",
    "category",
    "branch_city",
    "transaction_date",
    "units_sold",
    "unit_price",
    "margin_pct",
    "profit",
    "inventory_type",
    "created_at",
)


def _oracle_cols_upper(conn: oracledb.Connection) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM user_tab_columns "
            "WHERE table_name = 'SALES' ORDER BY column_id"
        )
        return {r[0].upper() for r in cur.fetchall()}


def ensure_oracle_sales_columns(conn: oracledb.Connection) -> None:
    existing = _oracle_cols_upper(conn)
    if "ID" not in existing:
        raise RuntimeError("Oracle table SALES must have an ID column.")
    with conn.cursor() as cur:
        for col_name, col_type in _ORACLE_EXTRA_COLS:
            if col_name.upper() in existing:
                continue
            # Unquoted identifiers -> uppercase column names in Oracle
            cur.execute(f"ALTER TABLE sales ADD ({col_name} {col_type})")
    conn.commit()


def fetch_mysql_rows() -> list[tuple[Any, ...]]:
    sql = (
        "SELECT id, brand, category, branch_city, transaction_date, "
        "units_sold, unit_price, margin_pct, profit, inventory_type, created_at "
        "FROM sony_sales WHERE id >= %s AND id <= %s ORDER BY id"
    )
    conn = pymysql.connect(**MYSQL_CFG)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (START_ID, END_ID))
            rows = cur.fetchall()
    finally:
        conn.close()
    return list(rows)


def _normalize_row(row: tuple[Any, ...]) -> dict[str, Any]:
    """Map MySQL row to Oracle bind dict (SONY_SALES column order)."""
    names = _INSERT_COLS
    out: dict[str, Any] = {}
    for name, val in zip(names, row, strict=True):
        if val is None:
            out[name] = None
        elif isinstance(val, (bytes, bytearray)):
            out[name] = val.decode("utf-8", errors="replace")
        elif isinstance(val, datetime):
            out[name] = val.replace(tzinfo=None) if val.tzinfo else val
        else:
            out[name] = val
    return out


def load_oracle(rows: Sequence[tuple[Any, ...]]) -> None:
    dsn = oracledb.makedsn(
        ORACLE_CFG["host"],
        ORACLE_CFG["port"],
        service_name=ORACLE_CFG["service_name"],
    )
    conn = oracledb.connect(
        user=ORACLE_CFG["user"],
        password=ORACLE_CFG["password"],
        dsn=dsn,
    )
    try:
        ensure_oracle_sales_columns(conn)
        col_list = ", ".join(c.upper() for c in _INSERT_COLS)
        ins = (
            f"INSERT INTO sales ({col_list}) VALUES ("
            + ", ".join(f":{c}" for c in _INSERT_COLS)
            + ")"
        )
        binds = [_normalize_row(r) for r in rows]
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM sales WHERE id BETWEEN {START_ID} AND {END_ID}"
            )
            cur.executemany(ins, binds)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> int:
    if not MYSQL_CFG["password"]:
        print("Set MYSQL_PASSWORD (or pass via env).", file=sys.stderr)
        return 1
    if not ORACLE_CFG["password"]:
        print("Set ORACLE_PASSWORD (or pass via env).", file=sys.stderr)
        return 1

    print(f"MySQL: fetch sony_sales id {START_ID}..{END_ID} ...")
    rows = fetch_mysql_rows()
    if not rows:
        print("No rows returned from MySQL. Check sony_sales has data in that id range.")
        return 1
    if len(rows) < (END_ID - START_ID + 1):
        print(f"Warning: expected up to {END_ID - START_ID + 1} rows, got {len(rows)}.")

    print(f"Oracle: ensure SALES columns, delete id {START_ID}..{END_ID}, insert {len(rows)} rows ...")
    load_oracle(rows)
    print(f"Done. Loaded {len(rows)} rows into Oracle sales.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
