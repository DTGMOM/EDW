#!/usr/bin/env python3
"""
Validate MySQL data vs Oracle data row-by-row.

MySQL  : retail_banking.sony_sales  (localhost:3306, root)
Oracle : configure via env vars below

Usage:
    python validate_mysql_vs_oracle.py

Env vars (override defaults):
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD
    ORACLE_HOST, ORACLE_PORT, ORACLE_SERVICE, ORACLE_USER, ORACLE_PASSWORD
"""

from __future__ import annotations

import os
import sys

try:
    import pymysql
except ImportError:
    raise SystemExit("Install PyMySQL:  pip install pymysql")

try:
    import oracledb
except ImportError:
    raise SystemExit("Install oracledb:  pip install oracledb")

# ─── MySQL config ────────────────────────────────────────────────────────────
MYSQL_CFG = dict(
    host=os.environ.get("MYSQL_HOST", "localhost"),
    port=int(os.environ.get("MYSQL_PORT", "3306")),
    user=os.environ.get("MYSQL_USER", "root"),
    password=os.environ.get("MYSQL_PASSWORD", ""),
    database="retail_banking",
    charset="utf8mb4",
)

# ─── Oracle config ───────────────────────────────────────────────────────────
ORACLE_CFG = dict(
    host=os.environ.get("ORACLE_HOST", "localhost"),
    port=int(os.environ.get("ORACLE_PORT", "1521")),
    service_name=os.environ.get("ORACLE_SERVICE", "ORCL"),
    user=os.environ.get("ORACLE_USER", "system"),
    password=os.environ.get("ORACLE_PASSWORD", ""),
)

# ─── Tables & columns to compare ────────────────────────────────────────────
MYSQL_TABLE  = "sony_sales"
ORACLE_TABLE = "SONY_SALES"       # adjust schema prefix if needed e.g. "EDW.SONY_SALES"
COMPARE_COLS = [
    "brand", "category", "branch_city", "transaction_date",
    "units_sold", "unit_price", "margin_pct", "profit", "inventory_type",
]
ORDER_BY_COL = "id"               # column used to sort both sides for alignment


# ─── Helpers ─────────────────────────────────────────────────────────────────

def fetch_mysql(cols: list[str]) -> list[tuple]:
    conn = pymysql.connect(**MYSQL_CFG)
    try:
        with conn.cursor() as cur:
            col_str = ", ".join(cols)
            cur.execute(
                f"SELECT {col_str} FROM {MYSQL_TABLE} ORDER BY {ORDER_BY_COL}"
            )
            return cur.fetchall()
    finally:
        conn.close()


def fetch_oracle(cols: list[str]) -> list[tuple]:
    dsn = oracledb.makedsn(
        ORACLE_CFG["host"],
        ORACLE_CFG["port"],
        service_name=ORACLE_CFG["service_name"],
    )
    conn = oracledb.connect(user=ORACLE_CFG["user"],
                             password=ORACLE_CFG["password"],
                             dsn=dsn)
    try:
        with conn.cursor() as cur:
            col_str = ", ".join(cols)
            cur.execute(
                f"SELECT {col_str} FROM {ORACLE_TABLE} ORDER BY {ORDER_BY_COL}"
            )
            return cur.fetchall()
    finally:
        conn.close()


def normalize(val):
    """Convert values to comparable strings."""
    if val is None:
        return "NULL"
    return str(val).strip()


# ─── Main validation ─────────────────────────────────────────────────────────

def validate() -> int:
    print("=" * 60)
    print("  MySQL vs Oracle Data Validation")
    print("  Table : sony_sales")
    print("  Cols  :", ", ".join(COMPARE_COLS))
    print("=" * 60)

    # ── fetch ────────────────────────────────────────────────────
    print("\n[1/3] Connecting to MySQL...")
    try:
        mysql_rows = fetch_mysql(COMPARE_COLS)
        print(f"      Fetched {len(mysql_rows)} rows from MySQL.")
    except Exception as e:
        print(f"      [FAIL] MySQL connection error: {e}")
        return 1

    print("[2/3] Connecting to Oracle...")
    try:
        oracle_rows = fetch_oracle(COMPARE_COLS)
        print(f"      Fetched {len(oracle_rows)} rows from Oracle.")
    except Exception as e:
        print(f"      [FAIL] Oracle connection error: {e}")
        return 1

    # ── row count check ───────────────────────────────────────────
    print("\n[3/3] Comparing data...")
    print(f"\n  Row count  →  MySQL: {len(mysql_rows)}  |  Oracle: {len(oracle_rows)}")

    if len(mysql_rows) != len(oracle_rows):
        print("  [FAIL] Row count mismatch — cannot do line-by-line compare.")
        return 1
    print("  [PASS] Row counts match.\n")

    # ── cell-by-cell compare ──────────────────────────────────────
    failures = 0
    for i, (m_row, o_row) in enumerate(zip(mysql_rows, oracle_rows)):
        row_ok = True
        for j, col in enumerate(COMPARE_COLS):
            m_val = normalize(m_row[j])
            o_val = normalize(o_row[j])
            if m_val != o_val:
                print(f"  [FAIL] row {i:03d} | col={col:20s} | MySQL={m_val!r:30s} | Oracle={o_val!r}")
                failures += 1
                row_ok = False
        if row_ok:
            print(f"  [PASS] row {i:03d}")

    # ── summary ───────────────────────────────────────────────────
    print("\n" + "=" * 60)
    if failures == 0:
        print(f"  RESULT : ALL {len(mysql_rows)} ROWS MATCH ✓")
    else:
        print(f"  RESULT : {failures} MISMATCH(ES) FOUND ✗")
    print("=" * 60)
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(validate())
