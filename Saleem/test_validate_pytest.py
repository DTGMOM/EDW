"""
Pytest validation: MySQL retail_banking.sony_sales  vs  Oracle TEST_USER.SALES
Scenarios:
  1. Count check  — row count must match between both tables
  2. Primary key  — no NULLs, no duplicates in each table's PK column
"""

import pymysql
import oracledb
import pytest


# ──────────────────────────────────────────────
# Helpers / fixtures
# ──────────────────────────────────────────────

def get_mysql_conn():
    return pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="Admin123",
        database="retail_banking",
        charset="utf8mb4",
        connect_timeout=10,
    )


def get_oracle_conn():
    return oracledb.connect(
        user="TEST_USER",
        password="Admin123",
        dsn="localhost:1521/XEPDB1",
    )


@pytest.fixture(scope="session")
def mysql_count():
    conn = get_mysql_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM sony_sales")
        count = cur.fetchone()[0]
    conn.close()
    return count


@pytest.fixture(scope="session")
def oracle_count():
    conn = get_oracle_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM SALES")
        count = cur.fetchone()[0]
    conn.close()
    return count


@pytest.fixture(scope="session")
def mysql_pk_ids():
    conn = get_mysql_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM sony_sales")
        ids = [row[0] for row in cur.fetchall()]
    conn.close()
    return ids


@pytest.fixture(scope="session")
def oracle_pk_ids():
    conn = get_oracle_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT ID FROM SALES")
        ids = [row[0] for row in cur.fetchall()]
    conn.close()
    return ids


# ──────────────────────────────────────────────
# Scenario 1: Count Check
# ──────────────────────────────────────────────

class TestCountCheck:

    def test_mysql_row_count_not_zero(self, mysql_count):
        """MySQL sony_sales must have at least 1 row."""
        print(f"\n  MySQL sony_sales row count: {mysql_count}")
        assert mysql_count > 0, f"MySQL sony_sales is empty! Got {mysql_count} rows."

    def test_oracle_row_count_not_zero(self, oracle_count):
        """Oracle SALES must have at least 1 row."""
        print(f"\n  Oracle SALES row count: {oracle_count}")
        assert oracle_count > 0, f"Oracle SALES is empty! Got {oracle_count} rows."

    def test_count_match_between_tables(self, mysql_count, oracle_count):
        """Row count must match between MySQL sony_sales and Oracle SALES."""
        print(f"\n  MySQL count  : {mysql_count}")
        print(f"  Oracle count : {oracle_count}")
        print(f"  Difference   : {abs(mysql_count - oracle_count)}")
        assert mysql_count == oracle_count, (
            f"COUNT MISMATCH: MySQL={mysql_count}, Oracle={oracle_count} "
            f"(diff={abs(mysql_count - oracle_count)})"
        )


# ──────────────────────────────────────────────
# Scenario 2: Primary Key Validation
# ──────────────────────────────────────────────

class TestPrimaryKeyValidation:

    def test_mysql_pk_no_nulls(self, mysql_pk_ids):
        """MySQL sony_sales.id must have no NULL values."""
        nulls = [i for i in mysql_pk_ids if i is None]
        print(f"\n  MySQL NULL PKs found: {len(nulls)}")
        assert len(nulls) == 0, f"MySQL sony_sales has {len(nulls)} NULL id values!"

    def test_mysql_pk_no_duplicates(self, mysql_pk_ids):
        """MySQL sony_sales.id must have no duplicate values."""
        duplicates = len(mysql_pk_ids) - len(set(mysql_pk_ids))
        print(f"\n  MySQL duplicate PKs found: {duplicates}")
        assert duplicates == 0, (
            f"MySQL sony_sales has {duplicates} duplicate id values!"
        )

    def test_oracle_pk_no_nulls(self, oracle_pk_ids):
        """Oracle SALES.ID must have no NULL values."""
        nulls = [i for i in oracle_pk_ids if i is None]
        print(f"\n  Oracle NULL PKs found: {len(nulls)}")
        assert len(nulls) == 0, f"Oracle SALES has {len(nulls)} NULL ID values!"

    def test_oracle_pk_no_duplicates(self, oracle_pk_ids):
        """Oracle SALES.ID must have no duplicate values."""
        duplicates = len(oracle_pk_ids) - len(set(oracle_pk_ids))
        print(f"\n  Oracle duplicate PKs found: {duplicates}")
        assert duplicates == 0, (
            f"Oracle SALES has {duplicates} duplicate ID values!"
        )

    def test_mysql_pk_all_positive(self, mysql_pk_ids):
        """MySQL sony_sales.id must all be positive integers."""
        invalid = [i for i in mysql_pk_ids if i is not None and i <= 0]
        print(f"\n  MySQL non-positive PKs: {len(invalid)}")
        assert len(invalid) == 0, f"MySQL has {len(invalid)} non-positive id values!"

    def test_oracle_pk_all_positive(self, oracle_pk_ids):
        """Oracle SALES.ID must all be positive integers."""
        invalid = [i for i in oracle_pk_ids if i is not None and i <= 0]
        print(f"\n  Oracle non-positive PKs: {len(invalid)}")
        assert len(invalid) == 0, f"Oracle has {len(invalid)} non-positive ID values!"
