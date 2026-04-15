"""
Great Expectations-style validation (pandas-based implementation).
Runs on Python 3.14 — mirrors GE expectation names, output format, and structure.

Scenarios:
  1. Count check  — row count must match between MySQL sony_sales & Oracle SALES
  2. Primary key  — no NULLs, no duplicates in each table's PK column

To use the real GX library, install Python 3.12/3.13 and:
    pip install great-expectations pymysql oracledb pandas
"""

import pymysql
import oracledb
import pandas as pd
from dataclasses import dataclass, field
from typing import Any


# ──────────────────────────────────────────────
# GE-style core classes
# ──────────────────────────────────────────────

@dataclass
class ExpectationResult:
    expectation_type: str
    success: bool
    scenario: str
    description: str
    result: dict = field(default_factory=dict)


@dataclass
class ValidationResult:
    suite_name: str
    results: list = field(default_factory=list)

    @property
    def success(self) -> bool:
        return all(r.success for r in self.results)

    @property
    def statistics(self) -> dict:
        total  = len(self.results)
        passed = sum(1 for r in self.results if r.success)
        return {
            "evaluated_expectations":    total,
            "successful_expectations":   passed,
            "unsuccessful_expectations": total - passed,
        }


# ──────────────────────────────────────────────
# GE-style Expectation runners
# ──────────────────────────────────────────────

def expect_table_row_count_to_equal(df, value, scenario, desc):
    actual  = len(df)
    success = actual == value
    return ExpectationResult(
        expectation_type="expect_table_row_count_to_equal",
        success=success, scenario=scenario, description=desc,
        result={"observed_value": actual, "expected_value": value}
    )


def expect_column_values_to_not_be_null(df, column, scenario, desc):
    null_count = int(df[column].isnull().sum())
    return ExpectationResult(
        expectation_type="expect_column_values_to_not_be_null",
        success=null_count == 0, scenario=scenario, description=desc,
        result={"null_count": null_count, "total_rows": len(df)}
    )


def expect_column_values_to_be_unique(df, column, scenario, desc):
    dup_count = int(df[column].duplicated().sum())
    return ExpectationResult(
        expectation_type="expect_column_values_to_be_unique",
        success=dup_count == 0, scenario=scenario, description=desc,
        result={"duplicate_count": dup_count, "total_rows": len(df)}
    )


def expect_column_values_to_be_between(df, column, min_value, scenario, desc):
    invalid = int((df[column] < min_value).sum())
    return ExpectationResult(
        expectation_type="expect_column_values_to_be_between",
        success=invalid == 0, scenario=scenario, description=desc,
        result={"invalid_count": invalid, "min_value": min_value}
    )


# ──────────────────────────────────────────────
# Connection helpers
# ──────────────────────────────────────────────

def get_mysql_df():
    conn = pymysql.connect(
        host="127.0.0.1", port=3306,
        user="root", password="Admin123",
        database="retail_banking",
        charset="utf8mb4", connect_timeout=10,
    )
    df = pd.read_sql("SELECT * FROM sony_sales", conn)
    conn.close()
    return df


def get_oracle_df():
    conn = oracledb.connect(
        user="TEST_USER", password="Admin123",
        dsn="localhost:1521/XEPDB1",
    )
    df = pd.read_sql("SELECT * FROM SALES", conn)
    conn.close()
    return df


# ──────────────────────────────────────────────
# Expectation Suites
# ──────────────────────────────────────────────

def validate_mysql(df, expected_count):
    vr = ValidationResult(suite_name="mysql_sony_sales_suite")
    pk = "id"
    vr.results.append(expect_table_row_count_to_equal(
        df, expected_count, "Count Check", f"Table must have {expected_count} rows"))
    vr.results.append(expect_column_values_to_not_be_null(
        df, pk, "Primary Key", f"{pk} must not be NULL"))
    vr.results.append(expect_column_values_to_be_unique(
        df, pk, "Primary Key", f"{pk} must be unique"))
    vr.results.append(expect_column_values_to_be_between(
        df, pk, 1, "Primary Key", f"{pk} must be >= 1"))
    return vr


def validate_oracle(df, expected_count):
    vr = ValidationResult(suite_name="oracle_sales_suite")
    df.columns = [c.upper() for c in df.columns]
    pk = "ID"
    vr.results.append(expect_table_row_count_to_equal(
        df, expected_count, "Count Check", f"Table must have {expected_count} rows"))
    vr.results.append(expect_column_values_to_not_be_null(
        df, pk, "Primary Key", f"{pk} must not be NULL"))
    vr.results.append(expect_column_values_to_be_unique(
        df, pk, "Primary Key", f"{pk} must be unique"))
    vr.results.append(expect_column_values_to_be_between(
        df, pk, 1, "Primary Key", f"{pk} must be >= 1"))
    return vr


# ──────────────────────────────────────────────
# Print results
# ──────────────────────────────────────────────

def print_validation_results(vr, table_label):
    sep = "=" * 65
    s   = vr.statistics
    print(f"\n{sep}")
    print(f"  Validation Suite : {vr.suite_name}")
    print(f"  Table            : {table_label}")
    print(f"  Overall Success  : {'PASS' if vr.success else 'FAIL'}")
    print(f"  Evaluated        : {s['evaluated_expectations']}")
    print(f"  Passed           : {s['successful_expectations']}")
    print(f"  Failed           : {s['unsuccessful_expectations']}")
    print(f"\n  {'Scenario':<15} {'Status':<8} Expectation Type")
    print(f"  {'-'*60}")
    for r in vr.results:
        status = "PASS" if r.success else "FAIL"
        print(f"  {r.scenario:<15} [{status}]   {r.expectation_type}")
        if not r.success:
            print(f"               --> {r.result}")
    print(sep)


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main():
    print("\nLoading MySQL retail_banking.sony_sales ...")
    mysql_df = get_mysql_df()
    print(f"  Rows loaded : {len(mysql_df)}")

    print("\nLoading Oracle TEST_USER.SALES ...")
    oracle_df = get_oracle_df()
    print(f"  Rows loaded : {len(oracle_df)}")

    expected_count = len(mysql_df)

    mysql_vr  = validate_mysql(mysql_df,  expected_count)
    oracle_vr = validate_oracle(oracle_df, expected_count)

    print_validation_results(mysql_vr,  "MySQL  — retail_banking.sony_sales")
    print_validation_results(oracle_vr, "Oracle — TEST_USER.SALES")

    # Cross-table count summary
    sep  = "=" * 65
    diff = abs(len(mysql_df) - len(oracle_df))
    print(f"\n{sep}")
    print(f"  Cross-Table Count Comparison")
    print(f"  {'MySQL  sony_sales':<25}: {len(mysql_df)} rows")
    print(f"  {'Oracle SALES':<25}: {len(oracle_df)} rows")
    if diff == 0:
        print(f"  [PASS] Row counts MATCH between MySQL and Oracle")
    else:
        print(f"  [FAIL] Row counts DO NOT MATCH — difference = {diff} rows")
    print(sep)


if __name__ == "__main__":
    main()
