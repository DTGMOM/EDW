#!/usr/bin/env python3
"""
Validate .csv / .json extensions and compare row-by-row (same logical 500x25 dataset).

  python validate_csv_json_files.py --generate   # write sample_500x25.csv + .json
  python validate_csv_json_files.py --compare  # extension + structure + line-by-line row match

Env: none required. Uses UTF-8.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import sys
from pathlib import Path

HEADERS = [f"col_{i:02d}" for i in range(1, 26)]
ROWS = 500
CSV_NAME = "sample_500x25.csv"
JSON_NAME = "sample_500x25.json"
BASE_DIR = Path(__file__).resolve().parent


def validate_extension(path: Path, expected: str) -> tuple[bool, str]:
    ext = path.suffix.lower()
    exp = expected.lower()
    ok = ext == exp
    return ok, f"path={path.name} suffix={ext!r} expected={exp!r}"


def generate_rows(seed: int = 42) -> list[list[str]]:
    rng = random.Random(seed)
    rows: list[list[str]] = []
    for r in range(ROWS):
        row = []
        for c in range(25):
            # short stable strings for diff-friendly output
            row.append(f"r{r:03d}_{HEADERS[c]}_{rng.randint(0, 9999):04d}")
        rows.append(row)
    return rows


def write_samples() -> None:
    data = generate_rows()
    csv_path = BASE_DIR / CSV_NAME
    json_path = BASE_DIR / JSON_NAME

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(HEADERS)
        w.writerows(data)

    as_dicts = [dict(zip(HEADERS, row)) for row in data]
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(as_dicts, f, indent=2, ensure_ascii=False)

    print(f"Wrote {csv_path} ({ROWS} data rows + header)")
    print(f"Wrote {json_path} ({len(as_dicts)} objects)")


def load_csv_rows(path: Path) -> tuple[list[str], list[list[str]]]:
    with path.open(newline="", encoding="utf-8") as f:
        r = csv.reader(f)
        header = next(r)
        rows = [row for row in r]
    return header, rows


def load_json_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(encoding="utf-8") as f:
        arr = json.load(f)
    if not arr:
        return [], []
    header = list(arr[0].keys())
    return header, arr


def compare_line_by_line(csv_path: Path, json_path: Path) -> int:
    print("\n=== Extension checks ===")
    ok_csv, msg_csv = validate_extension(csv_path, ".csv")
    ok_json, msg_json = validate_extension(json_path, ".json")
    print(f"  [{'PASS' if ok_csv else 'FAIL'}] CSV {msg_csv}")
    print(f"  [{'PASS' if ok_json else 'FAIL'}] JSON {msg_json}")
    if not (ok_csv and ok_json):
        return 1

    print("\n=== Load & structure ===")
    try:
        h_csv, rows_csv = load_csv_rows(csv_path)
        h_json, rows_json = load_json_rows(json_path)
    except Exception as e:
        print(f"  [FAIL] read error: {e}")
        return 1

    print(f"  [{'PASS' if h_csv == HEADERS else 'FAIL'}] CSV header columns={len(h_csv)}")
    print(f"  [{'PASS' if rows_json and list(rows_json[0].keys()) == HEADERS else 'FAIL'}] JSON keys count={len(rows_json[0]) if rows_json else 0}")
    print(f"  [{'PASS' if len(rows_csv) == ROWS else 'FAIL'}] CSV data rows={len(rows_csv)} (expect {ROWS})")
    print(f"  [{'PASS' if len(rows_json) == ROWS else 'FAIL'}] JSON array length={len(rows_json)} (expect {ROWS})")

    if len(rows_csv) != len(rows_json):
        print("  [FAIL] row count mismatch; skipping line-by-line value compare")
        return 1

    print("\n=== Line-by-line row compare (CSV row i vs JSON[i]) ===")
    failures = 0
    for i in range(len(rows_csv)):
        line_ok = True
        row_c = rows_csv[i]
        obj = rows_json[i]
        if len(row_c) != 25:
            print(f"  [FAIL] row {i:03d}: CSV column count {len(row_c)} != 25")
            failures += 1
            continue
        for j, key in enumerate(HEADERS):
            v_csv = row_c[j]
            v_json = obj.get(key, "__MISSING__")
            if v_csv != v_json:
                print(f"  [FAIL] row {i:03d} {key}: csv={v_csv!r} json={v_json!r}")
                line_ok = False
                failures += 1
        if line_ok:
            print(f"  [PASS] row {i:03d}")

    print("\n=== Summary ===")
    if failures == 0:
        print(f"All {ROWS} rows match; extensions OK.")
        return 0
    print(f"{failures} field/row mismatch(es).")
    return 1


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--generate", action="store_true", help="Create CSV + JSON samples")
    p.add_argument(
        "--compare",
        action="store_true",
        help="Validate extensions and compare CSV vs JSON row-by-row",
    )
    args = p.parse_args()

    if not args.generate and not args.compare:
        p.print_help()
        print("\nTypical:  python validate_csv_json_files.py --generate --compare")
        return 0

    if args.generate:
        write_samples()
        if not args.compare:
            return 0

    if args.compare or not args.generate:
        csv_path = BASE_DIR / CSV_NAME
        json_path = BASE_DIR / JSON_NAME
        if not csv_path.is_file() or not json_path.is_file():
            print("Missing sample files. Run: python validate_csv_json_files.py --generate", file=sys.stderr)
            return 2
        return compare_line_by_line(csv_path, json_path)

    return 0


if __name__ == "__main__":
    sys.exit(main())
