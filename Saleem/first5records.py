#!/usr/bin/env python3
"""Print first 5 records from sample_500x25.csv and sample_500x25.json."""

from __future__ import annotations

import csv
import json
from pathlib import Path

BASE = Path(__file__).resolve().parent
CSV_PATH = BASE / "sample_500x25.csv"
JSON_PATH = BASE / "sample_500x25.json"


def main() -> None:
    print("=== First 5 rows from CSV ===")
    with CSV_PATH.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        print("Header:", header)
        for i, row in enumerate(reader):
            if i >= 5:
                break
            print(f"  row {i}:", row)

    print("\n=== First 5 objects from JSON ===")
    with JSON_PATH.open(encoding="utf-8") as f:
        data = json.load(f)
    for i, obj in enumerate(data[:5]):
        print(f"  row {i}:", obj)


if __name__ == "__main__":
    main()
