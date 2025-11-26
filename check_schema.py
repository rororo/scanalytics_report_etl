#!/usr/bin/env python3
"""CLI utility to verify tabular files against predefined schemas."""

from __future__ import annotations

import argparse
import re
import sys
from collections import Counter
from pathlib import Path

from src.schema import SCHEMAS
from src.transfer import ScanReportProcessor


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate a CSV or Excel file against a configured schema.",
    )
    parser.add_argument(
        "file",
        type=Path,
        help="Path to the input CSV or Excel file.",
    )
    parser.add_argument(
        "--schema",
        dest="schema_name",
        help="Explicit schema key to use (defaults to deriving from the file name).",
    )
    parser.add_argument(
        "--scan-date",
        dest="scan_date",
        help="Override empty scan_date values with the provided YYYY-MM-DD date.",
    )
    parser.add_argument(
        "--max-errors",
        dest="max_errors",
        type=int,
        default=20,
        help="Maximum number of invalid rows to display in the summary (default: 20).",
    )
    return parser.parse_args()


def resolve_schema(schema_name: str | None, file_name: str) -> tuple[str, dict] | None:
    if schema_name:
        schema = SCHEMAS.get(schema_name)
        if schema is None:
            return None
        return schema_name, schema

    derived_name = Path(file_name).stem
    schema = SCHEMAS.get(derived_name)
    if schema is None:
        return None
    return derived_name, schema


def ensure_column_type_spec(schema_name: str, schema: dict) -> bool:
    type_spec = ScanReportProcessor.COLUMN_TYPE_SPEC
    missing = [column for column in schema["columns"] if column not in type_spec]

    if missing:
        missing_list = ", ".join(missing)
        print(
            "Error: COLUMN_TYPE_SPEC lacks definitions for "
            f"schema '{schema_name}' columns: {missing_list}",
            file=sys.stderr,
        )
        return False

    return True


def main() -> int:
    args = parse_args()
    file_path = args.file

    if not file_path.exists():
        print(f"Error: file not found: {file_path}", file=sys.stderr)
        return 2

    schema_info = resolve_schema(args.schema_name, file_path.name)
    if schema_info is None:
        available = ", ".join(sorted(SCHEMAS))
        target = args.schema_name or f"derived name '{file_path.stem}'"
        print(
            "Error: no schema found for "
            f"{target}. Available schemas: {available}",
            file=sys.stderr,
        )
        return 2

    schema_name, schema = schema_info

    if not ensure_column_type_spec(schema_name, schema):
        return 2

    processor = ScanReportProcessor(
        sample_dir=file_path.parent,
        persist_output=False,
    )

    try:
        clean_df, invalid_df = processor.process_input_file(
            file_path,
            schema,
            scan_date=args.scan_date,
        )
    except Exception as exc:  # pragma: no cover - defensive logging
        print(f"Error while validating {file_path.name}: {exc}", file=sys.stderr)
        return 1

    valid_count = len(clean_df)
    invalid_count = len(invalid_df)

    if invalid_count == 0:
        print(
            f"Success: {file_path.name} conforms to schema '{schema_name}'. "
            f"Validated rows: {valid_count}",
        )
        return 0

    print(
        f"Validation failed for {file_path.name} with schema '{schema_name}'. "
        f"Invalid rows: {invalid_count}; valid rows: {valid_count}",
        file=sys.stderr,
    )

    error_preview = invalid_df[["_row_number", "_error"]].head(args.max_errors)
    if not error_preview.empty:
        print("\nFirst validation errors:", file=sys.stderr)
        print(error_preview.to_string(index=False), file=sys.stderr)
        if invalid_count > args.max_errors:
            remaining = invalid_count - args.max_errors
            print(
                f"...omitted {remaining} additional invalid rows.",
                file=sys.stderr,
            )

    type_error_pattern = re.compile(r"Invalid ([A-Za-z0-9_]+) ")
    type_error_counts: Counter[str] = Counter()
    for message in invalid_df["_error"].dropna():
        for column in type_error_pattern.findall(message):
            if column in ScanReportProcessor.COLUMN_TYPE_SPEC:
                type_error_counts[column] += 1

    if type_error_counts:
        print("\nColumn type issues detected:", file=sys.stderr)
        for column, count in sorted(type_error_counts.items()):
            print(f"  - {column}: {count} rows", file=sys.stderr)

    return 1


if __name__ == "__main__":
    sys.exit(main())
