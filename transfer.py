#!/usr/bin/env python3
import re
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

import pandas as pd


@dataclass
class ProcessingResult:
    source_file: Path
    clean_df: pd.DataFrame
    invalid_df: pd.DataFrame
    clean_output_path: Path | None
    invalid_output_path: Path | None


class ScanReportProcessor:
    # Schema definitions based on table definitions in docs/transfer.md
    SCHEMAS: ClassVar[dict[str, dict]] = {
        "scan_report_daily": {
            "columns": [
                "scan_date",
                "point_card_id",
                "store_id",
                "employee_id",
                "shoe_sold",
                "shoe_exist_in_db",
                "shoes_marked_sold_rwa",
                "insole_sold",
                "shoe_functional",
                "size_recommendation",
                "safesize_code",
                "scanner_id",
            ],
            "not_null": ["scan_date", "store_id", "employee_id", "safesize_code"],
            "validations": {
                "store_id": {"type": "numeric", "pattern": r"^[0-9]+$"},
                "employee_id": {"type": "numeric", "pattern": r"^[0-9]{7}$"},
            },
        },
        "scan_report_weekly": {
            "columns": [
                "scan_date",
                "point_card_id",
                "store_id",
                "employee_id",
                "shoe_sold",
                "shoe_exist_in_db",
                "shoes_marked_sold_rwa",
                "insole_sold",
                "shoe_functional",
                "size_recommendation",
                "safesize_code",
                "scanner_id",
            ],
            "not_null": ["scan_date", "store_id", "employee_id", "safesize_code"],
            "validations": {
                "store_id": {"type": "numeric", "pattern": r"^[0-9]+$"},
                "employee_id": {"type": "numeric", "pattern": r"^[0-9]{7}$"},
            },
        },
    }

    COLUMN_ALIASES: ClassVar[dict[str, str]] = {
        "sspc": "point_card_id",
        "store_id": "store_id",
        "employee_id": "employee_id",
        "shoe_sold": "shoe_sold",
        "shoe_exist_in_database": "shoe_exist_in_db",
        "shoes_marked_as_sold_in_rwa": "shoes_marked_sold_rwa",
        "insole_sold": "insole_sold",
        "shoe_functionally": "shoe_functional",
        "shoe_functionally_": "shoe_functional",
        "shoe_functional": "shoe_functional",
        "size_recommendation": "size_recommendation",
        "safesize_code": "safesize_code",
        "scanner_id": "scanner_id",
        "scan_date": "scan_date",
        "unnamed_11": "scan_date",
        "unnamed_11_": "scan_date",
    }

    def __init__(
        self,
        sample_dir: str | Path = "sample",
        output_dir: str | Path = "output",
        persist_output: bool = True,
    ):
        self.sample_dir = Path(sample_dir)
        self.output_dir = Path(output_dir)
        self.persist_output = persist_output
        self._setup_directories()

    def _setup_directories(self):
        """Create sample directory and clear/create output directory"""
        self.sample_dir.mkdir(exist_ok=True)

        if not self.persist_output:
            return

        # Clear output directory if it exists
        if self.output_dir.exists():
            shutil.rmtree(self.output_dir)
        self.output_dir.mkdir(exist_ok=True)

    def get_schema_for_file(self, file_name: str) -> dict | None:
        """Get schema for a file based on its name"""
        # Remove .csv extension and check if schema exists
        base_name = Path(file_name).stem
        return self.SCHEMAS.get(base_name)

    def validate_data_with_schema(
        self, df: pd.DataFrame, schema: dict
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Validate data using the specified schema"""
        # Add error column and row number
        df["_error"] = ""
        df["_row_number"] = range(2, len(df) + 2)  # Start from 2 (header is 1)

        def append_error(mask: pd.Series, message: str) -> None:
            if mask.any():
                current = df.loc[mask, "_error"].fillna("").astype(str)
                df.loc[mask, "_error"] = current + message

        # Check NOT NULL constraints
        for column in schema["not_null"]:
            if column in df.columns:
                null_mask = df[column].isna() | (df[column].astype(str).str.strip() == "")
                append_error(null_mask, f"NOT NULL violation: {column}; ")

        # Apply field-specific validations
        for field, validation in schema.get("validations", {}).items():
            if field in df.columns:
                field_str = df[field].astype(str)
                pattern = validation["pattern"]

                # Check pattern match (excluding NULL values already caught)
                invalid_mask = ~field_str.str.match(pattern, na=False)
                # Exclude rows already marked with NOT NULL violation for this field
                invalid_mask = invalid_mask & (df[field].notna())

                if validation["type"] == "numeric":
                    error_msg = f"Invalid {field} (must be numeric)"
                    if field == "employee_id":
                        error_msg = f"Invalid {field} (must be 7 digits)"
                    append_error(invalid_mask, error_msg + "; ")

        # Clean up error messages
        df["_error"] = df["_error"].str.rstrip("; ")

        # Filter columns to only those defined in schema (plus error columns)
        schema_columns = schema["columns"]
        existing_columns = [col for col in schema_columns if col in df.columns]
        df_filtered = df[[*existing_columns, "_error", "_row_number"]].copy()

        # Split into clean and invalid
        clean_df = df_filtered[df_filtered["_error"] == ""].copy()
        invalid_df = df_filtered[df_filtered["_error"] != ""].copy()

        # Remove error columns from clean data
        if not clean_df.empty:
            clean_df = clean_df.drop(columns=["_error", "_row_number"])

        return clean_df, invalid_df

    def _load_dataframe(self, file_path: Path) -> pd.DataFrame:
        """Load source data regardless of CSV or Excel extension."""
        suffix = file_path.suffix.lower()

        if suffix == ".csv":
            return pd.read_csv(file_path, encoding="utf-8-sig", dtype=str)
        if suffix in {".xlsx", ".xls"}:
            return pd.read_excel(file_path, dtype=str)

        raise ValueError(f"Unsupported file extension for {file_path.name}")

    @classmethod
    def _normalize_column_name(cls, column_name: str) -> str:
        """Normalize raw column headers for schema matching."""
        cleaned = column_name.strip().lower()
        cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned)
        cleaned = re.sub(r"_+", "_", cleaned).strip("_")
        return cleaned

    def _normalize_dataframe_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rename incoming columns to match schema expectations."""
        rename_map: dict[str, str] = {}
        for column in df.columns:
            normalized = self._normalize_column_name(column)
            target = self.COLUMN_ALIASES.get(normalized, normalized)
            rename_map[column] = target

        # Pandas handles duplicate keys by retaining the last occurrence; ensure uniqueness
        # by appending suffixes for duplicate targets.
        seen: dict[str, int] = {}
        for original, target in rename_map.items():
            count = seen.get(target, 0)
            if count:
                rename_map[original] = f"{target}_{count}"
            seen[target] = count + 1

        return df.rename(columns=rename_map)

    def process_input_file(self, csv_file: Path, schema: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Process a single tabular file with the given schema"""
        try:
            # Read CSV/Excel with UTF-8 BOM or Excel support
            df = self._load_dataframe(csv_file)
            df = self._normalize_dataframe_columns(df)

            # Validate and split data using schema
            clean_df, invalid_df = self.validate_data_with_schema(df, schema)

            return clean_df, invalid_df

        except Exception as e:
            print(f"Error processing file {csv_file}: {e}")
            raise

    def process_file(self, csv_file: Path, schema: dict | None = None) -> ProcessingResult:
        """Validate a file and optionally persist outputs"""
        target_schema = schema or self.get_schema_for_file(csv_file.name)

        if not target_schema:
            raise ValueError(f"No schema defined for file: {csv_file.name}")

        clean_df, invalid_df = self.process_input_file(csv_file, target_schema)

        clean_output_path: Path | None = None
        invalid_output_path: Path | None = None

        if self.persist_output:
            base_name = csv_file.stem
            clean_output = self.output_dir / f"{base_name}_clean.csv"
            invalid_output = self.output_dir / f"{base_name}_invalid.csv"

            if not clean_df.empty:
                clean_df.to_csv(clean_output, index=False, encoding="utf-8")
                clean_output_path = clean_output

            if not invalid_df.empty:
                invalid_df.to_csv(invalid_output, index=False, encoding="utf-8")
                invalid_output_path = invalid_output

        return ProcessingResult(
            source_file=csv_file,
            clean_df=clean_df,
            invalid_df=invalid_df,
            clean_output_path=clean_output_path,
            invalid_output_path=invalid_output_path,
        )

    def process_all_files(self):
        """Process all tabular files that have defined schemas"""
        csv_files = list(self.sample_dir.glob("*.csv"))
        excel_files = list(self.sample_dir.glob("*.xlsx")) + list(self.sample_dir.glob("*.xls"))
        input_files = csv_files + excel_files

        if not input_files:
            print(f"No input files found in {self.sample_dir}")
            return

        processed_count = 0
        skipped_files = []

        for input_file in input_files:
            # Get schema for this file
            schema = self.get_schema_for_file(input_file.name)

            if not schema:
                skipped_files.append(input_file.name)
                continue

            processed_count += 1
            print(f"\nProcessing: {input_file.name}")
            print(f"  - Using schema: {input_file.stem}")

            result = self.process_file(input_file, schema=schema)

            clean_count = len(result.clean_df)
            invalid_count = len(result.invalid_df)

            if clean_count:
                if result.clean_output_path:
                    print(f"  - Saved {clean_count} clean rows to {result.clean_output_path}")
                else:
                    print(f"  - {clean_count} clean rows ready")
            else:
                print("  - No clean rows found")

            if invalid_count:
                if result.invalid_output_path:
                    print(f"  - Saved {invalid_count} invalid rows to {result.invalid_output_path}")
                else:
                    print(f"  - {invalid_count} invalid rows detected")
            else:
                print("  - No invalid rows found")

            # Print statistics
            total_rows = clean_count + invalid_count
            if total_rows > 0:
                print(f"  - Total: {total_rows} rows processed")
                print(f"  - Valid: {clean_count} ({clean_count / total_rows * 100:.1f}%)")
                print(f"  - Invalid: {invalid_count} ({invalid_count / total_rows * 100:.1f}%)")

        # Report skipped files
        if skipped_files:
            print("\n" + "-" * 60)
            print(f"Skipped files (no schema defined): {', '.join(skipped_files)}")

        if processed_count == 0:
            print("\nNo files with defined schemas found to process.")


def main():
    processor = ScanReportProcessor()

    print("=" * 60)
    print("Scan Report Data Transfer Processing")
    print("=" * 60)
    print(f"Sample directory: {processor.sample_dir.absolute()}")
    print(f"Output directory: {processor.output_dir.absolute()}")
    print("Note: Output directory has been cleared")
    print(f"Available schemas: {', '.join(processor.SCHEMAS.keys())}")
    print("-" * 60)

    try:
        processor.process_all_files()
        print("\n" + "=" * 60)
        print("Processing completed successfully!")
        print("=" * 60)
    except Exception as e:
        print(f"\nError during processing: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
