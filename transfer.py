#!/usr/bin/env python3
import shutil
import sys
from pathlib import Path
from typing import Dict, Optional

import pandas as pd


class ScanReportProcessor:
    # Schema definitions based on table definitions in docs/transfer.md
    SCHEMAS = {
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
                "scanner_id"
            ],
            "not_null": ["scan_date", "store_id", "employee_id", "safesize_code"],
            "validations": {
                "store_id": {"type": "numeric", "pattern": r"^[0-9]+$"},
                "employee_id": {"type": "numeric", "pattern": r"^[0-9]{7}$"}
            }
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
                "scanner_id"
            ],
            "not_null": ["scan_date", "store_id", "employee_id", "safesize_code"],
            "validations": {
                "store_id": {"type": "numeric", "pattern": r"^[0-9]+$"},
                "employee_id": {"type": "numeric", "pattern": r"^[0-9]{7}$"}
            }
        }
    }

    def __init__(self, sample_dir: str = "sample", output_dir: str = "output"):
        self.sample_dir = Path(sample_dir)
        self.output_dir = Path(output_dir)
        self._setup_directories()

    def _setup_directories(self):
        """Create sample directory and clear/create output directory"""
        self.sample_dir.mkdir(exist_ok=True)
        
        # Clear output directory if it exists
        if self.output_dir.exists():
            shutil.rmtree(self.output_dir)
        self.output_dir.mkdir(exist_ok=True)

    def get_schema_for_file(self, file_name: str) -> Optional[Dict]:
        """Get schema for a file based on its name"""
        # Remove .csv extension and check if schema exists
        base_name = Path(file_name).stem
        return self.SCHEMAS.get(base_name)

    def validate_data_with_schema(self, df: pd.DataFrame, schema: Dict) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Validate data using the specified schema"""
        # Add error column and row number
        df["_error"] = ""
        df["_row_number"] = range(2, len(df) + 2)  # Start from 2 (header is 1)

        # Check NOT NULL constraints
        for column in schema["not_null"]:
            if column in df.columns:
                null_mask = df[column].isna() | (df[column].astype(str).str.strip() == "")
                df.loc[null_mask, "_error"] += f"NOT NULL violation: {column}; "

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
                    df.loc[invalid_mask, "_error"] += error_msg + "; "

        # Clean up error messages
        df["_error"] = df["_error"].str.rstrip("; ")

        # Filter columns to only those defined in schema (plus error columns)
        schema_columns = schema["columns"]
        existing_columns = [col for col in schema_columns if col in df.columns]
        df_filtered = df[existing_columns + ["_error", "_row_number"]].copy()

        # Split into clean and invalid
        clean_df = df_filtered[df_filtered["_error"] == ""].copy()
        invalid_df = df_filtered[df_filtered["_error"] != ""].copy()

        # Remove error columns from clean data
        if not clean_df.empty:
            clean_df = clean_df.drop(columns=["_error", "_row_number"])

        return clean_df, invalid_df

    def process_csv_file(self, csv_file: Path, schema: Dict) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Process a single CSV file with the given schema"""
        try:
            # Read CSV with UTF-8 BOM support
            df = pd.read_csv(csv_file, encoding="utf-8-sig", dtype=str)
            
            # Validate and split data using schema
            clean_df, invalid_df = self.validate_data_with_schema(df, schema)
            
            return clean_df, invalid_df
            
        except Exception as e:
            print(f"Error processing file {csv_file}: {e}")
            raise

    def process_all_files(self):
        """Process all CSV files that have defined schemas"""
        csv_files = list(self.sample_dir.glob("*.csv"))

        if not csv_files:
            print(f"No CSV files found in {self.sample_dir}")
            return

        processed_count = 0
        skipped_files = []

        for csv_file in csv_files:
            # Get schema for this file
            schema = self.get_schema_for_file(csv_file.name)
            
            if not schema:
                skipped_files.append(csv_file.name)
                continue

            processed_count += 1
            print(f"\nProcessing: {csv_file.name}")
            print(f"  - Using schema: {csv_file.stem}")

            clean_df, invalid_df = self.process_csv_file(csv_file, schema)

            base_name = csv_file.stem
            clean_output = self.output_dir / f"{base_name}_clean.csv"
            invalid_output = self.output_dir / f"{base_name}_invalid.csv"

            # Save clean data
            if not clean_df.empty:
                clean_df.to_csv(clean_output, index=False, encoding="utf-8")
                print(f"  - Saved {len(clean_df)} clean rows to {clean_output}")
            else:
                print(f"  - No clean rows found")

            # Save invalid data
            if not invalid_df.empty:
                invalid_df.to_csv(invalid_output, index=False, encoding="utf-8")
                print(f"  - Saved {len(invalid_df)} invalid rows to {invalid_output}")
            else:
                print(f"  - No invalid rows found")

            # Print statistics
            total_rows = len(clean_df) + len(invalid_df)
            if total_rows > 0:
                print(f"  - Total: {total_rows} rows processed")
                print(f"  - Valid: {len(clean_df)} ({len(clean_df) / total_rows * 100:.1f}%)")
                print(f"  - Invalid: {len(invalid_df)} ({len(invalid_df) / total_rows * 100:.1f}%)")

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