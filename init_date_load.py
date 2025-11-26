#!/usr/bin/env python3
"""Aggregate historical scan report CSV files into the latest-dated key."""

from __future__ import annotations

import argparse
import io
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd
from botocore.exceptions import ClientError

from etl import create_s3_client
from src.env import load_env
from src.schema import SCAN_REPORT_COLUMNS, SCHEMAS
from src.transfer import ScanReportProcessor


@dataclass(frozen=True)
class DatasetConfig:
    name: str  # human friendly identifier ("daily" or "weekly")
    key_prefix: str  # base filename without date suffix, e.g. "scan_report_daily"

    @property
    def list_prefix(self) -> str:
        return f"{self.key_prefix}_"

    def filename_for(self, report_date: date) -> str:
        return f"{self.key_prefix}_{report_date.strftime('%Y%m%d')}.csv"


@dataclass(frozen=True)
class LocalReportFile:
    path: Path
    report_date: date

    @property
    def name(self) -> str:
        return self.path.name


def build_s3_path(base_prefix: str, suffix: str) -> str:
    """Join bucket-level prefix and suffix into a normalized key/prefix."""
    base = base_prefix.strip("/")
    cleaned_suffix = suffix.lstrip("/")
    if base and cleaned_suffix:
        return f"{base}/{cleaned_suffix}"
    if base:
        return base
    return cleaned_suffix


def extract_report_date(key: str, dataset: DatasetConfig) -> date | None:
    basename = key.rsplit("/", 1)[-1]
    expected_prefix = dataset.list_prefix
    if not basename.startswith(expected_prefix) or not basename.lower().endswith(".csv"):
        return None
    date_part = basename[len(expected_prefix) : -4]
    if len(date_part) != 8 or not date_part.isdigit():
        return None
    try:
        return datetime.strptime(date_part, "%Y%m%d").date()
    except ValueError:
        return None


def resolve_source_dir(root: Path, base_prefix: str) -> Path:
    """Map the configured prefix to a directory under the local root."""

    cleaned = base_prefix.strip("/")
    if not cleaned:
        return root

    candidate = root.joinpath(*cleaned.split("/"))
    if candidate.exists():
        return candidate
    return root


def list_dataset_files(
    source_dir: Path,
    dataset: DatasetConfig,
) -> list[LocalReportFile]:
    pattern = f"{dataset.list_prefix}*.csv"
    files: list[LocalReportFile] = []

    for path in source_dir.glob(pattern):
        if not path.is_file():
            continue
        report_date = extract_report_date(path.name, dataset)
        if not report_date:
            print(
                f"[{dataset.name}] Skipping file without parsable date: {path}",
                file=sys.stderr,
            )
            continue
        files.append(LocalReportFile(path=path, report_date=report_date))

    files.sort(key=lambda obj: (obj.report_date, obj.name))
    return files


def load_dataset_files(
    objects: Iterable[LocalReportFile],
    processor: ScanReportProcessor,
) -> list[pd.DataFrame]:
    frames: list[pd.DataFrame] = []
    for obj in objects:
        try:
            frame = processor._load_dataframe(obj.path)
        except FileNotFoundError as exc:
            raise RuntimeError(f"Failed to load {obj.path}: {exc}") from exc

        frame = processor._normalize_dataframe_columns(frame)
        frame = processor._apply_domain_cleaning(frame)

        scan_date_value = obj.report_date.isoformat()
        if "scan_date" not in frame.columns:
            frame["scan_date"] = scan_date_value
        else:
            column = frame["scan_date"].astype("string")
            mask = column.isna() | (column.str.strip() == "")
            frame["scan_date"] = column.mask(mask, scan_date_value)
        frames.append(frame)
        print(f"  - Loaded {obj.path} with {len(frame)} rows")

    return frames


def align_columns(frames: Sequence[pd.DataFrame]) -> list[pd.DataFrame]:
    if not frames:
        return []

    ordered_columns: list[str] = list(frames[0].columns)
    for frame in frames[1:]:
        for column in frame.columns:
            if column not in ordered_columns:
                ordered_columns.append(column)

    return [frame.reindex(columns=ordered_columns) for frame in frames]


def concatenate_frames(frames: Sequence[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        raise ValueError("No frames supplied for concatenation")

    normalized = align_columns(frames)
    return pd.concat(normalized, ignore_index=True)


def reorder_columns(frame: pd.DataFrame) -> pd.DataFrame:
    desired = [column for column in SCAN_REPORT_COLUMNS if column in frame.columns]
    remaining = [column for column in frame.columns if column not in desired]
    return frame.loc[:, [*desired, *remaining]]


def ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def enforce_schema_types(
    frame: pd.DataFrame,
    dataset: DatasetConfig,
    processor: ScanReportProcessor,
) -> pd.DataFrame:
    schema = SCHEMAS.get(dataset.key_prefix)
    if not schema:
        raise RuntimeError(f"No schema configuration for dataset '{dataset.name}'")

    # Work on a copy to preserve the original combined frame structure for logging
    working = frame.copy()
    clean_df, invalid_df = processor.validate_data_with_schema(working, schema)

    if not invalid_df.empty:
        preview = invalid_df[["_row_number", "_error"]].head()
        raise RuntimeError(
            f"[{dataset.name}] Invalid rows detected while enforcing schema types: {preview.to_dict('records')}"
        )

    return reorder_columns(clean_df)


def process_dataset(
    dataset: DatasetConfig,
    source_root: Path,
    s3_client,
    bucket: str,
    base_prefix: str,
    dry_run: bool,
    output_dir: Path,
) -> None:
    print(f"Processing dataset: {dataset.name}")
    dataset_dir = resolve_source_dir(source_root, base_prefix)
    if not dataset_dir.exists():
        raise RuntimeError(f"Source directory does not exist: {dataset_dir}")

    objects = list_dataset_files(dataset_dir, dataset)
    if not objects:
        raise RuntimeError(f"No files found for dataset '{dataset.name}' in {dataset_dir}")

    latest_date = max(obj.report_date for obj in objects)
    target_filename = dataset.filename_for(latest_date)
    target_key = build_s3_path(base_prefix, target_filename)

    target_path = dataset_dir / target_filename
    download_objects = objects

    print(f"  - Found {len(objects)} file(s); using {len(download_objects)} for aggregation")

    processor = ScanReportProcessor(
        sample_dir=dataset_dir,
        output_dir=output_dir,
        persist_output=False,
    )
    frames = load_dataset_files(download_objects, processor)
    combined = concatenate_frames(frames)
    combined = enforce_schema_types(combined, dataset, processor)
    print(f"  - Combined row count: {len(combined)}")

    if dry_run:
        ensure_output_dir(output_dir)
        destination = output_dir / target_filename
        combined.to_csv(destination, index=False)
        print(f"  - Dry run: wrote consolidated CSV to {destination}")
        return

    buffer = io.StringIO()
    combined.to_csv(buffer, index=False)
    payload = buffer.getvalue().encode("utf-8")
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=target_key,
            Body=payload,
            ContentType="text/csv",
        )
    except ClientError as exc:  # pragma: no cover - requires AWS failure
        raise RuntimeError(f"Failed to upload consolidated dataset to {target_key}: {exc}") from exc

    print(f"  - Uploaded consolidated CSV to s3://{bucket}/{target_key}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate scan report history from local backups",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to the .env file with S3 credentials",
    )
    parser.add_argument(
        "--datasets",
        choices=["daily", "weekly", "all"],
        default="all",
        help="Limit aggregation to a specific dataset (default: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Write consolidated CSV under output/ without uploading to S3",
    )
    parser.add_argument(
        "--source-dir",
        default="/backup",
        help="Directory containing historic CSV backups (default: /backup)",
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Destination directory for --dry-run exports",
    )
    return parser.parse_args()


def resolve_datasets(option: str) -> list[DatasetConfig]:
    configs = {
        "daily": DatasetConfig(name="daily", key_prefix="scan_report_daily"),
        "weekly": DatasetConfig(name="weekly", key_prefix="scan_report_weekly"),
    }
    if option == "all":
        return [configs["daily"], configs["weekly"]]
    return [configs[option]]


def main() -> None:
    args = parse_args()
    load_env(args.env_file)

    try:
        s3_client, bucket, base_prefix = create_s3_client()
    except RuntimeError as exc:
        print(f"Failed to configure S3 client: {exc}", file=sys.stderr)
        sys.exit(1)

    datasets = resolve_datasets(args.datasets)
    source_root = Path(args.source_dir).expanduser()
    output_dir = Path(args.output_dir).expanduser()

    for dataset in datasets:
        try:
            process_dataset(
                dataset=dataset,
                source_root=source_root,
                s3_client=s3_client,
                bucket=bucket,
                base_prefix=base_prefix,
                dry_run=args.dry_run,
                output_dir=output_dir,
            )
        except Exception as exc:
            print(f"{dataset.name} aggregation failed: {exc}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
