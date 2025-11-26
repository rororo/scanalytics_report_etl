#!/usr/bin/env python3
"""End-to-end ETL pipeline following docs/etl.md."""

import argparse
import io
import os
import sys
from datetime import date, datetime
from datetime import time as dt_time
from pathlib import Path

from boto3.session import Session

from src.downloader import download_report
from src.env import ensure_env, load_env
from src.schema import SCHEMAS
from src.spec import build_report_specs
from src.time_utils import ASIA_TOKYO
from src.transfer import ScanReportProcessor

S3_DEFAULT_BUCKET = "redshift-dwh-prod-uploads"
S3_DEFAULT_PREFIX = "feetaxis"


def get_ftp_config() -> dict[str, str]:
    return {
        "host": ensure_env("FTP_HOST"),
        "port": os.getenv("FTP_PORT", "21"),
        "username": ensure_env("FTP_USERNAME"),
        "password": ensure_env("FTP_PASSWORD"),
    }


def create_s3_client():
    access_key = ensure_env("S3_ACCESS_KEY_ID")
    secret_key = ensure_env("S3_SECRET_ACCESS_KEY")
    region = os.getenv("S3_REGION", "ap-northeast-1")
    bucket = os.getenv("S3_BUCKET", S3_DEFAULT_BUCKET)
    prefix = os.getenv("S3_PREFIX", S3_DEFAULT_PREFIX)

    session = Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )
    return session.client("s3"), bucket, prefix


def upload_clean_dataset(s3_client, bucket: str, prefix: str, key: str, dataframe) -> str:
    if not key.lower().endswith(".csv"):
        raise ValueError(f"S3 key must end with .csv, got: {key}")

    buffer = io.StringIO()
    dataframe.to_csv(buffer, index=False)
    payload = buffer.getvalue().encode("utf-8")
    s3_key = f"{prefix.rstrip('/')}/{key}" if prefix else key
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=payload,
        ContentType="text/csv",
    )
    return s3_key


def run_pipeline(
    env_path: str,
    save_output: bool,
    today_override: date | None = None,
    *,
    dry_run: bool = False,
) -> None:
    load_env(env_path)

    ftp_config = get_ftp_config()
    s3_client = None
    s3_bucket = ""
    s3_prefix = ""
    if not dry_run:
        s3_client, s3_bucket, s3_prefix = create_s3_client()
    reference_now = datetime.now(tz=ASIA_TOKYO)
    if today_override:
        reference_now = datetime.combine(today_override, dt_time.min, tzinfo=ASIA_TOKYO)
    report_specs = build_report_specs(reference_now)

    download_dir = Path("./tmp")
    download_dir.mkdir(parents=True, exist_ok=True)

    # try:
    #     ftp_wait_seconds = float(os.getenv("FTP_WAIT_SECONDS", "5"))
    # except ValueError:
    #     ftp_wait_seconds = 5.0

    for _, spec in enumerate(report_specs):
        local_path = download_dir / spec.filename
        print(f"Fetching {spec.remote_path} -> {local_path}")

        if local_path.exists():
            if local_path.is_dir():
                raise SystemExit(f"Cannot overwrite directory at {local_path}")
            local_path.unlink()

        try:
            download_report(ftp_config, spec.remote_path, local_path)
        except FileNotFoundError as exc:
            raise SystemExit(str(exc)) from exc

        schema_override = SCHEMAS.get(spec.name)
        if not schema_override:
            raise SystemExit(f"No schema configured for {spec.name}")

        processor = ScanReportProcessor(sample_dir=download_dir, persist_output=save_output)
        result = processor.process_file(
            local_path,
            schema=schema_override,
            scan_date=spec.end_date,
        )
        clean_rows = len(result.clean_df)
        invalid_rows = len(result.invalid_df)

        print(f"  - Clean rows: {clean_rows}, Invalid rows: {invalid_rows}")

        if save_output:
            if result.clean_output_path:
                print(f"  - Clean CSV saved to {result.clean_output_path}")
            if result.invalid_output_path:
                print(f"  - Invalid CSV saved to {result.invalid_output_path}")

        if dry_run:
            print("  - Dry run enabled; skipping upload to S3")
            continue

        uploaded_key = upload_clean_dataset(
            s3_client,
            s3_bucket,
            s3_prefix,
            spec.s3_key,
            result.clean_df,
        )
        print(f"  - Uploaded to s3://{s3_bucket}/{uploaded_key}")

        # if ftp_wait_seconds > 0 and index < len(report_specs) - 1:
        #     print(f"  - Waiting {ftp_wait_seconds:.1f}s before next FTP download")
        #     time.sleep(ftp_wait_seconds)


def main() -> None:
    parser = argparse.ArgumentParser(description="ETL pipeline for scan report files")
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to the .env file containing FTP and S3 credentials",
    )
    parser.add_argument(
        "--save-output",
        action="store_true",
        help="Persist processed CSVs under output/ (enable for local debugging)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Process files without uploading results to S3",
    )
    parser.add_argument(
        "--today",
        help="Override today's date (JST) in YYYY-MM-DD format",
    )

    args = parser.parse_args()

    today_override = None
    if args.today:
        try:
            today_override = datetime.strptime(args.today, "%Y-%m-%d").date()
        except ValueError:
            print("Invalid --today format, expected YYYY-MM-DD.", file=sys.stderr)
            sys.exit(1)

    try:
        run_pipeline(
            env_path=args.env_file,
            save_output=args.save_output,
            today_override=today_override,
            dry_run=args.dry_run,
        )
    except RuntimeError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        sys.exit(1)
    except SystemExit:
        raise
    except Exception as exc:
        print(f"ETL failed: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
