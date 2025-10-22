#!/usr/bin/env python3
"""End-to-end ETL pipeline following docs/etl.md."""

import argparse
import io
import os
import sys
import time
from contextlib import suppress
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from datetime import time as dt_time
from ftplib import FTP, error_perm, error_proto
from pathlib import Path
from typing import Any, cast
from zoneinfo import ZoneInfo

from boto3.session import Session

from transfer import ScanReportProcessor

ASIA_TOKYO = ZoneInfo("Asia/Tokyo")
S3_DEFAULT_BUCKET = "redshift-dwh-prod-uploads"
S3_DEFAULT_PREFIX = "feetaxis"


@dataclass
class ReportSpec:
    name: str
    remote_path: str
    s3_key: str
    start_date: str
    end_date: str

    @property
    def filename(self) -> str:
        return Path(self.remote_path).name


def load_env(env_path: str) -> None:
    """Populate os.environ with values from the given .env file."""
    if not env_path:
        return

    path = Path(env_path)
    if not path.exists():
        return

    for line in path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        if "=" not in stripped:
            continue

        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip()

        if not key or key.startswith("export "):
            key = key.replace("export ", "", 1).strip()

        if not key:
            continue

        if value and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]

        os.environ.setdefault(key, value)


def ensure_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        raise RuntimeError(f"Environment variable {var_name} is required")
    return value


def build_report_specs(now: datetime) -> list[ReportSpec]:
    jst_now = now.astimezone(ASIA_TOKYO)

    jst_yesterday = jst_now.date() - timedelta(days=1)
    daily_date = jst_yesterday.strftime("%Y-%m-%d")
    daily_date_compact = jst_yesterday.strftime("%Y%m%d")
    daily_remote = f"/POSReportDaily/report_Xebio_{daily_date}-{daily_date}.xlsx"
    daily_s3_key = f"scan_report_daily_{daily_date_compact}.csv"

    weekday = jst_now.weekday()  # Monday=0
    days_since_sunday = (weekday + 1) % 7
    sunday = jst_now.date() - timedelta(days=days_since_sunday)
    monday = sunday - timedelta(days=6)
    monday_str = monday.strftime("%Y-%m-%d")
    sunday_str = sunday.strftime("%Y-%m-%d")
    sunday_str_compact = sunday.strftime("%Y%m%d")
    weekly_remote = f"/POSReport/report_Xebio_{monday_str}-{sunday_str}.xlsx"
    weekly_s3_key = f"scan_report_weekly_{sunday_str_compact}.csv"

    return [
        ReportSpec(
            name="scan_report_daily",
            remote_path=daily_remote,
            s3_key=daily_s3_key,
            start_date=daily_date,
            end_date=daily_date,
        ),
        ReportSpec(
            name="scan_report_weekly",
            remote_path=weekly_remote,
            s3_key=weekly_s3_key,
            start_date=monday_str,
            end_date=sunday_str,
        ),
    ]


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


def _fetch_report(
    ftp_config: dict[str, str],
    remote_path: str,
    destination: Path,
    *,
    use_epsv: bool,
    passive: bool,
) -> None:
    host = ftp_config["host"]
    port = int(ftp_config["port"])
    username = ftp_config["username"]
    password = ftp_config["password"]

    if destination.exists():
        destination.unlink()

    with FTP() as ftp:
        ftp.connect(host=host, port=port, timeout=30)
        ftp.login(user=username, passwd=password)

        ftp_any = cast(Any, ftp)
        if hasattr(ftp_any, "use_epsv"):
            ftp_any.use_epsv = use_epsv
        ftp.set_pasv(passive)

        path_parts = remote_path.strip("/").split("/")
        directories = path_parts[:-1]
        filename = path_parts[-1]

        def reset_root() -> None:
            with suppress(error_perm):
                ftp.cwd("/")

        reset_root()

        try:
            for directory in directories:
                if directory:
                    ftp.cwd(directory)

            with destination.open("wb") as file_obj:
                ftp.retrbinary(f"RETR {filename}", file_obj.write)
        except error_perm as exc:
            if str(exc).startswith("550"):
                raise FileNotFoundError(f"FTP file not found: {remote_path}") from exc
            raise
        finally:
            reset_root()


def _download_report_once(ftp_config: dict[str, str], remote_path: str, destination: Path) -> None:
    modes = [
        (False, True),
        (False, False),
        (True, True),
    ]

    last_proto_exc: Exception | None = None

    for use_epsv, passive in modes:
        try:
            _fetch_report(
                ftp_config,
                remote_path,
                destination,
                use_epsv=use_epsv,
                passive=passive,
            )
            return
        except error_proto as exc:
            if "Extended Passive Mode" in str(exc):
                last_proto_exc = exc
                continue
            raise

    if last_proto_exc:
        raise last_proto_exc


def download_report(ftp_config: dict[str, str], remote_path: str, destination: Path) -> None:
    try:
        retry_count = max(1, int(os.getenv("FTP_MAX_RETRIES", "3")))
    except ValueError:
        retry_count = 3

    try:
        retry_delay = float(os.getenv("FTP_RETRY_DELAY_SECONDS", "5"))
    except ValueError:
        retry_delay = 5.0

    last_exc: Exception | None = None

    for attempt in range(1, retry_count + 1):
        try:
            _download_report_once(ftp_config, remote_path, destination)
            return
        except FileNotFoundError:
            raise
        except Exception as exc:
            last_exc = exc
            if attempt < retry_count:
                wait_seconds = max(0.0, retry_delay)
                print(
                    f"  - FTP attempt {attempt} failed ({exc}); retrying in {wait_seconds:.1f}s..."
                )
                if wait_seconds > 0:
                    time.sleep(wait_seconds)
            else:
                raise exc

    if last_exc:
        raise last_exc


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


def run_pipeline(env_path: str, save_output: bool, today_override: date | None = None) -> None:
    load_env(env_path)

    ftp_config = get_ftp_config()
    s3_client, s3_bucket, s3_prefix = create_s3_client()
    reference_now = datetime.now(tz=ASIA_TOKYO)
    if today_override:
        reference_now = datetime.combine(today_override, dt_time.min, tzinfo=ASIA_TOKYO)
    report_specs = build_report_specs(reference_now)

    download_dir = Path("./tmp")
    download_dir.mkdir(parents=True, exist_ok=True)
    processor = ScanReportProcessor(sample_dir=download_dir, persist_output=save_output)

    try:
        ftp_wait_seconds = float(os.getenv("FTP_WAIT_SECONDS", "5"))
    except ValueError:
        ftp_wait_seconds = 5.0

    for index, spec in enumerate(report_specs):
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

        schema_override = ScanReportProcessor.SCHEMAS.get(spec.name)
        if not schema_override:
            raise SystemExit(f"No schema configured for {spec.name}")

        result = processor.process_file(local_path, schema=schema_override)
        clean_rows = len(result.clean_df)
        invalid_rows = len(result.invalid_df)

        print(f"  - Clean rows: {clean_rows}, Invalid rows: {invalid_rows}")

        if save_output:
            if result.clean_output_path:
                print(f"  - Clean CSV saved to {result.clean_output_path}")
            if result.invalid_output_path:
                print(f"  - Invalid CSV saved to {result.invalid_output_path}")

        uploaded_key = upload_clean_dataset(
            s3_client,
            s3_bucket,
            s3_prefix,
            spec.s3_key,
            result.clean_df,
        )
        print(f"  - Uploaded to s3://{s3_bucket}/{uploaded_key}")

        if ftp_wait_seconds > 0 and index < len(report_specs) - 1:
            print(f"  - Waiting {ftp_wait_seconds:.1f}s before next FTP download")
            time.sleep(ftp_wait_seconds)


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
