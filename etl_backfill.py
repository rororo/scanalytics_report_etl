#!/usr/bin/env python3
"""Batch runner that backfills scan reports over a date range."""

import argparse
from datetime import date, datetime, timedelta
from typing import Iterator

from etl import ASIA_TOKYO, run_pipeline


def parse_iso_date(value: str, *, argument_name: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{argument_name} must be in YYYY-MM-DD format") from exc


def iter_dates(start: date, end: date) -> Iterator[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def backfill_reports(
    start_date: date,
    end_date: date,
    *,
    env_file: str,
    save_output: bool,
    dry_run: bool,
) -> None:
    total_days = (end_date - start_date).days + 1

    for index, report_date in enumerate(iter_dates(start_date, end_date), start=1):
        # run_pipeline expects a reference date that represents "today" in JST.
        today_override = report_date + timedelta(days=1)

        print(
            f"\n[{index}/{total_days}] Processing report date {report_date.isoformat()} "
            f"(running ETL with --today {today_override.isoformat()})"
        )

        run_pipeline(
            env_path=env_file,
            save_output=save_output,
            today_override=today_override,
            dry_run=dry_run,
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill scan reports by invoking the existing ETL pipeline over a date range."
        )
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to the .env file with FTP and S3 credentials (default: .env)",
    )
    parser.add_argument(
        "--save-output",
        action="store_true",
        help="Persist processed CSVs to output/ for each run (same behaviour as etl.py)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip uploading results to S3 while still running validation",
    )
    parser.add_argument(
        "--start-date",
        default="2023-03-04",
        help="First report date to process (YYYY-MM-DD, default: 2023-03-04)",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Last report date to process (YYYY-MM-DD, default: today in JST)",
    )

    args = parser.parse_args()

    start_date = parse_iso_date(args.start_date, argument_name="--start-date")

    if args.end_date:
        end_date = parse_iso_date(args.end_date, argument_name="--end-date")
    else:
        end_date = datetime.now(tz=ASIA_TOKYO).date()

    if end_date < start_date:
        parser.error("--end-date must be on or after --start-date")

    backfill_reports(
        start_date,
        end_date,
        env_file=args.env_file,
        save_output=args.save_output,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
