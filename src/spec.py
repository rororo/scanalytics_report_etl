from src.time_utils import ASIA_TOKYO
from dataclasses import dataclass
from pathlib import Path
from datetime import timedelta, datetime


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
