# Schema definitions based on table definitions in docs/transfer.md

SCAN_REPORT_COLUMNS: list[str] = [
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
    "created_at",
]


SCHEMAS: dict[str, dict] = {
    "scan_report_daily": {
        "columns": SCAN_REPORT_COLUMNS,
        "not_null": ["scan_date", "store_id", "employee_id", "safesize_code"],
        "validations": {
            "store_id": {"type": "numeric", "pattern": r"^[0-9]+$"},
            "employee_id": {"type": "numeric", "pattern": r"^[0-9]{7}$"},
        },
    },
    "scan_report_weekly": {
        "columns": SCAN_REPORT_COLUMNS,
        "not_null": ["scan_date", "store_id", "employee_id", "safesize_code"],
        "validations": {
            "store_id": {"type": "numeric", "pattern": r"^[0-9]+$"},
            "employee_id": {"type": "numeric", "pattern": r"^[0-9]{7}$"},
        },
    },
}
