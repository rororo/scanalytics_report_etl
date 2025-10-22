## Commands

```bash
uv run python transfer.py
```

## ETL Pipeline

```bash
uv run python etl.py --env-file .env --save-output
```

Set the FTP and S3 credentials in `.env` (or export them) before running. Omit `--save-output` when running outside local development to skip writing processed CSVs to `output/`.
Add `--today YYYY-MM-DD` to backfill for a specific reference date (interpreted in JST).

## Development

Use Ruff for both linting (flake8 + isort equivalent) and formatting:

```bash
uv run ruff check .
uv run ruff check . --fix
uv run ruff format .
```

Run the Pyright type checker:

```bash
uv run pyright
```

## Upload employees to s3

```bash
aws s3 cp ./sample/employees.csv s3://redshift-dwh-prod-uploads/feetaxis/employees_20250926.csv
```
