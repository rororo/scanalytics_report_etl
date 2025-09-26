## Commands

```bash
uv run python transfer.py
```

## Upload employees to s3

```bash
aws s3 cp ./sample/employees.csv s3://redshift-dwh-prod-uploads/feetaxis/employees_20250926.csv
```
