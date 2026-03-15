# Data Pebbles SDK

Python SDK for the [Data Pebbles](https://github.com/LeonDavidZipp/data-pebbles) platform — manage data across bronze, silver, and gold layers with built-in lineage tracking.

## Installation

```bash
pip install data-pebbles
```

or with `uv`:

```bash
uv pip install data-pebbles
```

Requires Python 3.13+. Dependencies: `polars`, `httpx`, `mlflow`.

## Quick Start

```python
from data_pebbles import DataPebbles

dp = DataPebbles("http://localhost:8000", token="your-token")

# Projects: create and list projects
dp.projects.create_project("analytics", description="Analytics workspace")
projects = dp.projects.list_projects()

# Bronze: create a resource in a project and upload (.csv, .parquet, .json, .xlsx)
dp.bronze.create_resource("raw_sales", project_id=projects[0].id)
dp.bronze.upload(projects[0].id, file_path="sales.csv")
raw = dp.bronze.download(projects[0].id)

# Silver: create resource inside a project and upload LazyFrame with lineage
dp.silver.create_resource("clean_sales", project_id=projects[0].id)
dp.silver.upload(2, lf, from_resource_id=1)
lf = dp.silver.download(2)

# Gold: aggregated LazyFrames with multi-source lineage
dp.gold.create_resource("sales_summary", project_id=projects[0].id)
dp.gold.upload(3, lf, from_resource_ids=[2])
```

The client can be used as a context manager:

```python
with DataPebbles("http://localhost:8000") as dp:
    resources = dp.bronze.list_resources()
```

## Layers

### Bronze

Stores raw, unprocessed files. Only `.csv`, `.parquet`, `.json`, and `.xlsx` files are accepted.

| Method | Description |
| --- | --- |
| `create_resource(name)` | Create a new resource |
| `list_resources()` | List all resources |
| `get_resource(resource_id)` | Get resource metadata |
| `update_resource(resource_id, name)` | Rename a resource |
| `delete_resource(resource_id)` | Delete a resource |
| `list_versions(resource_id)` | List all versions |
| `upload(resource_id, *, file_path=None, data=None, file_name="upload")` | Upload a file by path or raw bytes |
| `download(resource_id, *, version=None)` | Download raw bytes (defaults to latest version) |
| `activate_version(resource_id, version)` | Set a version as active |
| `delete_version(resource_id, version)` | Delete a version |

### Silver

Stores cleaned, structured data as Parquet. Works with Polars DataFrames/LazyFrames and tracks lineage back to bronze.

| Method | Description |
| --- | --- |
| `create_resource(name)` | Create a new resource |
| `list_resources()` | List all resources |
| `get_resource(resource_id)` | Get resource metadata |
| `update_resource(resource_id, name)` | Rename a resource |
| `delete_resource(resource_id)` | Delete a resource |
| `list_versions(resource_id)` | List all versions with lineage |
| `upload(resource_id, data, *, from_resource_id)` | Upload a DataFrame/LazyFrame with bronze lineage |
| `download(resource_id, *, version=None)` | Download as a Polars LazyFrame |

### Gold

Stores aggregated, business-ready data as Parquet. Supports multi-source lineage from silver.

| Method | Description |
| --- | --- |
| `create_resource(name)` | Create a new resource |
| `list_resources()` | List all resources |
| `get_resource(resource_id)` | Get resource metadata |
| `update_resource(resource_id, name)` | Rename a resource |
| `delete_resource(resource_id)` | Delete a resource |
| `list_versions(resource_id)` | List all versions with lineage |
| `upload(resource_id, data, *, from_resource_ids)` | Upload a DataFrame/LazyFrame with silver lineage |
| `download(resource_id, *, version=None)` | Download as a Polars LazyFrame |

## Transform Decorators

Automate downloading, transforming, and uploading data between layers with lineage tracking.

### silver_transform

Transforms bronze → silver. The decorator auto-parses bronze data into a `LazyFrame` based on the original file extension. The decorated function receives a `LazyFrame` and returns a `LazyFrame`.

```python
@dp.silver_transform(target_id=2, from_bronze_id=1)
def clean(lf: pl.LazyFrame) -> pl.LazyFrame:
    return lf.filter(pl.col("amount") > 0)

clean()            # uses latest bronze version
clean(version=5)   # uses a specific bronze version
```

For CSV files with a non-standard delimiter, use `csv_separator`:

```python
@dp.silver_transform(target_id=2, from_bronze_id=1, csv_separator=";")
def clean_eu(lf: pl.LazyFrame) -> pl.LazyFrame:
    return lf.filter(pl.col("amount") > 0)
```

### gold_transform

Transforms silver → gold. The decorated function receives a dict mapping silver resource IDs to their LazyFrames and returns a `LazyFrame`.

```python
@dp.gold_transform(target_id=3, from_silver_ids=[1, 2])
def aggregate(sources: dict[int, pl.LazyFrame]) -> pl.LazyFrame:
    return (
        pl.concat(sources.values())
        .group_by("category")
        .agg(pl.sum("amount"))
    )

aggregate()
```
