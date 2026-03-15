# Data Pebbles SDK

Python SDK for the [Data Pebbles](https://github.com/LeonDavidZipp/data-pebbles) platform — manage data across bronze, silver, and gold layers with built-in lineage tracking.

## Installation

```bash
pip install data-pebbles
```

Requires Python 3.13+. Dependencies: `polars`, `httpx`, `mlflow`.

## Quick Start

```python
from data_pebbles import DataPebbles

dp = DataPebbles("http://localhost:8000", token="your-token")

# Bronze: upload raw files (.csv, .parquet, .json, .xlsx)
dp.bronze.create_source("raw_sales")
dp.bronze.upload(1, file_path="sales.csv")
raw = dp.bronze.download(1)

# Silver: cleaned LazyFrames with lineage
dp.silver.create_source("clean_sales")
dp.silver.upload(2, lf, from_source_id=1)
lf = dp.silver.download(2)

# Gold: aggregated LazyFrames with multi-source lineage
dp.gold.create_source("sales_summary")
dp.gold.upload(3, lf, from_source_ids=[2])
```

The client can be used as a context manager:

```python
with DataPebbles("http://localhost:8000") as dp:
    sources = dp.bronze.list_sources()
```

## Layers

### Bronze

Stores raw, unprocessed files. Only `.csv`, `.parquet`, `.json`, and `.xlsx` files are accepted.

| Method | Description |
| --- | --- |
| `create_source(name)` | Create a new source |
| `list_sources()` | List all sources |
| `get_source(source_id)` | Get source metadata |
| `update_source(source_id, name)` | Rename a source |
| `delete_source(source_id)` | Delete a source |
| `list_versions(source_id)` | List all versions |
| `upload(source_id, *, file_path=None, data=None, file_name="upload")` | Upload a file by path or raw bytes |
| `download(source_id, *, version=None)` | Download raw bytes (defaults to latest version) |
| `activate_version(source_id, version)` | Set a version as active |
| `delete_version(source_id, version)` | Delete a version |

### Silver

Stores cleaned, structured data as Parquet. Works with Polars DataFrames/LazyFrames and tracks lineage back to bronze.

| Method | Description |
| --- | --- |
| `create_source(name)` | Create a new source |
| `list_sources()` | List all sources |
| `get_source(source_id)` | Get source metadata |
| `update_source(source_id, name)` | Rename a source |
| `delete_source(source_id)` | Delete a source |
| `list_versions(source_id)` | List all versions with lineage |
| `upload(source_id, data, *, from_source_id)` | Upload a DataFrame/LazyFrame with bronze lineage |
| `download(source_id, *, version=None)` | Download as a Polars LazyFrame |

### Gold

Stores aggregated, business-ready data as Parquet. Supports multi-source lineage from silver.

| Method | Description |
| --- | --- |
| `create_source(name)` | Create a new source |
| `list_sources()` | List all sources |
| `get_source(source_id)` | Get source metadata |
| `update_source(source_id, name)` | Rename a source |
| `delete_source(source_id)` | Delete a source |
| `list_versions(source_id)` | List all versions with lineage |
| `upload(source_id, data, *, from_source_ids)` | Upload a DataFrame/LazyFrame with silver lineage |
| `download(source_id, *, version=None)` | Download as a Polars LazyFrame |

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

Transforms silver → gold. The decorated function receives a dict mapping silver source IDs to their LazyFrames and returns a `LazyFrame`.

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
