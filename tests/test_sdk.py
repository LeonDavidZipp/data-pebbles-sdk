from __future__ import annotations

import io
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from data_pebbles.client.models.gold_lineage_response import GoldLineageResponse
from data_pebbles.client.models.gold_metadata_response import GoldMetadataResponse
from data_pebbles.client.models.http_validation_error import HTTPValidationError
from data_pebbles.client.models.metadata_response import MetadataResponse
from data_pebbles.client.models.silver_lineage_response import SilverLineageResponse
from data_pebbles.client.models.silver_metadata_response import SilverMetadataResponse
from data_pebbles.client.models.validation_error import ValidationError
from data_pebbles.client.models.version_response import VersionResponse
from data_pebbles.sdk import (
	BronzeLayer,
	DataPebbles,
	GoldLayer,
	SilverLayer,
	_check_response,
	_read_bronze_bytes,
)

_SDK_MOD = "data_pebbles.sdk"


# ---------------------------------------------------------------------------
# Fixtures — data
# ---------------------------------------------------------------------------


@pytest.fixture()
def csv_bytes() -> bytes:
	return b"a,b\n1,2\n3,4\n"


@pytest.fixture()
def csv_bytes_semicolon() -> bytes:
	return b"a;b\n1;2\n3;4\n"


@pytest.fixture()
def parquet_bytes() -> bytes:
	buf = io.BytesIO()
	pl.DataFrame({"a": [1, 3], "b": [2, 4]}).write_parquet(buf)
	return buf.getvalue()


@pytest.fixture()
def json_bytes() -> bytes:
	return b'[{"a": 1, "b": 2}, {"a": 3, "b": 4}]'


@pytest.fixture()
def expected_dict() -> dict[str, list[int]]:
	return {"a": [1, 3], "b": [2, 4]}


# ---------------------------------------------------------------------------
# Fixtures — model instances
# ---------------------------------------------------------------------------


@pytest.fixture()
def version_response() -> VersionResponse:
	return VersionResponse(
		id=1,
		source_id=1,
		version=1,
		status="active",
		s3_key="data.csv",
		created_at="2026-01-01T00:00:00Z",
		updated_at="2026-01-01T00:00:00Z",
	)


@pytest.fixture()
def silver_lineage() -> SilverLineageResponse:
	return SilverLineageResponse(
		id=1,
		source_id=1,
		delta_version=1,
		from_source_id=1,
		created_at="2026-01-01T00:00:00Z",
	)


@pytest.fixture()
def gold_lineage() -> GoldLineageResponse:
	return GoldLineageResponse(
		id=1,
		source_id=1,
		delta_version=1,
		from_source_id=1,
		created_at="2026-01-01T00:00:00Z",
	)


# ---------------------------------------------------------------------------
# Fixtures — mock client / layers
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_client() -> MagicMock:
	client = MagicMock()
	httpx = MagicMock()
	client.get_httpx_client.return_value = httpx
	return client


@pytest.fixture()
def mock_httpx(mock_client: MagicMock) -> MagicMock:
	return mock_client.get_httpx_client()


@pytest.fixture()
def bronze(mock_client: MagicMock) -> BronzeLayer:
	return BronzeLayer(mock_client)


@pytest.fixture()
def silver(mock_client: MagicMock) -> SilverLayer:
	return SilverLayer(mock_client)


@pytest.fixture()
def gold(mock_client: MagicMock) -> GoldLayer:
	return GoldLayer(mock_client)


@pytest.fixture()
def dp() -> DataPebbles:
	with patch(f"{_SDK_MOD}.Client"):
		return DataPebbles("http://localhost")


# ---------------------------------------------------------------------------
# _read_bronze_bytes
# ---------------------------------------------------------------------------


class TestReadBronzeBytes:
	def test_csv(self, csv_bytes: bytes, expected_dict: dict[str, list[int]]):
		lf = _read_bronze_bytes(csv_bytes, ".csv")
		assert isinstance(lf, pl.LazyFrame)
		assert lf.collect().to_dict(as_series=False) == expected_dict

	def test_csv_custom_separator(
		self,
		csv_bytes_semicolon: bytes,
		expected_dict: dict[str, list[int]],
	):
		lf = _read_bronze_bytes(csv_bytes_semicolon, ".csv", csv_separator=";")
		assert lf.collect().to_dict(as_series=False) == expected_dict

	def test_parquet(self, parquet_bytes: bytes, expected_dict: dict[str, list[int]]):
		lf = _read_bronze_bytes(parquet_bytes, ".parquet")
		assert isinstance(lf, pl.LazyFrame)
		assert lf.collect().to_dict(as_series=False) == expected_dict

	def test_json(self, json_bytes: bytes, expected_dict: dict[str, list[int]]):
		lf = _read_bronze_bytes(json_bytes, ".json")
		assert isinstance(lf, pl.LazyFrame)
		assert lf.collect().to_dict(as_series=False) == expected_dict

	def test_unsupported_extension(self):
		with pytest.raises(ValueError, match="Unsupported file extension"):
			_read_bronze_bytes(b"data", ".txt")

	def test_invalid_data_raises(self):
		with pytest.raises(ValueError, match="Failed to parse"):
			_read_bronze_bytes(b"not,valid\x00\xff", ".parquet")


# ---------------------------------------------------------------------------
# _check_response
# ---------------------------------------------------------------------------


class TestCheckResponse:
	def test_passthrough(self):
		result = {"id": 1}
		assert _check_response(result) is result

	def test_validation_error_raises(self):
		err = HTTPValidationError(
			detail=[
				ValidationError(
					loc=["body", "name"], msg="field required", type_="missing"
				),
			]
		)
		with pytest.raises(ValueError, match="field required"):
			_check_response(err)

	def test_validation_error_empty_detail(self):
		err = HTTPValidationError(detail=[])
		with pytest.raises(ValueError, match="API validation error"):
			_check_response(err)


# ---------------------------------------------------------------------------
# BronzeLayer
# ---------------------------------------------------------------------------


class TestBronzeLayer:
	def test_create_source(self, bronze: BronzeLayer):
		with patch(f"{_SDK_MOD}._bronze_create") as m:
			m.sync.return_value = MetadataResponse(
				id=1, name="src", created_at="2026-01-01T00:00:00Z"
			)
			result = bronze.create_source("src")
			assert result.id == 1
			m.sync.assert_called_once()

	def test_list_sources(self, bronze: BronzeLayer):
		with patch(f"{_SDK_MOD}._bronze_list") as m:
			m.sync.return_value = [
				MetadataResponse(id=1, name="a", created_at="2026-01-01T00:00:00Z"),
			]
			assert len(bronze.list_sources()) == 1

	def test_list_sources_returns_empty_on_none(self, bronze: BronzeLayer):
		with patch(f"{_SDK_MOD}._bronze_list") as m:
			m.sync.return_value = None
			assert bronze.list_sources() == []

	def test_get_source(self, bronze: BronzeLayer):
		with patch(f"{_SDK_MOD}._bronze_get") as m:
			m.sync.return_value = MetadataResponse(
				id=1, name="s", created_at="2026-01-01T00:00:00Z"
			)
			assert bronze.get_source(1).name == "s"

	def test_update_source(self, bronze: BronzeLayer):
		with patch(f"{_SDK_MOD}._bronze_update") as m:
			m.sync.return_value = MetadataResponse(
				id=1, name="new", created_at="2026-01-01T00:00:00Z"
			)
			assert bronze.update_source(1, "new").name == "new"

	def test_delete_source(self, bronze: BronzeLayer):
		with patch(f"{_SDK_MOD}._bronze_delete") as m:
			m.sync.return_value = {"ok": True}
			assert bronze.delete_source(1) == {"ok": True}

	def test_list_versions(
		self, bronze: BronzeLayer, version_response: VersionResponse
	):
		with patch(f"{_SDK_MOD}._bronze_list_versions") as m:
			m.sync.return_value = [version_response]
			assert len(bronze.list_versions(1)) == 1

	def test_upload_with_data(self, bronze: BronzeLayer, mock_httpx: MagicMock):
		resp = MagicMock()
		resp.json.return_value = {"version": 1}
		mock_httpx.post.return_value = resp

		result = bronze.upload(1, data=b"a,b\n1,2", file_name="data.csv")
		assert result == {"version": 1}
		mock_httpx.post.assert_called_once()

	def test_upload_with_file_path(
		self, bronze: BronzeLayer, mock_httpx: MagicMock, tmp_path: Path
	):
		csv_file = tmp_path / "data.csv"
		csv_file.write_text("a,b\n1,2")

		resp = MagicMock()
		resp.json.return_value = {"version": 1}
		mock_httpx.post.return_value = resp

		assert bronze.upload(1, file_path=csv_file) == {"version": 1}

	def test_upload_rejects_unsupported_extension(self, bronze: BronzeLayer):
		with pytest.raises(ValueError, match="Unsupported file extension"):
			bronze.upload(1, data=b"data", file_name="file.txt")

	def test_upload_requires_file_path_or_data(self, bronze: BronzeLayer):
		with pytest.raises(ValueError, match="Provide either"):
			bronze.upload(1)

	def test_download_specific_version(
		self, bronze: BronzeLayer, mock_httpx: MagicMock
	):
		resp = MagicMock()
		resp.content = b"raw-bytes"
		mock_httpx.get.return_value = resp

		assert bronze.download(1, version=3) == b"raw-bytes"
		mock_httpx.get.assert_called_once_with("/bronze/1/versions/3")

	def test_download_latest_version(self, bronze: BronzeLayer, mock_httpx: MagicMock):
		resp = MagicMock()
		resp.content = b"raw-bytes"
		mock_httpx.get.return_value = resp

		with patch(f"{_SDK_MOD}._bronze_list_versions") as m:
			m.sync.return_value = [
				VersionResponse(
					id=1,
					source_id=1,
					version=1,
					status="active",
					s3_key="d.csv",
					created_at="2026-01-01T00:00:00Z",
					updated_at="2026-01-01T00:00:00Z",
				),
				VersionResponse(
					id=2,
					source_id=1,
					version=5,
					status="active",
					s3_key="d.csv",
					created_at="2026-01-01T00:00:00Z",
					updated_at="2026-01-01T00:00:00Z",
				),
			]
			assert bronze.download(1) == b"raw-bytes"
			mock_httpx.get.assert_called_once_with("/bronze/1/versions/5")

	def test_delete_version(self, bronze: BronzeLayer):
		with patch(f"{_SDK_MOD}._bronze_delete_version") as m:
			m.sync.return_value = {"ok": True}
			assert bronze.delete_version(1, 2) == {"ok": True}

	def test_activate_version(self, bronze: BronzeLayer):
		with patch(f"{_SDK_MOD}._bronze_activate") as m:
			m.sync.return_value = {"ok": True}
			assert bronze.activate_version(1, 2) == {"ok": True}

	def test_latest_version_raises_on_empty(self, bronze: BronzeLayer):
		with patch(f"{_SDK_MOD}._bronze_list_versions") as m:
			m.sync.return_value = []
			with pytest.raises(ValueError, match="No versions found"):
				bronze._latest_version(1)


# ---------------------------------------------------------------------------
# SilverLayer
# ---------------------------------------------------------------------------


class TestSilverLayer:
	def test_create_source(self, silver: SilverLayer):
		with patch(f"{_SDK_MOD}._silver_create") as m:
			m.sync.return_value = SilverMetadataResponse(
				id=1, name="s", created_at="2026-01-01T00:00:00Z"
			)
			assert silver.create_source("s").id == 1

	def test_list_sources(self, silver: SilverLayer):
		with patch(f"{_SDK_MOD}._silver_list") as m:
			m.sync.return_value = [
				SilverMetadataResponse(
					id=1, name="s", created_at="2026-01-01T00:00:00Z"
				),
			]
			assert len(silver.list_sources()) == 1

	def test_list_sources_returns_empty_on_none(self, silver: SilverLayer):
		with patch(f"{_SDK_MOD}._silver_list") as m:
			m.sync.return_value = None
			assert silver.list_sources() == []

	def test_get_source(self, silver: SilverLayer):
		with patch(f"{_SDK_MOD}._silver_get") as m:
			m.sync.return_value = SilverMetadataResponse(
				id=1, name="s", created_at="2026-01-01T00:00:00Z"
			)
			assert silver.get_source(1).name == "s"

	def test_update_source(self, silver: SilverLayer):
		with patch(f"{_SDK_MOD}._silver_update") as m:
			m.sync.return_value = SilverMetadataResponse(
				id=1, name="new", created_at="2026-01-01T00:00:00Z"
			)
			assert silver.update_source(1, "new").name == "new"

	def test_delete_source(self, silver: SilverLayer):
		with patch(f"{_SDK_MOD}._silver_delete") as m:
			m.sync.return_value = {"ok": True}
			assert silver.delete_source(1) == {"ok": True}

	def test_list_versions(
		self, silver: SilverLayer, silver_lineage: SilverLineageResponse
	):
		with patch(f"{_SDK_MOD}._silver_list_versions") as m:
			m.sync.return_value = [silver_lineage]
			assert len(silver.list_versions(1)) == 1

	def test_upload_dataframe(self, silver: SilverLayer, mock_httpx: MagicMock):
		resp = MagicMock()
		resp.json.return_value = {"version": 1}
		mock_httpx.post.return_value = resp

		df = pl.DataFrame({"x": [1, 2]})
		silver.upload(1, df, from_source_id=10)

		mock_httpx.post.assert_called_once()
		assert mock_httpx.post.call_args.kwargs["params"] == {"from_source_id": 10}

	def test_upload_lazyframe(self, silver: SilverLayer, mock_httpx: MagicMock):
		resp = MagicMock()
		resp.json.return_value = {"version": 1}
		mock_httpx.post.return_value = resp

		lf = pl.DataFrame({"x": [1, 2]}).lazy()
		silver.upload(1, lf, from_source_id=10)
		mock_httpx.post.assert_called_once()

	def test_download_specific_version(
		self,
		silver: SilverLayer,
		mock_httpx: MagicMock,
		parquet_bytes: bytes,
		expected_dict: dict[str, list[int]],
	):
		resp = MagicMock()
		resp.content = parquet_bytes
		mock_httpx.get.return_value = resp

		lf = silver.download(1, version=2)
		assert isinstance(lf, pl.LazyFrame)
		assert lf.collect().to_dict(as_series=False) == expected_dict

	def test_download_latest_version(
		self, silver: SilverLayer, mock_httpx: MagicMock, parquet_bytes: bytes
	):
		resp = MagicMock()
		resp.content = parquet_bytes
		mock_httpx.get.return_value = resp

		with patch(f"{_SDK_MOD}._silver_list_versions") as m:
			m.sync.return_value = [
				SilverLineageResponse(
					id=1,
					source_id=1,
					delta_version=1,
					from_source_id=1,
					created_at="2026-01-01T00:00:00Z",
				),
				SilverLineageResponse(
					id=2,
					source_id=1,
					delta_version=3,
					from_source_id=1,
					created_at="2026-01-01T00:00:00Z",
				),
			]
			lf = silver.download(1)
			mock_httpx.get.assert_called_once_with("/silver/1/versions/3")
			assert isinstance(lf, pl.LazyFrame)

	def test_latest_version_raises_on_empty(self, silver: SilverLayer):
		with patch(f"{_SDK_MOD}._silver_list_versions") as m:
			m.sync.return_value = []
			with pytest.raises(ValueError, match="No versions found"):
				silver._latest_version(1)


# ---------------------------------------------------------------------------
# GoldLayer
# ---------------------------------------------------------------------------


class TestGoldLayer:
	def test_create_source(self, gold: GoldLayer):
		with patch(f"{_SDK_MOD}._gold_create") as m:
			m.sync.return_value = GoldMetadataResponse(
				id=1, name="g", created_at="2026-01-01T00:00:00Z"
			)
			assert gold.create_source("g").id == 1

	def test_list_sources(self, gold: GoldLayer):
		with patch(f"{_SDK_MOD}._gold_list") as m:
			m.sync.return_value = [
				GoldMetadataResponse(id=1, name="g", created_at="2026-01-01T00:00:00Z"),
			]
			assert len(gold.list_sources()) == 1

	def test_list_sources_returns_empty_on_none(self, gold: GoldLayer):
		with patch(f"{_SDK_MOD}._gold_list") as m:
			m.sync.return_value = None
			assert gold.list_sources() == []

	def test_get_source(self, gold: GoldLayer):
		with patch(f"{_SDK_MOD}._gold_get") as m:
			m.sync.return_value = GoldMetadataResponse(
				id=1, name="g", created_at="2026-01-01T00:00:00Z"
			)
			assert gold.get_source(1).name == "g"

	def test_update_source(self, gold: GoldLayer):
		with patch(f"{_SDK_MOD}._gold_update") as m:
			m.sync.return_value = GoldMetadataResponse(
				id=1, name="new", created_at="2026-01-01T00:00:00Z"
			)
			assert gold.update_source(1, "new").name == "new"

	def test_delete_source(self, gold: GoldLayer):
		with patch(f"{_SDK_MOD}._gold_delete") as m:
			m.sync.return_value = {"ok": True}
			assert gold.delete_source(1) == {"ok": True}

	def test_list_versions(self, gold: GoldLayer, gold_lineage: GoldLineageResponse):
		with patch(f"{_SDK_MOD}._gold_list_versions") as m:
			m.sync.return_value = [gold_lineage]
			assert len(gold.list_versions(1)) == 1

	def test_upload_dataframe(self, gold: GoldLayer, mock_httpx: MagicMock):
		resp = MagicMock()
		resp.json.return_value = {"version": 1}
		mock_httpx.post.return_value = resp

		df = pl.DataFrame({"x": [1, 2]})
		gold.upload(1, df, from_source_ids=[10, 11])

		mock_httpx.post.assert_called_once()
		assert mock_httpx.post.call_args.kwargs["params"] == {"sources": [10, 11]}

	def test_upload_lazyframe(self, gold: GoldLayer, mock_httpx: MagicMock):
		resp = MagicMock()
		resp.json.return_value = {"version": 1}
		mock_httpx.post.return_value = resp

		lf = pl.DataFrame({"x": [1, 2]}).lazy()
		gold.upload(1, lf, from_source_ids=[10])
		mock_httpx.post.assert_called_once()

	def test_download_specific_version(
		self, gold: GoldLayer, mock_httpx: MagicMock, parquet_bytes: bytes
	):
		resp = MagicMock()
		resp.content = parquet_bytes
		mock_httpx.get.return_value = resp

		lf = gold.download(1, version=2)
		assert isinstance(lf, pl.LazyFrame)

	def test_download_latest_version(
		self, gold: GoldLayer, mock_httpx: MagicMock, parquet_bytes: bytes
	):
		resp = MagicMock()
		resp.content = parquet_bytes
		mock_httpx.get.return_value = resp

		with patch(f"{_SDK_MOD}._gold_list_versions") as m:
			m.sync.return_value = [
				GoldLineageResponse(
					id=1,
					source_id=1,
					delta_version=1,
					from_source_id=1,
					created_at="2026-01-01T00:00:00Z",
				),
				GoldLineageResponse(
					id=2,
					source_id=1,
					delta_version=7,
					from_source_id=1,
					created_at="2026-01-01T00:00:00Z",
				),
			]
			_ = gold.download(1)
			mock_httpx.get.assert_called_once_with("/gold/1/versions/7")

	def test_latest_version_raises_on_empty(self, gold: GoldLayer):
		with patch(f"{_SDK_MOD}._gold_list_versions") as m:
			m.sync.return_value = []
			with pytest.raises(ValueError, match="No versions found"):
				gold._latest_version(1)


# ---------------------------------------------------------------------------
# DataPebbles
# ---------------------------------------------------------------------------


class TestDataPebbles:
	def test_init_with_token(self):
		with patch(f"{_SDK_MOD}.AuthenticatedClient") as m:
			dp = DataPebbles("http://localhost", token="tok")
			m.assert_called_once_with(
				base_url="http://localhost",
				token="tok",
				raise_on_unexpected_status=True,
			)
			assert isinstance(dp.bronze, BronzeLayer)
			assert isinstance(dp.silver, SilverLayer)
			assert isinstance(dp.gold, GoldLayer)

	def test_init_without_token(self):
		with patch(f"{_SDK_MOD}.Client") as m:
			_ = DataPebbles("http://localhost")
			m.assert_called_once_with(
				base_url="http://localhost",
				raise_on_unexpected_status=True,
			)

	def test_context_manager(self):
		with patch(f"{_SDK_MOD}.Client") as m:
			mock_client = MagicMock()
			m.return_value = mock_client
			with DataPebbles("http://localhost") as dp:
				assert dp is not None
			mock_client.__enter__.assert_called_once()
			mock_client.__exit__.assert_called_once()


# ---------------------------------------------------------------------------
# silver_transform decorator
# ---------------------------------------------------------------------------


class TestSilverTransform:
	def test_basic_flow(self, dp: DataPebbles, csv_bytes: bytes):
		dp.bronze._latest_version = MagicMock(return_value=1)
		dp.bronze.list_versions = MagicMock(
			return_value=[
				VersionResponse(
					id=1,
					source_id=1,
					version=1,
					status="active",
					s3_key="data.csv",
					created_at="2026-01-01T00:00:00Z",
					updated_at="2026-01-01T00:00:00Z",
				)
			]
		)
		dp.bronze.download = MagicMock(return_value=csv_bytes)
		dp.silver.upload = MagicMock()

		@dp.silver_transform(target_id=2, from_bronze_id=1)
		def clean(lf: pl.LazyFrame) -> pl.LazyFrame:
			return lf.filter(pl.col("a") > 1)

		clean()

		dp.bronze._latest_version.assert_called_once_with(1)
		dp.bronze.download.assert_called_once_with(1, version=1)
		dp.silver.upload.assert_called_once()
		call_args = dp.silver.upload.call_args
		assert call_args.args[0] == 2
		assert call_args.kwargs["from_source_id"] == 1

	def test_specific_version(self, dp: DataPebbles, csv_bytes: bytes):
		dp.bronze.list_versions = MagicMock(
			return_value=[
				VersionResponse(
					id=1,
					source_id=1,
					version=5,
					status="active",
					s3_key="data.csv",
					created_at="2026-01-01T00:00:00Z",
					updated_at="2026-01-01T00:00:00Z",
				)
			]
		)
		dp.bronze.download = MagicMock(return_value=csv_bytes)
		dp.silver.upload = MagicMock()

		@dp.silver_transform(target_id=2, from_bronze_id=1)
		def clean(lf: pl.LazyFrame) -> pl.LazyFrame:
			return lf

		clean(version=5)
		dp.bronze.download.assert_called_once_with(1, version=5)

	def test_csv_separator(
		self,
		dp: DataPebbles,
		csv_bytes_semicolon: bytes,
		expected_dict: dict[str, list[int]],
	):
		dp.bronze._latest_version = MagicMock(return_value=1)
		dp.bronze.list_versions = MagicMock(
			return_value=[
				VersionResponse(
					id=1,
					source_id=1,
					version=1,
					status="active",
					s3_key="data.csv",
					created_at="2026-01-01T00:00:00Z",
					updated_at="2026-01-01T00:00:00Z",
				)
			]
		)
		dp.bronze.download = MagicMock(return_value=csv_bytes_semicolon)
		dp.silver.upload = MagicMock()

		@dp.silver_transform(target_id=2, from_bronze_id=1, csv_separator=";")
		def clean(lf: pl.LazyFrame) -> pl.LazyFrame:
			return lf

		clean()

		uploaded = dp.silver.upload.call_args.args[1]
		assert isinstance(uploaded, pl.LazyFrame)
		assert uploaded.collect().to_dict(as_series=False) == expected_dict

	def test_parquet_input(self, dp: DataPebbles, parquet_bytes: bytes):
		dp.bronze._latest_version = MagicMock(return_value=1)
		dp.bronze.list_versions = MagicMock(
			return_value=[
				VersionResponse(
					id=1,
					source_id=1,
					version=1,
					status="active",
					s3_key="data.parquet",
					created_at="2026-01-01T00:00:00Z",
					updated_at="2026-01-01T00:00:00Z",
				)
			]
		)
		dp.bronze.download = MagicMock(return_value=parquet_bytes)
		dp.silver.upload = MagicMock()

		@dp.silver_transform(target_id=2, from_bronze_id=1)
		def clean(lf: pl.LazyFrame) -> pl.LazyFrame:
			return lf

		clean()
		dp.silver.upload.assert_called_once()


# ---------------------------------------------------------------------------
# gold_transform decorator
# ---------------------------------------------------------------------------


class TestGoldTransform:
	def test_basic_flow(self, dp: DataPebbles, parquet_bytes: bytes):
		httpx: MagicMock = cast(Any, dp.silver._client).get_httpx_client()
		resp = MagicMock()
		resp.content = parquet_bytes
		httpx.get.return_value = resp

		dp.silver._latest_version = MagicMock(return_value=1)
		dp.gold.upload = MagicMock()

		@dp.gold_transform(target_id=3, from_silver_ids=[1, 2])
		def agg(sources: dict[int, pl.LazyFrame]) -> pl.LazyFrame:
			assert set(sources.keys()) == {1, 2}
			return pl.concat(sources.values())

		agg()

		dp.gold.upload.assert_called_once()
		call_args = dp.gold.upload.call_args
		assert call_args.args[0] == 3
		assert call_args.kwargs["from_source_ids"] == [1, 2]

	def test_result_is_uploaded(self, dp: DataPebbles, parquet_bytes: bytes):
		httpx: MagicMock = cast(Any, dp.silver._client).get_httpx_client()
		resp = MagicMock()
		resp.content = parquet_bytes
		httpx.get.return_value = resp

		dp.silver._latest_version = MagicMock(return_value=1)
		dp.gold.upload = MagicMock()

		expected = pl.DataFrame({"total": [10]})

		@dp.gold_transform(target_id=3, from_silver_ids=[1])
		def agg(sources: dict[int, pl.LazyFrame]) -> pl.DataFrame:
			return expected

		agg()

		uploaded = dp.gold.upload.call_args.args[1]
		assert uploaded.equals(expected)
