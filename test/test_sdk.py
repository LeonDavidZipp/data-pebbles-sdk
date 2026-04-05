from __future__ import annotations

import io
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from data_pebbles.client.models.create_resource_response import CreateResourceResponse
from data_pebbles.client.models.lineage_response import LineageResponse
from data_pebbles.client.models.metadata_response import MetadataResponse
from data_pebbles.client.models.project_response import ProjectResponse
from data_pebbles.client.models.version_response import VersionResponse
from data_pebbles.sdk import (
	BronzeLayer,
	DataPebbles,
	FileType,
	GoldLayer,
	RawLayer,
	SilverLayer,
	_read_bronze_bytes,
)
from data_pebbles.sdk import ProjectsLayer as ProjectsLayer

_SDK_MOD = "data_pebbles.sdk"


def _response(parsed: Any) -> MagicMock:
	"""Create a mock Response with a .parsed attribute."""
	r = MagicMock()
	r.parsed = parsed
	return r


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
def ipc_bytes() -> bytes:
	buf = io.BytesIO()
	pl.DataFrame({"a": [1, 3], "b": [2, 4]}).write_ipc_stream(buf)
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
		resource_id=1,
		version=1,
		status="active",
		s3_key="data.csv",
		created_at="2026-01-01T00:00:00Z",
		updated_at="2026-01-01T00:00:00Z",
	)


@pytest.fixture()
def lineage_response() -> LineageResponse:
	return LineageResponse(
		id=1,
		resource_id=1,
		delta_version=1,
		from_resource_id=1,
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
def raw(mock_client: MagicMock) -> RawLayer:
	return RawLayer(mock_client)


@pytest.fixture()
def silver(mock_client: MagicMock) -> SilverLayer:
	return SilverLayer(mock_client)


@pytest.fixture()
def gold(mock_client: MagicMock) -> GoldLayer:
	return GoldLayer(mock_client)


@pytest.fixture()
def projects(mock_client: MagicMock) -> ProjectsLayer:
	return ProjectsLayer(mock_client)


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
		lf = _read_bronze_bytes(
			csv_bytes_semicolon, ".csv", read_options={"separator": ";"}
		)
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
# RawLayer
# ---------------------------------------------------------------------------


class TestRawLayer:
	def test_create_resource(self, raw: RawLayer):
		with patch(f"{_SDK_MOD}._raw_create") as m:
			m.sync_detailed.return_value = _response(
				CreateResourceResponse(resource_id=1, message="ok")
			)
			result = raw.create_resource("src", project_id=1)
			assert result == 1
			m.sync_detailed.assert_called_once()

	def test_list_resources(self, raw: RawLayer):
		with patch(f"{_SDK_MOD}._raw_list") as m:
			m.sync_detailed.return_value = _response(
				[
					MetadataResponse(
						id=1,
						name="a",
						description=None,
						project_id=1,
						created_at="2026-01-01T00:00:00Z",
					),
				]
			)
			assert len(raw.list_resources()) == 1

	def test_list_resources_returns_empty_on_none(self, raw: RawLayer):
		with patch(f"{_SDK_MOD}._raw_list") as m:
			m.sync_detailed.return_value = _response(None)
			assert raw.list_resources() == []

	def test_get_resource(self, raw: RawLayer):
		with patch(f"{_SDK_MOD}._raw_get") as m:
			m.sync_detailed.return_value = _response(
				MetadataResponse(
					id=1,
					name="s",
					description=None,
					project_id=1,
					created_at="2026-01-01T00:00:00Z",
				)
			)
			assert raw.get_resource(1).name == "s"

	def test_update_resource(self, raw: RawLayer):
		with patch(f"{_SDK_MOD}._raw_update") as m:
			m.sync_detailed.return_value = _response(
				MetadataResponse(
					id=1,
					name="new",
					description=None,
					project_id=1,
					created_at="2026-01-01T00:00:00Z",
				)
			)
			assert raw.update_resource(1, "new") == 1

	def test_delete_resource(self, raw: RawLayer):
		with patch(f"{_SDK_MOD}._raw_delete") as m:
			assert raw.delete_resource(1) is None
			m.sync_detailed.assert_called_once()

	def test_list_versions(self, raw: RawLayer, version_response: VersionResponse):
		with patch(f"{_SDK_MOD}._raw_list_versions") as m:
			m.sync_detailed.return_value = _response([version_response])
			assert len(raw.list_versions(1)) == 1

	def test_upload_dataframe(self, raw: RawLayer, mock_httpx: MagicMock):
		resp = MagicMock()
		resp.json.return_value = {"version": 1}
		mock_httpx.post.return_value = resp

		df = pl.DataFrame({"x": [1, 2]})
		result = raw.upload(1, df=df)
		assert result == {"version": 1}
		mock_httpx.post.assert_called_once()
		_, kwargs = mock_httpx.post.call_args
		file_name, file_bytes, mime = kwargs["files"]["file"]
		assert file_name == "data"
		assert mime == "application/octet-stream"
		# Verify the bytes are valid IPC stream
		round_tripped = pl.read_ipc_stream(io.BytesIO(file_bytes))
		assert round_tripped.to_dict(as_series=False) == {"x": [1, 2]}

	def test_upload_lazyframe(self, raw: RawLayer, mock_httpx: MagicMock):
		resp = MagicMock()
		resp.json.return_value = {"version": 1}
		mock_httpx.post.return_value = resp

		lf = pl.DataFrame({"x": [1, 2]}).lazy()
		result = raw.upload(1, df=lf)
		assert result == {"version": 1}
		mock_httpx.post.assert_called_once()

	def test_upload_custom_file_name(self, raw: RawLayer, mock_httpx: MagicMock):
		resp = MagicMock()
		resp.json.return_value = {"version": 1}
		mock_httpx.post.return_value = resp

		df = pl.DataFrame({"x": [1]})
		raw.upload(1, df=df, file_name="custom_name")
		_, kwargs = mock_httpx.post.call_args
		file_name, _, _ = kwargs["files"]["file"]
		assert file_name == "custom_name"

	def test_upload_with_data(self, raw: RawLayer, mock_httpx: MagicMock):
		resp = MagicMock()
		resp.json.return_value = {"version": 1}
		mock_httpx.post.return_value = resp

		result = raw.upload_file(1, data=b"a,b\n1,2", file_name="data.csv")
		assert result == {"version": 1}
		mock_httpx.post.assert_called_once()

	def test_upload_with_file_path(
		self, raw: RawLayer, mock_httpx: MagicMock, tmp_path: Path
	):
		csv_file = tmp_path / "data.csv"
		csv_file.write_text("a,b\n1,2")

		resp = MagicMock()
		resp.json.return_value = {"version": 1}
		mock_httpx.post.return_value = resp

		assert raw.upload_file(1, file_path=csv_file) == {"version": 1}

	def test_upload_rejects_unsupported_extension(self, raw: RawLayer):
		with pytest.raises(ValueError, match="Unsupported file extension"):
			raw.upload_file(1, data=b"data", file_name="file.txt")

	def test_upload_requires_file_path_or_data(self, raw: RawLayer):
		with pytest.raises(ValueError, match="Provide either"):
			raw.upload_file(1)

	def test_download_specific_version(self, raw: RawLayer, mock_httpx: MagicMock):
		resp = MagicMock()
		resp.content = b"raw-bytes"
		mock_httpx.get.return_value = resp

		assert raw.download(1, version=3) == b"raw-bytes"
		mock_httpx.get.assert_called_once_with("/raw/1/versions/3")

	def test_download_latest_version(self, raw: RawLayer, mock_httpx: MagicMock):
		resp = MagicMock()
		resp.content = b"raw-bytes"
		mock_httpx.get.return_value = resp

		with patch(f"{_SDK_MOD}._raw_list_versions") as m:
			m.sync_detailed.return_value = _response(
				[
					VersionResponse(
						id=1,
						resource_id=1,
						version=1,
						status="active",
						s3_key="d.csv",
						created_at="2026-01-01T00:00:00Z",
						updated_at="2026-01-01T00:00:00Z",
					),
					VersionResponse(
						id=2,
						resource_id=1,
						version=5,
						status="active",
						s3_key="d.csv",
						created_at="2026-01-01T00:00:00Z",
						updated_at="2026-01-01T00:00:00Z",
					),
				]
			)
			assert raw.download(1) == b"raw-bytes"
			mock_httpx.get.assert_called_once_with("/raw/1/versions/5")

	def test_delete_version(self, raw: RawLayer):
		with patch(f"{_SDK_MOD}._raw_delete_version") as m:
			assert raw.delete_version(1, 2) is None
			m.sync_detailed.assert_called_once()

	def test_activate_version(self, raw: RawLayer):
		with patch(f"{_SDK_MOD}._raw_activate") as m:
			assert raw.activate_version(1, 2) is None
			m.sync_detailed.assert_called_once()

	def test_latest_version_raises_on_empty(self, raw: RawLayer):
		with patch(f"{_SDK_MOD}._raw_list_versions") as m:
			m.sync_detailed.return_value = _response([])
			with pytest.raises(ValueError, match="No versions found"):
				raw._latest_version(1)


# ---------------------------------------------------------------------------
# BronzeLayer
# ---------------------------------------------------------------------------


class TestBronzeLayer:
	def test_create_resource(self, bronze: BronzeLayer):
		with patch(f"{_SDK_MOD}._bronze_create") as m:
			m.sync_detailed.return_value = _response(
				CreateResourceResponse(resource_id=1, message="ok")
			)
			result = bronze.create_resource("src", project_id=1)
			assert result == 1
			m.sync_detailed.assert_called_once()

	def test_list_resources(self, bronze: BronzeLayer):
		with patch(f"{_SDK_MOD}._bronze_list") as m:
			m.sync_detailed.return_value = _response(
				[
					MetadataResponse(
						id=1,
						name="a",
						description=None,
						project_id=1,
						created_at="2026-01-01T00:00:00Z",
					),
				]
			)
			assert len(bronze.list_resources()) == 1

	def test_list_resources_returns_empty_on_none(self, bronze: BronzeLayer):
		with patch(f"{_SDK_MOD}._bronze_list") as m:
			m.sync_detailed.return_value = _response(None)
			assert bronze.list_resources() == []

	def test_get_resource(self, bronze: BronzeLayer):
		with patch(f"{_SDK_MOD}._bronze_get") as m:
			m.sync_detailed.return_value = _response(
				MetadataResponse(
					id=1,
					name="s",
					description=None,
					project_id=1,
					created_at="2026-01-01T00:00:00Z",
				)
			)
			assert bronze.get_resource(1).name == "s"

	def test_update_resource(self, bronze: BronzeLayer):
		with patch(f"{_SDK_MOD}._bronze_update") as m:
			m.sync_detailed.return_value = _response(
				MetadataResponse(
					id=1,
					name="new",
					description=None,
					project_id=1,
					created_at="2026-01-01T00:00:00Z",
				)
			)
			assert bronze.update_resource(1, "new") == 1

	def test_delete_resource(self, bronze: BronzeLayer):
		with patch(f"{_SDK_MOD}._bronze_delete") as m:
			assert bronze.delete_resource(1) is None
			m.sync_detailed.assert_called_once()

	def test_list_versions(
		self, bronze: BronzeLayer, lineage_response: LineageResponse
	):
		with patch(f"{_SDK_MOD}._bronze_list_versions") as m:
			m.sync_detailed.return_value = _response([lineage_response])
			assert len(bronze.list_versions(1)) == 1

	def test_upload_dataframe(self, bronze: BronzeLayer, mock_httpx: MagicMock):
		resp = MagicMock()
		resp.json.return_value = {"version": 1}
		mock_httpx.post.return_value = resp

		df = pl.DataFrame({"x": [1, 2]})
		result = bronze.upload(1, df, from_resource_id=10)
		assert result == {"version": 1}
		mock_httpx.post.assert_called_once()
		_, kwargs = mock_httpx.post.call_args
		assert kwargs["params"] == {"from_resource_id": 10}
		file_name, file_bytes, mime = kwargs["files"]["file"]
		assert file_name == "data.parquet"
		assert mime == "application/octet-stream"
		# Verify the bytes are valid Parquet
		round_tripped = pl.read_parquet(io.BytesIO(file_bytes))
		assert round_tripped.to_dict(as_series=False) == {"x": [1, 2]}

	def test_upload_lazyframe(self, bronze: BronzeLayer, mock_httpx: MagicMock):
		resp = MagicMock()
		resp.json.return_value = {"version": 1}
		mock_httpx.post.return_value = resp

		lf = pl.DataFrame({"x": [1, 2]}).lazy()
		result = bronze.upload(1, lf, from_resource_id=10)
		assert result == {"version": 1}
		mock_httpx.post.assert_called_once()

	def test_download_specific_version(
		self, bronze: BronzeLayer, mock_httpx: MagicMock, parquet_bytes: bytes
	):
		resp = MagicMock()
		resp.content = parquet_bytes
		mock_httpx.get.return_value = resp

		lf = bronze.download(1, version=3)
		assert isinstance(lf, pl.LazyFrame)
		mock_httpx.get.assert_called_once_with("/bronze/1/versions/3")

	def test_download_latest_version(
		self, bronze: BronzeLayer, mock_httpx: MagicMock, parquet_bytes: bytes
	):
		resp = MagicMock()
		resp.content = parquet_bytes
		mock_httpx.get.return_value = resp

		with patch(f"{_SDK_MOD}._bronze_list_versions") as m:
			m.sync_detailed.return_value = _response(
				[
					LineageResponse(
						id=1,
						resource_id=1,
						delta_version=1,
						from_resource_id=1,
						created_at="2026-01-01T00:00:00Z",
					),
					LineageResponse(
						id=2,
						resource_id=1,
						delta_version=5,
						from_resource_id=1,
						created_at="2026-01-01T00:00:00Z",
					),
				]
			)
			lf = bronze.download(1)
			assert isinstance(lf, pl.LazyFrame)
			mock_httpx.get.assert_called_once_with("/bronze/1/versions/5")

	def test_latest_version_raises_on_empty(self, bronze: BronzeLayer):
		with patch(f"{_SDK_MOD}._bronze_list_versions") as m:
			m.sync_detailed.return_value = _response([])
			with pytest.raises(ValueError, match="No versions found"):
				bronze._latest_version(1)


# ---------------------------------------------------------------------------
# SilverLayer
# ---------------------------------------------------------------------------


class TestSilverLayer:
	def test_create_resource(self, silver: SilverLayer):
		with patch(f"{_SDK_MOD}._silver_create") as m:
			m.sync_detailed.return_value = _response(
				CreateResourceResponse(resource_id=1, message="ok")
			)
			assert silver.create_resource("s", project_id=1) == 1

	def test_list_resources(self, silver: SilverLayer):
		with patch(f"{_SDK_MOD}._silver_list") as m:
			m.sync_detailed.return_value = _response(
				[
					MetadataResponse(
						id=1,
						name="s",
						description=None,
						project_id=1,
						created_at="2026-01-01T00:00:00Z",
					),
				]
			)
			assert len(silver.list_resources()) == 1

	def test_list_resources_returns_empty_on_none(self, silver: SilverLayer):
		with patch(f"{_SDK_MOD}._silver_list") as m:
			m.sync_detailed.return_value = _response(None)
			assert silver.list_resources() == []

	def test_get_resource(self, silver: SilverLayer):
		with patch(f"{_SDK_MOD}._silver_get") as m:
			m.sync_detailed.return_value = _response(
				MetadataResponse(
					id=1,
					name="s",
					description=None,
					project_id=1,
					created_at="2026-01-01T00:00:00Z",
				)
			)
			assert silver.get_resource(1).name == "s"

	def test_update_resource(self, silver: SilverLayer):
		with patch(f"{_SDK_MOD}._silver_update") as m:
			m.sync_detailed.return_value = _response(
				MetadataResponse(
					id=1,
					name="new",
					description=None,
					project_id=1,
					created_at="2026-01-01T00:00:00Z",
				)
			)
			assert silver.update_resource(1, "new") == 1

	def test_delete_resource(self, silver: SilverLayer):
		with patch(f"{_SDK_MOD}._silver_delete") as m:
			assert silver.delete_resource(1) is None
			m.sync_detailed.assert_called_once()

	def test_list_versions(
		self, silver: SilverLayer, lineage_response: LineageResponse
	):
		with patch(f"{_SDK_MOD}._silver_list_versions") as m:
			m.sync_detailed.return_value = _response([lineage_response])
			assert len(silver.list_versions(1)) == 1

	def test_upload_dataframe(self, silver: SilverLayer, mock_httpx: MagicMock):
		resp = MagicMock()
		resp.json.return_value = {"version": 1}
		mock_httpx.post.return_value = resp

		df = pl.DataFrame({"x": [1, 2]})
		silver.upload(1, df, from_resource_id=10)

		mock_httpx.post.assert_called_once()
		assert mock_httpx.post.call_args.kwargs["params"] == {"from_resource_id": 10}

	def test_upload_lazyframe(self, silver: SilverLayer, mock_httpx: MagicMock):
		resp = MagicMock()
		resp.json.return_value = {"version": 1}
		mock_httpx.post.return_value = resp

		lf = pl.DataFrame({"x": [1, 2]}).lazy()
		silver.upload(1, lf, from_resource_id=10)
		mock_httpx.post.assert_called_once()

	def test_download_specific_version(
		self,
		silver: SilverLayer,
		mock_httpx: MagicMock,
		ipc_bytes: bytes,
		expected_dict: dict[str, list[int]],
	):
		resp = MagicMock()
		resp.content = ipc_bytes
		mock_httpx.get.return_value = resp

		lf = silver.download(1, version=2)
		assert isinstance(lf, pl.LazyFrame)
		assert lf.collect().to_dict(as_series=False) == expected_dict

	def test_download_latest_version(
		self, silver: SilverLayer, mock_httpx: MagicMock, ipc_bytes: bytes
	):
		resp = MagicMock()
		resp.content = ipc_bytes
		mock_httpx.get.return_value = resp

		with patch(f"{_SDK_MOD}._silver_list_versions") as m:
			m.sync_detailed.return_value = _response(
				[
					LineageResponse(
						id=1,
						resource_id=1,
						delta_version=1,
						from_resource_id=1,
						created_at="2026-01-01T00:00:00Z",
					),
					LineageResponse(
						id=2,
						resource_id=1,
						delta_version=3,
						from_resource_id=1,
						created_at="2026-01-01T00:00:00Z",
					),
				]
			)
			lf = silver.download(1)
			mock_httpx.get.assert_called_once_with("/silver/1/versions/3")
			assert isinstance(lf, pl.LazyFrame)

	def test_latest_version_raises_on_empty(self, silver: SilverLayer):
		with patch(f"{_SDK_MOD}._silver_list_versions") as m:
			m.sync_detailed.return_value = _response([])
			with pytest.raises(ValueError, match="No versions found"):
				silver._latest_version(1)


# ---------------------------------------------------------------------------
# GoldLayer
# ---------------------------------------------------------------------------


class TestGoldLayer:
	def test_create_resource(self, gold: GoldLayer):
		with patch(f"{_SDK_MOD}._gold_create") as m:
			m.sync_detailed.return_value = _response(
				CreateResourceResponse(resource_id=1, message="ok")
			)
			assert gold.create_resource("g", project_id=1) == 1

	def test_list_resources(self, gold: GoldLayer):
		with patch(f"{_SDK_MOD}._gold_list") as m:
			m.sync_detailed.return_value = _response(
				[
					MetadataResponse(
						id=1,
						name="g",
						description=None,
						project_id=1,
						created_at="2026-01-01T00:00:00Z",
					),
				]
			)
			assert len(gold.list_resources()) == 1

	def test_list_resources_returns_empty_on_none(self, gold: GoldLayer):
		with patch(f"{_SDK_MOD}._gold_list") as m:
			m.sync_detailed.return_value = _response(None)
			assert gold.list_resources() == []

	def test_get_resource(self, gold: GoldLayer):
		with patch(f"{_SDK_MOD}._gold_get") as m:
			m.sync_detailed.return_value = _response(
				MetadataResponse(
					id=1,
					name="g",
					description=None,
					project_id=1,
					created_at="2026-01-01T00:00:00Z",
				)
			)
			assert gold.get_resource(1).name == "g"

	def test_update_resource(self, gold: GoldLayer):
		with patch(f"{_SDK_MOD}._gold_update") as m:
			m.sync_detailed.return_value = _response(
				MetadataResponse(
					id=1,
					name="new",
					description=None,
					project_id=1,
					created_at="2026-01-01T00:00:00Z",
				)
			)
			assert gold.update_resource(1, "new") == 1

	def test_delete_resource(self, gold: GoldLayer):
		with patch(f"{_SDK_MOD}._gold_delete") as m:
			assert gold.delete_resource(1) is None
			m.sync_detailed.assert_called_once()

	def test_list_versions(self, gold: GoldLayer, lineage_response: LineageResponse):
		with patch(f"{_SDK_MOD}._gold_list_versions") as m:
			m.sync_detailed.return_value = _response([lineage_response])
			assert len(gold.list_versions(1)) == 1

	def test_upload_dataframe(self, gold: GoldLayer, mock_httpx: MagicMock):
		resp = MagicMock()
		resp.json.return_value = {"version": 1}
		mock_httpx.post.return_value = resp

		df = pl.DataFrame({"x": [1, 2]})
		gold.upload(1, df, from_resource_ids=[10, 11])

		mock_httpx.post.assert_called_once()
		assert mock_httpx.post.call_args.kwargs["params"] == {"resources": [10, 11]}

	def test_upload_lazyframe(self, gold: GoldLayer, mock_httpx: MagicMock):
		resp = MagicMock()
		resp.json.return_value = {"version": 1}
		mock_httpx.post.return_value = resp

		lf = pl.DataFrame({"x": [1, 2]}).lazy()
		gold.upload(1, lf, from_resource_ids=[10])
		mock_httpx.post.assert_called_once()

	def test_download_specific_version(
		self, gold: GoldLayer, mock_httpx: MagicMock, ipc_bytes: bytes
	):
		resp = MagicMock()
		resp.content = ipc_bytes
		mock_httpx.get.return_value = resp

		lf = gold.download(1, version=2)
		assert isinstance(lf, pl.LazyFrame)

	def test_download_latest_version(
		self, gold: GoldLayer, mock_httpx: MagicMock, ipc_bytes: bytes
	):
		resp = MagicMock()
		resp.content = ipc_bytes
		mock_httpx.get.return_value = resp

		with patch(f"{_SDK_MOD}._gold_list_versions") as m:
			m.sync_detailed.return_value = _response(
				[
					LineageResponse(
						id=1,
						resource_id=1,
						delta_version=1,
						from_resource_id=1,
						created_at="2026-01-01T00:00:00Z",
					),
					LineageResponse(
						id=2,
						resource_id=1,
						delta_version=7,
						from_resource_id=1,
						created_at="2026-01-01T00:00:00Z",
					),
				]
			)
			_ = gold.download(1)
			mock_httpx.get.assert_called_once_with("/gold/1/versions/7")

	def test_latest_version_raises_on_empty(self, gold: GoldLayer):
		with patch(f"{_SDK_MOD}._gold_list_versions") as m:
			m.sync_detailed.return_value = _response([])
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
			assert isinstance(dp.raw, RawLayer)
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
	def test_basic_flow(self, dp: DataPebbles):
		dp.bronze._latest_version = MagicMock(return_value=1)
		dp.bronze.download = MagicMock(
			return_value=pl.DataFrame({"a": [1, 3], "b": [2, 4]}).lazy()
		)
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
		assert call_args.kwargs["from_resource_id"] == 1

	def test_specific_version(self, dp: DataPebbles):
		dp.bronze.download = MagicMock(return_value=pl.DataFrame({"a": [1]}).lazy())
		dp.silver.upload = MagicMock()

		@dp.silver_transform(target_id=2, from_bronze_id=1)
		def clean(lf: pl.LazyFrame) -> pl.LazyFrame:
			return lf

		clean(version=5)
		dp.bronze.download.assert_called_once_with(1, version=5)


# ---------------------------------------------------------------------------
# bronze_transform decorator
# ---------------------------------------------------------------------------


class TestBronzeTransform:
	def test_basic_flow(self, dp: DataPebbles, csv_bytes: bytes):
		dp.raw._latest_version = MagicMock(return_value=1)
		dp.raw.list_versions = MagicMock(
			return_value=[
				VersionResponse(
					id=1,
					resource_id=1,
					version=1,
					status="active",
					s3_key="data.csv",
					created_at="2026-01-01T00:00:00Z",
					updated_at="2026-01-01T00:00:00Z",
				)
			]
		)
		dp.raw.download = MagicMock(return_value=csv_bytes)
		dp.bronze.upload = MagicMock()

		@dp.bronze_transform(target_id=2, from_raw_id=1)
		def parse(lf: pl.LazyFrame) -> pl.LazyFrame:
			return lf.filter(pl.col("a") > 1)

		parse()

		dp.raw._latest_version.assert_called_once_with(1)
		dp.raw.download.assert_called_once_with(1, version=1)
		dp.bronze.upload.assert_called_once()
		call_args = dp.bronze.upload.call_args
		assert call_args.args[0] == 2
		assert call_args.kwargs["from_resource_id"] == 1

	def test_specific_version(self, dp: DataPebbles, csv_bytes: bytes):
		dp.raw.list_versions = MagicMock(
			return_value=[
				VersionResponse(
					id=1,
					resource_id=1,
					version=5,
					status="active",
					s3_key="data.csv",
					created_at="2026-01-01T00:00:00Z",
					updated_at="2026-01-01T00:00:00Z",
				)
			]
		)
		dp.raw.download = MagicMock(return_value=csv_bytes)
		dp.bronze.upload = MagicMock()

		@dp.bronze_transform(target_id=2, from_raw_id=1)
		def parse(lf: pl.LazyFrame) -> pl.LazyFrame:
			return lf

		parse(version=5)
		dp.raw.download.assert_called_once_with(1, version=5)

	def test_csv_separator(
		self,
		dp: DataPebbles,
		csv_bytes_semicolon: bytes,
		expected_dict: dict[str, list[int]],
	):
		dp.raw._latest_version = MagicMock(return_value=1)
		dp.raw.list_versions = MagicMock(
			return_value=[
				VersionResponse(
					id=1,
					resource_id=1,
					version=1,
					status="active",
					s3_key="data.csv",
					created_at="2026-01-01T00:00:00Z",
					updated_at="2026-01-01T00:00:00Z",
				)
			]
		)
		dp.raw.download = MagicMock(return_value=csv_bytes_semicolon)
		dp.bronze.upload = MagicMock()

		@dp.bronze_transform(
			target_id=2, from_raw_id=1, read_options={"separator": ";"}
		)
		def parse(lf: pl.LazyFrame) -> pl.LazyFrame:
			return lf

		parse()

		uploaded = dp.bronze.upload.call_args.args[1]
		assert isinstance(uploaded, pl.LazyFrame)
		assert uploaded.collect().to_dict(as_series=False) == expected_dict

	def test_file_type_override(
		self,
		dp: DataPebbles,
		csv_bytes: bytes,
		expected_dict: dict[str, list[int]],
	):
		"""file_type overrides the extension from s3_key."""
		dp.raw._latest_version = MagicMock(return_value=1)
		dp.raw.list_versions = MagicMock(
			return_value=[
				VersionResponse(
					id=1,
					resource_id=1,
					version=1,
					status="active",
					s3_key="data.unknown",
					created_at="2026-01-01T00:00:00Z",
					updated_at="2026-01-01T00:00:00Z",
				)
			]
		)
		dp.raw.download = MagicMock(return_value=csv_bytes)
		dp.bronze.upload = MagicMock()

		@dp.bronze_transform(
			target_id=2,
			from_raw_id=1,
			file_type=FileType.CSV,
		)
		def parse(lf: pl.LazyFrame) -> pl.LazyFrame:
			return lf

		parse()

		uploaded = dp.bronze.upload.call_args.args[1]
		assert isinstance(uploaded, pl.LazyFrame)
		assert uploaded.collect().to_dict(as_series=False) == expected_dict

	def test_file_type_as_string(
		self,
		dp: DataPebbles,
		csv_bytes: bytes,
	):
		"""file_type also accepts a plain string."""
		dp.raw._latest_version = MagicMock(return_value=1)
		dp.raw.list_versions = MagicMock(
			return_value=[
				VersionResponse(
					id=1,
					resource_id=1,
					version=1,
					status="active",
					s3_key="data.unknown",
					created_at="2026-01-01T00:00:00Z",
					updated_at="2026-01-01T00:00:00Z",
				)
			]
		)
		dp.raw.download = MagicMock(return_value=csv_bytes)
		dp.bronze.upload = MagicMock()

		@dp.bronze_transform(target_id=2, from_raw_id=1, file_type=".csv")
		def parse(lf: pl.LazyFrame) -> pl.LazyFrame:
			return lf

		parse()
		dp.bronze.upload.assert_called_once()

	def test_parquet_input(self, dp: DataPebbles, parquet_bytes: bytes):
		dp.raw._latest_version = MagicMock(return_value=1)
		dp.raw.list_versions = MagicMock(
			return_value=[
				VersionResponse(
					id=1,
					resource_id=1,
					version=1,
					status="active",
					s3_key="data.parquet",
					created_at="2026-01-01T00:00:00Z",
					updated_at="2026-01-01T00:00:00Z",
				)
			]
		)
		dp.raw.download = MagicMock(return_value=parquet_bytes)
		dp.bronze.upload = MagicMock()

		@dp.bronze_transform(target_id=2, from_raw_id=1)
		def parse(lf: pl.LazyFrame) -> pl.LazyFrame:
			return lf

		parse()
		dp.bronze.upload.assert_called_once()


# ---------------------------------------------------------------------------
# bronze_transform — source_id override
# ---------------------------------------------------------------------------


class TestBronzeTransformSourceId:
	def test_source_id_override(self, dp: DataPebbles, csv_bytes: bytes):
		"""When source_id is given, it replaces from_raw_id in lineage."""
		dp.raw._latest_version = MagicMock(return_value=1)
		dp.raw.list_versions = MagicMock(
			return_value=[
				VersionResponse(
					id=1,
					resource_id=1,
					version=1,
					status="active",
					s3_key="data.csv",
					created_at="2026-01-01T00:00:00Z",
					updated_at="2026-01-01T00:00:00Z",
				)
			]
		)
		dp.raw.download = MagicMock(return_value=csv_bytes)
		dp.bronze.upload = MagicMock()

		@dp.bronze_transform(target_id=2, from_raw_id=1, source_id=99)
		def parse(lf: pl.LazyFrame) -> pl.LazyFrame:
			return lf

		parse()

		assert dp.bronze.upload.call_args.kwargs["from_resource_id"] == 99

	def test_default_source_id_is_from_raw_id(self, dp: DataPebbles, csv_bytes: bytes):
		"""Without source_id, from_raw_id is recorded as lineage."""
		dp.raw._latest_version = MagicMock(return_value=1)
		dp.raw.list_versions = MagicMock(
			return_value=[
				VersionResponse(
					id=1,
					resource_id=1,
					version=1,
					status="active",
					s3_key="data.csv",
					created_at="2026-01-01T00:00:00Z",
					updated_at="2026-01-01T00:00:00Z",
				)
			]
		)
		dp.raw.download = MagicMock(return_value=csv_bytes)
		dp.bronze.upload = MagicMock()

		@dp.bronze_transform(target_id=2, from_raw_id=7)
		def parse(lf: pl.LazyFrame) -> pl.LazyFrame:
			return lf

		parse()

		assert dp.bronze.upload.call_args.kwargs["from_resource_id"] == 7


# ---------------------------------------------------------------------------
# gold_transform decorator
# ---------------------------------------------------------------------------


class TestGoldTransform:
	def test_basic_flow(self, dp: DataPebbles, ipc_bytes: bytes):
		httpx: MagicMock = cast(Any, dp.silver._client).get_httpx_client()
		resp = MagicMock()
		resp.content = ipc_bytes
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
		assert call_args.kwargs["from_resource_ids"] == [1, 2]

	def test_result_is_uploaded(self, dp: DataPebbles, ipc_bytes: bytes):
		httpx: MagicMock = cast(Any, dp.silver._client).get_httpx_client()
		resp = MagicMock()
		resp.content = ipc_bytes
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


# ---------------------------------------------------------------------------
# FileType enum
# ---------------------------------------------------------------------------


class TestFileType:
	def test_values(self):
		assert FileType.CSV == ".csv"
		assert FileType.PARQUET == ".parquet"
		assert FileType.JSON == ".json"
		assert FileType.EXCEL == ".xlsx"

	def test_str_comparison(self):
		assert FileType.CSV == ".csv"
		assert ".parquet" == FileType.PARQUET

	def test_members(self):
		assert set(FileType) == {
			FileType.CSV,
			FileType.PARQUET,
			FileType.JSON,
			FileType.EXCEL,
		}


# ---------------------------------------------------------------------------
# silver_transform — source_id override
# ---------------------------------------------------------------------------


class TestSilverTransformSourceId:
	def test_source_id_override(self, dp: DataPebbles):
		"""When source_id is given, it replaces from_bronze_id in lineage."""
		dp.bronze._latest_version = MagicMock(return_value=1)
		dp.bronze.download = MagicMock(
			return_value=pl.DataFrame({"a": [1, 3], "b": [2, 4]}).lazy()
		)
		dp.silver.upload = MagicMock()

		@dp.silver_transform(target_id=2, from_bronze_id=1, source_id=99)
		def clean(lf: pl.LazyFrame) -> pl.LazyFrame:
			return lf

		clean()

		assert dp.silver.upload.call_args.kwargs["from_resource_id"] == 99

	def test_default_source_id_is_from_bronze_id(self, dp: DataPebbles):
		"""Without source_id, from_bronze_id is recorded as lineage."""
		dp.bronze._latest_version = MagicMock(return_value=1)
		dp.bronze.download = MagicMock(
			return_value=pl.DataFrame({"a": [1, 3], "b": [2, 4]}).lazy()
		)
		dp.silver.upload = MagicMock()

		@dp.silver_transform(target_id=2, from_bronze_id=7)
		def clean(lf: pl.LazyFrame) -> pl.LazyFrame:
			return lf

		clean()

		assert dp.silver.upload.call_args.kwargs["from_resource_id"] == 7


# ---------------------------------------------------------------------------
# gold_transform — source_ids override
# ---------------------------------------------------------------------------


class TestGoldTransformSourceIds:
	def test_source_ids_override(self, dp: DataPebbles, ipc_bytes: bytes):
		"""When source_ids is given, it replaces from_silver_ids in lineage."""
		httpx: MagicMock = cast(Any, dp.silver._client).get_httpx_client()
		resp = MagicMock()
		resp.content = ipc_bytes
		httpx.get.return_value = resp

		dp.silver._latest_version = MagicMock(return_value=1)
		dp.gold.upload = MagicMock()

		@dp.gold_transform(target_id=3, from_silver_ids=[1], source_ids=[88, 89])
		def agg(sources: dict[int, pl.LazyFrame]) -> pl.LazyFrame:
			return pl.concat(sources.values())

		agg()

		assert dp.gold.upload.call_args.kwargs["from_resource_ids"] == [88, 89]

	def test_default_source_ids_is_from_silver_ids(
		self, dp: DataPebbles, ipc_bytes: bytes
	):
		"""Without source_ids, from_silver_ids is recorded as lineage."""
		httpx: MagicMock = cast(Any, dp.silver._client).get_httpx_client()
		resp = MagicMock()
		resp.content = ipc_bytes
		httpx.get.return_value = resp

		dp.silver._latest_version = MagicMock(return_value=1)
		dp.gold.upload = MagicMock()

		@dp.gold_transform(target_id=3, from_silver_ids=[5, 6])
		def agg(sources: dict[int, pl.LazyFrame]) -> pl.LazyFrame:
			return pl.concat(sources.values())

		agg()

		assert dp.gold.upload.call_args.kwargs["from_resource_ids"] == [5, 6]


# ---------------------------------------------------------------------------
# ProjectsLayer
# ---------------------------------------------------------------------------


class TestProjectsLayer:
	def test_create_project(self, projects: ProjectsLayer):
		with patch(f"{_SDK_MOD}._project_create") as m:
			from data_pebbles.client.models.create_project_response import (
				CreateProjectResponse,
			)

			m.sync_detailed.return_value = _response(
				CreateProjectResponse(project_id=42, message="ok")
			)
			assert projects.create_project("proj", description="desc") == 42
			m.sync_detailed.assert_called_once()

	def test_create_project_no_description(self, projects: ProjectsLayer):
		with patch(f"{_SDK_MOD}._project_create") as m:
			from data_pebbles.client.models.create_project_response import (
				CreateProjectResponse,
			)

			m.sync_detailed.return_value = _response(
				CreateProjectResponse(project_id=1, message="ok")
			)
			assert projects.create_project("proj") == 1

	def test_list_projects(self, projects: ProjectsLayer):
		with patch(f"{_SDK_MOD}._project_list") as m:
			m.sync_detailed.return_value = _response(
				[
					ProjectResponse(
						id=1,
						name="proj",
						description="d",
						created_at="2026-01-01T00:00:00Z",
					),
				]
			)
			result = projects.list_projects()
			assert len(result) == 1
			assert result[0].name == "proj"

	def test_list_projects_returns_empty_on_none(self, projects: ProjectsLayer):
		with patch(f"{_SDK_MOD}._project_list") as m:
			m.sync_detailed.return_value = _response(None)
			assert projects.list_projects() == []

	def test_get_project(self, projects: ProjectsLayer):
		with patch(f"{_SDK_MOD}._project_get") as m:
			m.sync_detailed.return_value = _response(
				ProjectResponse(
					id=1,
					name="proj",
					description="d",
					created_at="2026-01-01T00:00:00Z",
				)
			)
			assert projects.get_project(1).name == "proj"

	def test_update_project(self, projects: ProjectsLayer):
		with patch(f"{_SDK_MOD}._project_update") as m:
			m.sync_detailed.return_value = _response(
				ProjectResponse(
					id=1,
					name="new",
					description="d",
					created_at="2026-01-01T00:00:00Z",
				)
			)
			assert projects.update_project(1, name="new") == 1

	def test_delete_project(self, projects: ProjectsLayer):
		with patch(f"{_SDK_MOD}._project_delete") as m:
			assert projects.delete_project(1) is None
			m.sync_detailed.assert_called_once()
