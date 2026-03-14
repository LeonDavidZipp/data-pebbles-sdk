from __future__ import annotations

import io
from functools import wraps
from pathlib import Path
from typing import Any, Callable

import polars as pl

from data_pebbles.client.api.api_endpoints_for_interacting_with_the_bronze_layer import (
	activate_version_bronze_source_id_versions_version_patch as _bronze_activate,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_bronze_layer import (
	create_source_bronze_post as _bronze_create,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_bronze_layer import (
	delete_source_bronze_source_id_delete as _bronze_delete,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_bronze_layer import (
	delete_version_bronze_source_id_versions_version_delete as _bronze_delete_version,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_bronze_layer import (
	get_source_bronze_source_id_get as _bronze_get,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_bronze_layer import (
	list_sources_bronze_get as _bronze_list,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_bronze_layer import (
	list_versions_bronze_source_id_versions_get as _bronze_list_versions,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_bronze_layer import (
	update_source_bronze_source_id_patch as _bronze_update,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_gold_layer import (
	create_source_gold_post as _gold_create,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_gold_layer import (
	delete_source_gold_source_id_delete as _gold_delete,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_gold_layer import (
	get_source_gold_source_id_get as _gold_get,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_gold_layer import (
	list_sources_gold_get as _gold_list,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_gold_layer import (
	list_versions_gold_source_id_versions_get as _gold_list_versions,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_gold_layer import (
	update_source_gold_source_id_patch as _gold_update,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_silver_layer import (
	create_source_silver_post as _silver_create,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_silver_layer import (
	delete_source_silver_source_id_delete as _silver_delete,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_silver_layer import (
	get_source_silver_source_id_get as _silver_get,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_silver_layer import (
	list_sources_silver_get as _silver_list,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_silver_layer import (
	list_versions_silver_source_id_versions_get as _silver_list_versions,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_silver_layer import (
	update_source_silver_source_id_patch as _silver_update,
)
from data_pebbles.client.client import AuthenticatedClient, Client
from data_pebbles.client.models.create_gold_source_request import (
	CreateGoldSourceRequest,
)
from data_pebbles.client.models.create_silver_source_request import (
	CreateSilverSourceRequest,
)
from data_pebbles.client.models.create_source_request import CreateSourceRequest
from data_pebbles.client.models.gold_lineage_response import GoldLineageResponse
from data_pebbles.client.models.gold_metadata_response import GoldMetadataResponse
from data_pebbles.client.models.http_validation_error import HTTPValidationError
from data_pebbles.client.models.metadata_response import MetadataResponse
from data_pebbles.client.models.silver_lineage_response import SilverLineageResponse
from data_pebbles.client.models.silver_metadata_response import SilverMetadataResponse
from data_pebbles.client.models.update_gold_source_request import (
	UpdateGoldSourceRequest,
)
from data_pebbles.client.models.update_silver_source_request import (
	UpdateSilverSourceRequest,
)
from data_pebbles.client.models.update_source_request import UpdateSourceRequest
from data_pebbles.client.models.version_response import VersionResponse


def _check_response(result: Any) -> Any:
	if isinstance(result, HTTPValidationError):
		errors = result.detail or []
		messages = [e.msg for e in errors]
		raise ValueError(f"API validation error: {'; '.join(messages)}")
	return result


class BronzeLayer:
	def __init__(self, client: AuthenticatedClient | Client) -> None:
		self._client = client

	def create_source(self, name: str) -> Any:
		result = _bronze_create.sync(
			client=self._client, body=CreateSourceRequest(name=name)
		)
		return _check_response(result)

	def list_sources(self) -> list[MetadataResponse]:
		return _bronze_list.sync(client=self._client) or []

	def get_source(self, source_id: int) -> MetadataResponse:
		result = _bronze_get.sync(source_id=source_id, client=self._client)
		return _check_response(result)

	def update_source(self, source_id: int, name: str) -> MetadataResponse:
		result = _bronze_update.sync(
			source_id=source_id,
			client=self._client,
			body=UpdateSourceRequest(name=name),
		)
		return _check_response(result)

	def delete_source(self, source_id: int) -> Any:
		result = _bronze_delete.sync(source_id=source_id, client=self._client)
		return _check_response(result)

	def list_versions(self, source_id: int) -> list[VersionResponse]:
		result = _bronze_list_versions.sync(source_id=source_id, client=self._client)
		return _check_response(result) or []

	def upload(
		self,
		source_id: int,
		*,
		file_path: str | Path | None = None,
		data: bytes | None = None,
		file_name: str = "upload",
	) -> Any:
		"""Upload a file to a bronze source.

		Provide either ``file_path`` or raw ``data`` bytes.
		"""
		if file_path is not None:
			path = Path(file_path)
			file_name = path.name
			data = path.read_bytes()
		elif data is None:
			raise ValueError("Provide either file_path or data")

		response = self._client.get_httpx_client().post(
			f"/bronze/{source_id}/versions",
			files={"file": (file_name, data, "application/octet-stream")},
		)
		response.raise_for_status()
		return response.json()

	def download(self, source_id: int, *, version: int | None = None) -> bytes:
		"""Download a bronze version as raw bytes.

		If *version* is ``None``, the latest version is used.
		"""
		if version is None:
			version = self._latest_version(source_id)
		response = self._client.get_httpx_client().get(
			f"/bronze/{source_id}/versions/{version}"
		)
		response.raise_for_status()
		return response.content

	def delete_version(self, source_id: int, version: int) -> Any:
		result = _bronze_delete_version.sync(
			source_id=source_id, version=version, client=self._client
		)
		return _check_response(result)

	def activate_version(self, source_id: int, version: int) -> Any:
		result = _bronze_activate.sync(
			source_id=source_id, version=version, client=self._client
		)
		return _check_response(result)

	def _latest_version(self, source_id: int) -> int:
		versions = self.list_versions(source_id)
		if not versions:
			raise ValueError(f"No versions found for bronze source {source_id}")
		return max(v.version for v in versions)


class SilverLayer:
	def __init__(self, client: AuthenticatedClient | Client) -> None:
		self._client = client

	def create_source(self, name: str) -> Any:
		result = _silver_create.sync(
			client=self._client, body=CreateSilverSourceRequest(name=name)
		)
		return _check_response(result)

	def list_sources(self) -> list[SilverMetadataResponse]:
		return _silver_list.sync(client=self._client) or []

	def get_source(self, source_id: int) -> SilverMetadataResponse:
		result = _silver_get.sync(source_id=source_id, client=self._client)
		return _check_response(result)

	def update_source(self, source_id: int, name: str) -> SilverMetadataResponse:
		result = _silver_update.sync(
			source_id=source_id,
			client=self._client,
			body=UpdateSilverSourceRequest(name=name),
		)
		return _check_response(result)

	def delete_source(self, source_id: int) -> Any:
		result = _silver_delete.sync(source_id=source_id, client=self._client)
		return _check_response(result)

	def list_versions(self, source_id: int) -> list[SilverLineageResponse]:
		result = _silver_list_versions.sync(source_id=source_id, client=self._client)
		return _check_response(result) or []

	def upload(
		self,
		source_id: int,
		data: pl.DataFrame | pl.LazyFrame,
		*,
		from_source_id: int,
	) -> Any:
		"""Upload a DataFrame/LazyFrame to a silver source.

		Lineage is tracked via *from_source_id* (the originating bronze source).
		"""
		if isinstance(data, pl.LazyFrame):
			data = data.collect()
		buf = io.BytesIO()
		data.write_parquet(buf)
		parquet_bytes = buf.getvalue()

		response = self._client.get_httpx_client().post(
			f"/silver/{source_id}/versions",
			params={"from_source_id": from_source_id},
			files={"file": ("data.parquet", parquet_bytes, "application/octet-stream")},
		)
		response.raise_for_status()
		return response.json()

	def download(self, source_id: int, *, version: int | None = None) -> pl.LazyFrame:
		"""Download a silver version as a LazyFrame.

		If *version* is ``None``, the latest version is used.
		"""
		if version is None:
			version = self._latest_version(source_id)
		response = self._client.get_httpx_client().get(
			f"/silver/{source_id}/versions/{version}"
		)
		response.raise_for_status()
		return pl.read_parquet(io.BytesIO(response.content)).lazy()

	def _latest_version(self, source_id: int) -> int:
		versions = self.list_versions(source_id)
		if not versions:
			raise ValueError(f"No versions found for silver source {source_id}")
		return max(v.delta_version for v in versions)


class GoldLayer:
	def __init__(self, client: AuthenticatedClient | Client) -> None:
		self._client = client

	def create_source(self, name: str) -> Any:
		result = _gold_create.sync(
			client=self._client, body=CreateGoldSourceRequest(name=name)
		)
		return _check_response(result)

	def list_sources(self) -> list[GoldMetadataResponse]:
		return _gold_list.sync(client=self._client) or []

	def get_source(self, source_id: int) -> GoldMetadataResponse:
		result = _gold_get.sync(source_id=source_id, client=self._client)
		return _check_response(result)

	def update_source(self, source_id: int, name: str) -> GoldMetadataResponse:
		result = _gold_update.sync(
			source_id=source_id,
			client=self._client,
			body=UpdateGoldSourceRequest(name=name),
		)
		return _check_response(result)

	def delete_source(self, source_id: int) -> Any:
		result = _gold_delete.sync(source_id=source_id, client=self._client)
		return _check_response(result)

	def list_versions(self, source_id: int) -> list[GoldLineageResponse]:
		result = _gold_list_versions.sync(source_id=source_id, client=self._client)
		return _check_response(result) or []

	def upload(
		self,
		source_id: int,
		data: pl.DataFrame | pl.LazyFrame,
		*,
		from_source_ids: list[int],
	) -> Any:
		"""Upload a DataFrame/LazyFrame to a gold source.

		Lineage is tracked via *from_source_ids* (the originating silver sources).
		"""
		if isinstance(data, pl.LazyFrame):
			data = data.collect()
		buf = io.BytesIO()
		data.write_parquet(buf)
		parquet_bytes = buf.getvalue()

		response = self._client.get_httpx_client().post(
			f"/gold/{source_id}/versions",
			params={"sources": from_source_ids},
			files={"file": ("data.parquet", parquet_bytes, "application/octet-stream")},
		)
		response.raise_for_status()
		return response.json()

	def download(self, source_id: int, *, version: int | None = None) -> pl.LazyFrame:
		"""Download a gold version as a LazyFrame.

		If *version* is ``None``, the latest version is used.
		"""
		if version is None:
			version = self._latest_version(source_id)
		response = self._client.get_httpx_client().get(
			f"/gold/{source_id}/versions/{version}"
		)
		response.raise_for_status()
		return pl.read_parquet(io.BytesIO(response.content)).lazy()

	def _latest_version(self, source_id: int) -> int:
		versions = self.list_versions(source_id)
		if not versions:
			raise ValueError(f"No versions found for gold source {source_id}")
		return max(v.delta_version for v in versions)


class DataPebbles:
	"""High-level SDK for the Data Pebbles platform.

	Usage::

	                dp = DataPebbles("https://api.example.com", token="...")

	                # Bronze: upload raw files
	                dp.bronze.create_source("raw_sales")
	                dp.bronze.upload(1, file_path="sales.csv")

	                # Silver / Gold: work with LazyFrames
	                df = dp.silver.download(2)
	                dp.gold.upload(3, df, from_source_ids=[2])


	                # Decorators for lineage-tracked pipelines
	                @dp.silver_transform(target=2, from_bronze=1)
	                def clean(raw: bytes) -> pl.LazyFrame:
	                    return pl.read_csv(raw).lazy()


	                clean()  # runs on latest bronze version
	                clean(version=5)  # runs on a specific bronze version
	"""

	def __init__(self, base_url: str, *, token: str | None = None) -> None:
		if token:
			self._client: AuthenticatedClient | Client = AuthenticatedClient(
				base_url=base_url,
				token=token,
				raise_on_unexpected_status=True,
			)
		else:
			self._client = Client(
				base_url=base_url,
				raise_on_unexpected_status=True,
			)
		self._bronze = BronzeLayer(self._client)
		self._silver = SilverLayer(self._client)
		self._gold = GoldLayer(self._client)

	@property
	def bronze(self) -> BronzeLayer:
		return self._bronze

	@property
	def silver(self) -> SilverLayer:
		return self._silver

	@property
	def gold(self) -> GoldLayer:
		return self._gold

	def __enter__(self) -> DataPebbles:
		self._client.__enter__()
		return self

	def __exit__(self, *args: Any) -> None:
		self._client.__exit__(*args)

	# ------------------------------------------------------------------
	# Lineage-tracked transform decorators
	# ------------------------------------------------------------------

	def silver_transform(
		self,
		*,
		target: int,
		from_bronze: int,
	) -> Callable[
		[Callable[[bytes], pl.DataFrame | pl.LazyFrame]],
		Callable[..., None],
	]:
		"""Decorator: bronze → silver with automatic lineage tracking.

		The decorated function receives the raw bronze bytes and must
		return a ``DataFrame`` or ``LazyFrame``.  The result is serialised
		to Parquet and uploaded to the *target* silver source, recording
		*from_bronze* as the originating bronze source.

		Usage::

		                @dp.silver_transform(target=2, from_bronze=1)
		                def clean(raw: bytes) -> pl.LazyFrame:
		                    return pl.read_csv(raw).lazy().filter(pl.col("x") > 0)


		                clean()  # uses latest bronze version
		                clean(version=5)  # uses specific bronze version
		"""

		def decorator(
			func: Callable[[bytes], pl.DataFrame | pl.LazyFrame],
		) -> Callable[..., None]:
			@wraps(func)
			def wrapper(*, version: int | None = None) -> None:
				bronze_data = self.bronze.download(from_bronze, version=version)
				result = func(bronze_data)
				self.silver.upload(target, result, from_source_id=from_bronze)

			return wrapper

		return decorator

	def gold_transform(
		self,
		*,
		target: int,
		from_silver: list[int],
	) -> Callable[
		[Callable[[dict[int, pl.LazyFrame]], pl.DataFrame | pl.LazyFrame]],
		Callable[..., None],
	]:
		"""Decorator: silver → gold with automatic lineage tracking.

		The decorated function receives a ``dict`` mapping each silver
		source ID to its ``LazyFrame``.  The result is serialised to
		Parquet and uploaded to the *target* gold source, recording all
		*from_silver* IDs as the originating silver sources.

		Usage::

		                @dp.gold_transform(target=3, from_silver=[1, 2])
		                def aggregate(sources: dict[int, pl.LazyFrame]) -> pl.LazyFrame:
		                    return pl.concat(sources.values()).group_by("category").agg(pl.sum("amount"))


		                aggregate()
		"""

		def decorator(
			func: Callable[[dict[int, pl.LazyFrame]], pl.DataFrame | pl.LazyFrame],
		) -> Callable[..., None]:
			@wraps(func)
			def wrapper() -> None:
				silver_data = {sid: self.silver.download(sid) for sid in from_silver}
				result = func(silver_data)
				self.gold.upload(target, result, from_source_ids=from_silver)

			return wrapper

		return decorator
