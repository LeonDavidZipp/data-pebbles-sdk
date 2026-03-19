from __future__ import annotations

import io
from abc import ABC, abstractmethod
from enum import StrEnum
from functools import wraps
from pathlib import Path
from typing import Any, Callable

import polars as pl

from data_pebbles.client.api.api_endpoints_for_interacting_with_the_bronze_layer import (  # noqa: E501
	activate_version_bronze_resource_id_versions_version_patch as _bronze_activate,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_bronze_layer import (  # noqa: E501
	create_resource_bronze_post as _bronze_create,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_bronze_layer import (  # noqa: E501
	delete_resource_bronze_resource_id_delete as _bronze_delete,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_bronze_layer import (  # noqa: E501
	delete_version_bronze_resource_id_versions_version_delete as _bronze_delete_version,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_bronze_layer import (  # noqa: E501
	get_resource_bronze_resource_id_get as _bronze_get,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_bronze_layer import (  # noqa: E501
	list_resources_bronze_get as _bronze_list,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_bronze_layer import (  # noqa: E501
	list_versions_bronze_resource_id_versions_get as _bronze_list_versions,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_bronze_layer import (  # noqa: E501
	update_resource_bronze_resource_id_patch as _bronze_update,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_gold_layer import (
	create_resource_gold_post as _gold_create,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_gold_layer import (
	delete_resource_gold_resource_id_delete as _gold_delete,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_gold_layer import (
	get_resource_gold_resource_id_get as _gold_get,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_gold_layer import (
	list_resources_gold_get as _gold_list,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_gold_layer import (
	list_versions_gold_resource_id_versions_get as _gold_list_versions,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_gold_layer import (
	update_resource_gold_resource_id_patch as _gold_update,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_silver_layer import (  # noqa: E501
	create_resource_silver_post as _silver_create,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_silver_layer import (  # noqa: E501
	delete_resource_silver_resource_id_delete as _silver_delete,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_silver_layer import (  # noqa: E501
	get_resource_silver_resource_id_get as _silver_get,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_silver_layer import (  # noqa: E501
	list_resources_silver_get as _silver_list,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_silver_layer import (  # noqa: E501
	list_versions_silver_resource_id_versions_get as _silver_list_versions,
)
from data_pebbles.client.api.api_endpoints_for_interacting_with_the_silver_layer import (  # noqa: E501
	update_resource_silver_resource_id_patch as _silver_update,
)
from data_pebbles.client.api.api_endpoints_for_managing_projects import (
	create_project_projects_post as _project_create,
)
from data_pebbles.client.api.api_endpoints_for_managing_projects import (
	delete_project_projects_project_id_delete as _project_delete,
)
from data_pebbles.client.api.api_endpoints_for_managing_projects import (
	get_project_projects_project_id_get as _project_get,
)
from data_pebbles.client.api.api_endpoints_for_managing_projects import (
	list_projects_projects_get as _project_list,
)
from data_pebbles.client.api.api_endpoints_for_managing_projects import (
	update_project_projects_project_id_patch as _project_update,
)
from data_pebbles.client.client import AuthenticatedClient, Client
from data_pebbles.client.models.create_gold_resource_request import (
	CreateGoldResourceRequest,
)
from data_pebbles.client.models.create_project_request import (
	CreateProjectRequest,
)
from data_pebbles.client.models.create_project_response import (
	CreateProjectResponse,
)
from data_pebbles.client.models.create_resource_request import CreateResourceRequest
from data_pebbles.client.models.create_resource_response import (
	CreateResourceResponse,
)
from data_pebbles.client.models.create_silver_resource_request import (
	CreateSilverResourceRequest,
)
from data_pebbles.client.models.gold_lineage_response import GoldLineageResponse
from data_pebbles.client.models.gold_metadata_response import GoldMetadataResponse
from data_pebbles.client.models.metadata_response import MetadataResponse
from data_pebbles.client.models.project_response import ProjectResponse
from data_pebbles.client.models.silver_lineage_response import SilverLineageResponse
from data_pebbles.client.models.silver_metadata_response import SilverMetadataResponse
from data_pebbles.client.models.update_gold_resource_request import (
	UpdateGoldResourceRequest,
)
from data_pebbles.client.models.update_project_request import (
	UpdateProjectRequest,
)
from data_pebbles.client.models.update_resource_request import UpdateResourceRequest
from data_pebbles.client.models.update_silver_resource_request import (
	UpdateSilverResourceRequest,
)
from data_pebbles.client.models.version_response import VersionResponse


class FileType(StrEnum):
	"""Supported bronze file types.

	Each value corresponds to a file extension and maps to a Polars reader:

	- ``CSV``  → ``pl.read_csv``
	- ``PARQUET`` → ``pl.read_parquet``
	- ``JSON`` → ``pl.read_json``
	- ``EXCEL`` → ``pl.read_excel``
	"""

	CSV = ".csv"
	PARQUET = ".parquet"
	JSON = ".json"
	EXCEL = ".xlsx"


ALLOWED_EXTENSIONS = {ft.value for ft in FileType}

_READERS: dict[str, Callable[..., pl.DataFrame]] = {
	FileType.CSV: pl.read_csv,
	FileType.PARQUET: pl.read_parquet,
	FileType.JSON: pl.read_json,
	FileType.EXCEL: pl.read_excel,
}


def _read_bronze_bytes(
	data: bytes,
	ext: str,
	*,
	read_options: dict[str, Any] | None = None,
) -> pl.LazyFrame:
	"""Parse raw bronze bytes into a LazyFrame based on file extension.

	*read_options* are forwarded as keyword arguments to the corresponding
	Polars reader function (e.g. ``{"separator": ";"}`` for CSV).
	"""
	reader = _READERS.get(ext)
	if reader is None:
		raise ValueError(
			f"Unsupported file extension {ext!r}. "
			f"Allowed: {', '.join(sorted(ALLOWED_EXTENSIONS))}"
		)
	buf = io.BytesIO(data)
	try:
		return reader(buf, **(read_options or {})).lazy()
	except Exception as exc:
		raise ValueError(
			f"Failed to parse bronze data as {ext!r}. "
			f"Uploaded files must contain valid tabular data. "
			f"Original error: {exc}"
		) from exc


class BronzeLayer:
	def __init__(self, client: AuthenticatedClient | Client) -> None:
		self._client = client

	def create_resource(self, name: str, project_id: int) -> int:
		"""Create a new bronze resource.

		Args:
			name (str): Human-readable name for the resource.
			project_id (int): ID of the project this resource belongs to.

		Returns:
			int: The ID of the newly created bronze resource.
		"""
		result = _bronze_create.sync_detailed(
			client=self._client,
			body=CreateResourceRequest(name=name, project_id=project_id),
		)
		if isinstance(result.parsed, CreateResourceResponse):
			return result.parsed.resource_id
		raise ValueError(f"Unexpected response: {result.parsed}")

	def list_resources(self) -> list[MetadataResponse]:
		"""List all bronze resources.

		Returns:
			list[MetadataResponse]: Metadata for every bronze resource.
		"""
		result = _bronze_list.sync_detailed(client=self._client)
		if isinstance(result.parsed, list):
			return result.parsed
		return []

	def get_resource(self, resource_id: int) -> MetadataResponse:
		"""Get metadata for a single bronze resource.

		Args:
			resource_id (int): ID of the bronze resource.

		Returns:
			MetadataResponse: The resource metadata.
		"""
		result = _bronze_get.sync_detailed(resource_id=resource_id, client=self._client)
		if isinstance(result.parsed, MetadataResponse):
			return result.parsed
		raise ValueError(f"Unexpected response: {result.parsed}")

	def update_resource(self, resource_id: int, name: str) -> int:
		"""Rename a bronze resource.

		Args:
			resource_id (int): ID of the bronze resource to update.
			name (str): New name for the resource.

		Returns:
			int: The ID of the updated resource.
		"""
		result = _bronze_update.sync_detailed(
			resource_id=resource_id,
			client=self._client,
			body=UpdateResourceRequest(name=name),
		)
		if isinstance(result.parsed, MetadataResponse):
			return result.parsed.id
		raise ValueError(f"Unexpected response: {result.parsed}")

	def delete_resource(self, resource_id: int) -> None:
		"""Delete a bronze resource and all its versions.

		Args:
			resource_id (int): ID of the bronze resource to delete.
		"""
		_bronze_delete.sync_detailed(resource_id=resource_id, client=self._client)

	def list_versions(self, resource_id: int) -> list[VersionResponse]:
		"""List all versions of a bronze resource.

		Args:
			resource_id (int): ID of the bronze resource.

		Returns:
			list[VersionResponse]: Version metadata for each uploaded version.
		"""
		result = _bronze_list_versions.sync_detailed(
			resource_id=resource_id, client=self._client
		)
		if isinstance(result.parsed, list):
			return result.parsed
		return []

	def upload(
		self,
		resource_id: int,
		*,
		df: pl.DataFrame | pl.LazyFrame,
		file_name: str = "data",
	) -> Any:
		"""Upload a DataFrame/LazyFrame to a bronze resource as IPC stream.

		The data is serialised via ``write_ipc_stream`` and posted to
		the same ``/bronze/{resource_id}/versions`` endpoint used by
		:meth:`upload_file`.

		Args:
			resource_id (int): ID of the bronze resource to upload to.
			df (pl.DataFrame | pl.LazyFrame): The data to upload. A
				LazyFrame is collected before serialisation.
			file_name (str): Name stored alongside the upload. Defaults
				to ``"data"``.

		Returns:
			dict[str, Any]: JSON response from the server.
		"""
		if isinstance(df, pl.LazyFrame):
			df = df.collect()
		buf = df.write_ipc_stream(None)
		ipc_bytes = buf.getvalue()

		response = self._client.get_httpx_client().post(
			f"/bronze/{resource_id}/versions",
			files={"file": (file_name, ipc_bytes, "application/octet-stream")},
		)
		response.raise_for_status()
		return response.json()

	def upload_file(
		self,
		resource_id: int,
		*,
		file_path: str | Path | None = None,
		data: bytes | None = None,
		file_name: str = "upload",
	) -> Any:
		"""Upload a file to a bronze resource.

		Provide either *file_path* or raw *data* bytes.
		Only files with extensions in ``ALLOWED_EXTENSIONS``
		(.csv, .parquet, .json, .xlsx) are accepted.

		Args:
			resource_id (int): ID of the bronze resource to upload to.
			file_path (str | Path | None): Path to a local file. If given,
				*data* and *file_name* are derived from it.
			data (bytes | None): Raw file bytes. Required when *file_path*
				is not provided.
			file_name (str): Name (with extension) to store alongside the
				upload. Defaults to ``"upload"``.

		Returns:
			dict[str, Any]: JSON response from the server.
		"""
		if file_path is not None:
			path = Path(file_path)
			file_name = path.name
			data = path.read_bytes()
		elif data is None:
			raise ValueError("Provide either file_path or data")

		ext = Path(file_name).suffix.lower()
		if ext not in ALLOWED_EXTENSIONS:
			raise ValueError(
				f"Unsupported file extension {ext!r}. "
				f"Allowed: {', '.join(sorted(ALLOWED_EXTENSIONS))}"
			)

		response = self._client.get_httpx_client().post(
			f"/bronze/{resource_id}/versions",
			files={"file": (file_name, data, "application/octet-stream")},
		)
		response.raise_for_status()
		return response.json()

	def download(self, resource_id: int, *, version: int | None = None) -> bytes:
		"""Download a bronze version as raw bytes.

		Args:
			resource_id (int): ID of the bronze resource.
			version (int | None): Specific version to download. If ``None``,
				the latest version is used.

		Returns:
			bytes: The raw file content.
		"""
		if version is None:
			version = self._latest_version(resource_id)
		response = self._client.get_httpx_client().get(
			f"/bronze/{resource_id}/versions/{version}"
		)
		response.raise_for_status()
		return response.content

	def delete_version(self, resource_id: int, version: int) -> None:
		"""Delete a specific version of a bronze resource.

		Args:
			resource_id (int): ID of the bronze resource.
			version (int): Version number to delete.
		"""
		_bronze_delete_version.sync_detailed(
			resource_id=resource_id, version=version, client=self._client
		)

	def activate_version(self, resource_id: int, version: int) -> None:
		"""Activate a specific version of a bronze resource.

		Args:
			resource_id (int): ID of the bronze resource.
			version (int): Version number to activate.
		"""
		_bronze_activate.sync_detailed(
			resource_id=resource_id, version=version, client=self._client
		)

	def _latest_version(self, resource_id: int) -> int:
		versions = self.list_versions(resource_id)
		if not versions:
			raise ValueError(f"No versions found for bronze resource {resource_id}")
		return max(v.version for v in versions)


class SilverLayer:
	def __init__(self, client: AuthenticatedClient | Client) -> None:
		self._client = client

	def create_resource(self, name: str, project_id: int) -> int:
		"""Create a new silver resource.

		Args:
			name (str): Human-readable name for the resource.
			project_id (int): ID of the project this resource belongs to.

		Returns:
			int: The ID of the newly created silver resource.
		"""
		result = _silver_create.sync_detailed(
			client=self._client,
			body=CreateSilverResourceRequest(name=name, project_id=project_id),
		)
		if isinstance(result.parsed, CreateResourceResponse):
			return result.parsed.resource_id
		raise ValueError(f"Unexpected response: {result.parsed}")

	def list_resources(self) -> list[SilverMetadataResponse]:
		"""List all silver resources.

		Returns:
			list[SilverMetadataResponse]: Metadata for every silver resource.
		"""
		result = _silver_list.sync_detailed(client=self._client)
		if isinstance(result.parsed, list):
			return result.parsed
		return []

	def get_resource(self, resource_id: int) -> SilverMetadataResponse:
		"""Get metadata for a single silver resource.

		Args:
			resource_id (int): ID of the silver resource.

		Returns:
			SilverMetadataResponse: The resource metadata.
		"""
		result = _silver_get.sync_detailed(resource_id=resource_id, client=self._client)
		if isinstance(result.parsed, SilverMetadataResponse):
			return result.parsed
		raise ValueError(f"Unexpected response: {result.parsed}")

	def update_resource(self, resource_id: int, name: str) -> int:
		"""Rename a silver resource.

		Args:
			resource_id (int): ID of the silver resource to update.
			name (str): New name for the resource.

		Returns:
			int: The ID of the updated resource.
		"""
		result = _silver_update.sync_detailed(
			resource_id=resource_id,
			client=self._client,
			body=UpdateSilverResourceRequest(name=name),
		)
		if isinstance(result.parsed, SilverMetadataResponse):
			return result.parsed.id
		raise ValueError(f"Unexpected response: {result.parsed}")

	def delete_resource(self, resource_id: int) -> None:
		"""Delete a silver resource and all its versions.

		Args:
			resource_id (int): ID of the silver resource to delete.
		"""
		_silver_delete.sync_detailed(resource_id=resource_id, client=self._client)

	def list_versions(self, resource_id: int) -> list[SilverLineageResponse]:
		"""List all versions of a silver resource.

		Args:
			resource_id (int): ID of the silver resource.

		Returns:
			list[SilverLineageResponse]: Lineage metadata for each version.
		"""
		result = _silver_list_versions.sync_detailed(
			resource_id=resource_id, client=self._client
		)
		if isinstance(result.parsed, list):
			return result.parsed
		return []

	def upload(
		self,
		resource_id: int,
		df: pl.DataFrame | pl.LazyFrame,
		*,
		from_resource_id: int,
	) -> Any:
		"""Upload a DataFrame/LazyFrame to a silver resource.

		Lineage is tracked via *from_resource_id* (the originating bronze
		resource).

		Args:
			resource_id (int): ID of the silver resource to upload to.
			df (pl.DataFrame | pl.LazyFrame): The data to upload. A
				LazyFrame is collected before serialisation.
			from_resource_id (int): ID of the originating bronze resource
				(recorded for lineage).

		Returns:
			dict[str, Any]: JSON response from the server.
		"""
		if isinstance(df, pl.LazyFrame):
			df = df.collect()
		buf = io.BytesIO()
		df.write_parquet(buf)
		parquet_bytes = buf.getvalue()

		response = self._client.get_httpx_client().post(
			f"/silver/{resource_id}/versions",
			params={"from_resource_id": from_resource_id},
			files={"file": ("data.parquet", parquet_bytes, "application/octet-stream")},
		)
		response.raise_for_status()
		return response.json()

	def download(self, resource_id: int, *, version: int | None = None) -> pl.LazyFrame:
		"""Download a silver version as a LazyFrame.

		Args:
			resource_id (int): ID of the silver resource.
			version (int | None): Specific version to download. If ``None``,
				the latest version is used.

		Returns:
			pl.LazyFrame: The downloaded data.
		"""
		if version is None:
			version = self._latest_version(resource_id)
		response = self._client.get_httpx_client().get(
			f"/silver/{resource_id}/versions/{version}"
		)
		response.raise_for_status()
		return pl.read_ipc_stream(io.BytesIO(response.content)).lazy()

	def _latest_version(self, resource_id: int) -> int:
		versions = self.list_versions(resource_id)
		if not versions:
			raise ValueError(f"No versions found for silver resource {resource_id}")
		return max(v.delta_version for v in versions)


class GoldLayer:
	def __init__(self, client: AuthenticatedClient | Client) -> None:
		self._client = client

	def create_resource(self, name: str, project_id: int) -> int:
		"""Create a new gold resource.

		Args:
			name (str): Human-readable name for the resource.
			project_id (int): ID of the project this resource belongs to.

		Returns:
			int: The ID of the newly created gold resource.
		"""
		result = _gold_create.sync_detailed(
			client=self._client,
			body=CreateGoldResourceRequest(name=name, project_id=project_id),
		)
		if isinstance(result.parsed, CreateResourceResponse):
			return result.parsed.resource_id
		raise ValueError(f"Unexpected response: {result.parsed}")

	def list_resources(self) -> list[GoldMetadataResponse]:
		"""List all gold resources.

		Returns:
			list[GoldMetadataResponse]: Metadata for every gold resource.
		"""
		result = _gold_list.sync_detailed(client=self._client)
		if isinstance(result.parsed, list):
			return result.parsed
		return []

	def get_resource(self, resource_id: int) -> GoldMetadataResponse:
		"""Get metadata for a single gold resource.

		Args:
			resource_id (int): ID of the gold resource.

		Returns:
			GoldMetadataResponse: The resource metadata.
		"""
		result = _gold_get.sync_detailed(resource_id=resource_id, client=self._client)
		if isinstance(result.parsed, GoldMetadataResponse):
			return result.parsed
		raise ValueError(f"Unexpected response: {result.parsed}")

	def update_resource(self, resource_id: int, name: str) -> int:
		"""Rename a gold resource.

		Args:
			resource_id (int): ID of the gold resource to update.
			name (str): New name for the resource.

		Returns:
			int: The ID of the updated resource.
		"""
		result = _gold_update.sync_detailed(
			resource_id=resource_id,
			client=self._client,
			body=UpdateGoldResourceRequest(name=name),
		)
		if isinstance(result.parsed, GoldMetadataResponse):
			return result.parsed.id
		raise ValueError(f"Unexpected response: {result.parsed}")

	def delete_resource(self, resource_id: int) -> None:
		"""Delete a gold resource and all its versions.

		Args:
			resource_id (int): ID of the gold resource to delete.
		"""
		_gold_delete.sync_detailed(resource_id=resource_id, client=self._client)

	def list_versions(self, resource_id: int) -> list[GoldLineageResponse]:
		"""List all versions of a gold resource.

		Args:
			resource_id (int): ID of the gold resource.

		Returns:
			list[GoldLineageResponse]: Lineage metadata for each version.
		"""
		result = _gold_list_versions.sync_detailed(
			resource_id=resource_id, client=self._client
		)
		if isinstance(result.parsed, list):
			return result.parsed
		return []

	def upload(
		self,
		resource_id: int,
		df: pl.DataFrame | pl.LazyFrame,
		*,
		from_resource_ids: list[int],
	) -> Any:
		"""Upload a DataFrame/LazyFrame to a gold resource.

		Lineage is tracked via *from_resource_ids* (the originating silver
		resources).

		Args:
			resource_id (int): ID of the gold resource to upload to.
			df (pl.DataFrame | pl.LazyFrame): The data to upload. A
				LazyFrame is collected before serialisation.
			from_resource_ids (list[int]): IDs of the originating silver
				resources (recorded for lineage).

		Returns:
			dict[str, Any]: JSON response from the server.
		"""
		if isinstance(df, pl.LazyFrame):
			df = df.collect()
		buf = io.BytesIO()
		df.write_parquet(buf)
		parquet_bytes = buf.getvalue()

		response = self._client.get_httpx_client().post(
			f"/gold/{resource_id}/versions",
			params={"resources": from_resource_ids},
			files={"file": ("data.parquet", parquet_bytes, "application/octet-stream")},
		)
		response.raise_for_status()
		return response.json()

	def download(self, resource_id: int, *, version: int | None = None) -> pl.LazyFrame:
		"""Download a gold version as a LazyFrame.

		Args:
			resource_id (int): ID of the gold resource.
			version (int | None): Specific version to download. If ``None``,
				the latest version is used.

		Returns:
			pl.LazyFrame: The downloaded data.
		"""
		if version is None:
			version = self._latest_version(resource_id)
		response = self._client.get_httpx_client().get(
			f"/gold/{resource_id}/versions/{version}"
		)
		response.raise_for_status()
		return pl.read_ipc_stream(io.BytesIO(response.content)).lazy()

	def _latest_version(self, resource_id: int) -> int:
		versions = self.list_versions(resource_id)
		if not versions:
			raise ValueError(f"No versions found for gold resource {resource_id}")
		return max(v.delta_version for v in versions)


class ProjectsLayer:
	def __init__(self, client: AuthenticatedClient | Client) -> None:
		self._client = client

	def create_project(self, name: str, description: str | None = None) -> int:
		"""Create a new project.

		Args:
			name (str): Human-readable name for the project.
			description (str | None): Optional description of the project.

		Returns:
			int: The ID of the newly created project.
		"""
		result = _project_create.sync_detailed(
			client=self._client,
			body=CreateProjectRequest(
				name=name,
				description=(description if description is not None else None),
			),
		)
		if isinstance(result.parsed, CreateProjectResponse):
			return result.parsed.project_id
		raise ValueError(f"Unexpected response: {result.parsed}")

	def list_projects(self) -> list[ProjectResponse]:
		"""List all projects.

		Returns:
			list[ProjectResponse]: Metadata for every project.
		"""
		result = _project_list.sync_detailed(client=self._client)
		if isinstance(result.parsed, list):
			return result.parsed
		return []

	def get_project(self, project_id: int) -> ProjectResponse:
		"""Get metadata for a single project.

		Args:
			project_id (int): ID of the project.

		Returns:
			ProjectResponse: The project metadata.
		"""
		result = _project_get.sync_detailed(project_id=project_id, client=self._client)
		if isinstance(result.parsed, ProjectResponse):
			return result.parsed
		raise ValueError(f"Unexpected response: {result.parsed}")

	def update_project(
		self, project_id: int, name: str | None = None, description: str | None = None
	) -> int:
		"""Update a project's name and/or description.

		Args:
			project_id (int): ID of the project to update.
			name (str | None): New name, or ``None`` to keep the current one.
			description (str | None): New description, or ``None`` to keep
				the current one.

		Returns:
			int: The ID of the updated project.
		"""
		result = _project_update.sync_detailed(
			project_id=project_id,
			client=self._client,
			body=UpdateProjectRequest(
				name=(name if name is not None else None),
				description=(description if description is not None else None),
			),
		)
		if isinstance(result.parsed, ProjectResponse):
			return result.parsed.id
		raise ValueError(f"Unexpected response: {result.parsed}")

	def delete_project(self, project_id: int) -> None:
		"""Delete a project.

		Args:
			project_id (int): ID of the project to delete.
		"""
		_project_delete.sync_detailed(project_id=project_id, client=self._client)


class DataPebbles:
	"""High-level SDK for the Data Pebbles platform.

	Usage::

			dp = DataPebbles("https://api.example.com", token="...")

			# Bronze: upload raw files
			dp.bronze.create_resource("raw_sales", project_id=1)
			dp.bronze.upload(1, file_path="sales.csv")

			# Silver / Gold: work with LazyFrames
			lf = dp.silver.download(2)
			dp.gold.upload(3, lf, from_resource_ids=[2])


			# Decorators for lineage-tracked pipelines
			@dp.silver_transform(target_id=2, from_bronze_id=1)
			def clean(lf: pl.LazyFrame) -> pl.LazyFrame:
					return lf.filter(pl.col("x") > 0)


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

		# Projects (new backend feature)
		self._projects = ProjectsLayer(self._client)

	@property
	def bronze(self) -> BronzeLayer:
		return self._bronze

	@property
	def silver(self) -> SilverLayer:
		return self._silver

	@property
	def gold(self) -> GoldLayer:
		return self._gold

	@property
	def projects(self) -> ProjectsLayer:
		return self._projects

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
		target_id: int,
		from_bronze_id: int,
		source_id: int | None = None,
		file_type: FileType | str | None = None,
		read_options: dict[str, Any] | None = None,
	) -> Callable[
		[Callable[[pl.LazyFrame], pl.DataFrame | pl.LazyFrame]],
		Callable[..., None],
	]:
		"""Decorator: bronze → silver with automatic lineage tracking.

		The decorator downloads the bronze data, parses it into a
		``LazyFrame``, and passes it to the decorated function.

		By default the file extension is detected from the ``s3_key`` of
		the bronze version.  Pass *file_type* to override (e.g. when the
		key has no extension or the wrong one).

		*read_options* are forwarded as keyword arguments to the
		corresponding Polars reader (``pl.read_csv``, ``pl.read_parquet``,
		etc.).  For example ``{"separator": ";"}`` for semicolon-delimited
		CSVs, or ``{"schema": {"id": pl.Int32}}`` for Parquet.

		The result is serialised to Parquet and uploaded to the
		*target_id* silver resource, recording *from_bronze_id* as the
		originating bronze resource.

		Args:
			target_id (int): ID of the silver resource to upload the result to.
			from_bronze_id (int): ID of the bronze resource to download input
				from. Also recorded as the lineage origin.
			source_id (int | None): Optional ID to record as the lineage
				origin instead of *from_bronze_id*.
			file_type (FileType | str | None): Override the file extension
				used to choose the reader (a :class:`FileType` value or a
				string like ``".csv"``).  ``None`` means auto-detect from the
				s3 key.
			read_options (dict[str, Any] | None): Extra keyword arguments
				forwarded to the Polars reader function.

		Returns:
			Callable: A decorator that wraps a
				``(pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame`` function.

		The resulting wrapper accepts an optional ``version`` keyword
		argument to pin a specific bronze version (defaults to latest).

		Usage::

				@dp.silver_transform(target_id=2, from_bronze_id=1)
				def clean(lf: pl.LazyFrame) -> pl.LazyFrame:
					return lf.filter(pl.col("x") > 0)


				# Override file type and pass reader options
				@dp.silver_transform(
					target_id=3,
					from_bronze_id=2,
					file_type=FileType.CSV,
					read_options={"separator": ";", "has_header": True},
				)
				def clean_eu(lf: pl.LazyFrame) -> pl.LazyFrame:
					return lf.filter(pl.col("x") > 0)
		"""

		def decorator(
			func: Callable[[pl.LazyFrame], pl.DataFrame | pl.LazyFrame],
		) -> Callable[..., None]:
			@wraps(func)
			def wrapper(*, version: int | None = None) -> None:
				if version is None:
					version_ = self.bronze._latest_version(from_bronze_id)
				else:
					version_ = version

				versions = self.bronze.list_versions(from_bronze_id)
				ver = next(v for v in versions if v.version == version_)

				ext = (
					str(file_type)
					if file_type is not None
					else Path(ver.s3_key).suffix.lower()
				)

				bronze_data = self.bronze.download(from_bronze_id, version=version_)
				lf = _read_bronze_bytes(bronze_data, ext, read_options=read_options)
				result = func(lf)
				# Allow explicit override of the recorded source id (lineage)
				record_from_id = source_id if source_id is not None else from_bronze_id
				self.silver.upload(target_id, result, from_resource_id=record_from_id)

			return wrapper

		return decorator

	def gold_transform(
		self,
		*,
		target_id: int,
		from_silver_ids: list[int],
		source_ids: list[int] | None = None,
	) -> Callable[
		[Callable[[dict[int, pl.LazyFrame]], pl.DataFrame | pl.LazyFrame]],
		Callable[..., None],
	]:
		"""Decorator: silver → gold with automatic lineage tracking.

		The decorated function receives a ``dict`` mapping each silver
		resource ID to its ``LazyFrame``.  The result is serialised to
		Parquet and uploaded to the *target_id* gold resource, recording all
		*from_silver_ids* as the originating silver resources.

		Args:
			target_id (int): ID of the gold resource to upload the result to.
			from_silver_ids (list[int]): List of silver resource IDs to
				download as inputs. Also recorded as the lineage origins.
			source_ids (list[int] | None): Optional list of IDs to record
				as the lineage origins instead of *from_silver_ids*.

		Returns:
			Callable: A decorator that wraps a
				``(dict[int, pl.LazyFrame]) -> pl.DataFrame | pl.LazyFrame``
				function.

		The decorated function receives
		``dict[int, pl.LazyFrame]`` keyed by silver resource ID
		and returns a ``DataFrame`` or ``LazyFrame``.

		Usage::

				@dp.gold_transform(target_id=3, from_silver_ids=[1, 2])
				def aggregate(
					sources: dict[int, pl.LazyFrame],
				) -> pl.LazyFrame:
					return (
						pl.concat(sources.values()).group_by("category")
						.agg(pl.sum("amount"))
					)


				aggregate()
		"""

		def decorator(
			func: Callable[[dict[int, pl.LazyFrame]], pl.DataFrame | pl.LazyFrame],
		) -> Callable[..., None]:
			@wraps(func)
			def wrapper() -> None:
				silver_data = {
					sid: self.silver.download(sid) for sid in from_silver_ids
				}
				result = func(silver_data)
				# Allow explicit override of the recorded source ids (lineage)
				record_from_ids = (
					source_ids if source_ids is not None else from_silver_ids
				)
				self.gold.upload(target_id, result, from_resource_ids=record_from_ids)

			return wrapper

		return decorator


class Ingestor(ABC):
	"""Base class for data ingestors.

	An ingestor reads data from an external source — regardless of that source's
	structure — and returns it as a Polars LazyFrame or DataFrame.
	"""

	@abstractmethod
	def ingest(self) -> pl.DataFrame:
		"""Ingest data and return a Polars DataFrame."""
		...

	@abstractmethod
	def ingest_lazy(self) -> pl.LazyFrame:
		"""Ingest data and return a Polars LazyFrame."""
		...
