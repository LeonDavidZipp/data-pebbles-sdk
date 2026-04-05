from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.body_gold_upload_version import BodyGoldUploadVersion
from ...models.http_validation_error import HTTPValidationError
from ...models.message_response import MessageResponse
from ...types import UNSET, Response


def _get_kwargs(
	resource_id: int,
	*,
	body: BodyGoldUploadVersion,
	resources: list[int],
) -> dict[str, Any]:
	headers: dict[str, Any] = {}

	params: dict[str, Any] = {}

	json_resources = resources

	params["resources"] = json_resources

	params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

	_kwargs: dict[str, Any] = {
		"method": "post",
		"url": "/gold/{resource_id}/versions".format(
			resource_id=quote(str(resource_id), safe=""),
		),
		"params": params,
	}

	_kwargs["files"] = body.to_multipart()

	_kwargs["headers"] = headers
	return _kwargs


def _parse_response(
	*, client: AuthenticatedClient | Client, response: httpx.Response
) -> HTTPValidationError | MessageResponse | None:
	if response.status_code == 201:
		response_201 = MessageResponse.from_dict(response.json())

		return response_201

	if response.status_code == 422:
		response_422 = HTTPValidationError.from_dict(response.json())

		return response_422

	if client.raise_on_unexpected_status:
		raise errors.UnexpectedStatus(response.status_code, response.content)
	else:
		return None


def _build_response(
	*, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[HTTPValidationError | MessageResponse]:
	return Response(
		status_code=HTTPStatus(response.status_code),
		content=response.content,
		headers=response.headers,
		parsed=_parse_response(client=client, response=response),
	)


def sync_detailed(
	resource_id: int,
	*,
	client: AuthenticatedClient | Client,
	body: BodyGoldUploadVersion,
	resources: list[int],
) -> Response[HTTPValidationError | MessageResponse]:
	"""Upload Version Multi

	 Uploads a new version of the resource from a Parquet file, derived from
	multiple existing resources.

	Args:
	        resource_id: The ID of the resource to upload a new version for.
	        file: The uploaded Parquet file containing the new version of the data.
	        loader: The loader instance to use for uploading the data.
	        resources: The IDs of the existing resources that this new version is
	                derived from.

	Returns:
	        A MessageResponse indicating the result of the upload operation.

	Args:
	    resource_id (int):
	    resources (list[int]):
	    body (BodyGoldUploadVersion):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Response[HTTPValidationError | MessageResponse]
	"""

	kwargs = _get_kwargs(
		resource_id=resource_id,
		body=body,
		resources=resources,
	)

	response = client.get_httpx_client().request(
		**kwargs,
	)

	return _build_response(client=client, response=response)


def sync(
	resource_id: int,
	*,
	client: AuthenticatedClient | Client,
	body: BodyGoldUploadVersion,
	resources: list[int],
) -> HTTPValidationError | MessageResponse | None:
	"""Upload Version Multi

	 Uploads a new version of the resource from a Parquet file, derived from
	multiple existing resources.

	Args:
	        resource_id: The ID of the resource to upload a new version for.
	        file: The uploaded Parquet file containing the new version of the data.
	        loader: The loader instance to use for uploading the data.
	        resources: The IDs of the existing resources that this new version is
	                derived from.

	Returns:
	        A MessageResponse indicating the result of the upload operation.

	Args:
	    resource_id (int):
	    resources (list[int]):
	    body (BodyGoldUploadVersion):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    HTTPValidationError | MessageResponse
	"""

	return sync_detailed(
		resource_id=resource_id,
		client=client,
		body=body,
		resources=resources,
	).parsed


async def asyncio_detailed(
	resource_id: int,
	*,
	client: AuthenticatedClient | Client,
	body: BodyGoldUploadVersion,
	resources: list[int],
) -> Response[HTTPValidationError | MessageResponse]:
	"""Upload Version Multi

	 Uploads a new version of the resource from a Parquet file, derived from
	multiple existing resources.

	Args:
	        resource_id: The ID of the resource to upload a new version for.
	        file: The uploaded Parquet file containing the new version of the data.
	        loader: The loader instance to use for uploading the data.
	        resources: The IDs of the existing resources that this new version is
	                derived from.

	Returns:
	        A MessageResponse indicating the result of the upload operation.

	Args:
	    resource_id (int):
	    resources (list[int]):
	    body (BodyGoldUploadVersion):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Response[HTTPValidationError | MessageResponse]
	"""

	kwargs = _get_kwargs(
		resource_id=resource_id,
		body=body,
		resources=resources,
	)

	response = await client.get_async_httpx_client().request(**kwargs)

	return _build_response(client=client, response=response)


async def asyncio(
	resource_id: int,
	*,
	client: AuthenticatedClient | Client,
	body: BodyGoldUploadVersion,
	resources: list[int],
) -> HTTPValidationError | MessageResponse | None:
	"""Upload Version Multi

	 Uploads a new version of the resource from a Parquet file, derived from
	multiple existing resources.

	Args:
	        resource_id: The ID of the resource to upload a new version for.
	        file: The uploaded Parquet file containing the new version of the data.
	        loader: The loader instance to use for uploading the data.
	        resources: The IDs of the existing resources that this new version is
	                derived from.

	Returns:
	        A MessageResponse indicating the result of the upload operation.

	Args:
	    resource_id (int):
	    resources (list[int]):
	    body (BodyGoldUploadVersion):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    HTTPValidationError | MessageResponse
	"""

	return (
		await asyncio_detailed(
			resource_id=resource_id,
			client=client,
			body=body,
			resources=resources,
		)
	).parsed
