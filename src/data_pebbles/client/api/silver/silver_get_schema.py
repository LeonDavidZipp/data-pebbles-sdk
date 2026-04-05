from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...models.schema_response import SchemaResponse
from ...types import Response


def _get_kwargs(
	resource_id: int,
	version: int,
) -> dict[str, Any]:

	_kwargs: dict[str, Any] = {
		"method": "get",
		"url": "/silver/{resource_id}/versions/{version}/schema".format(
			resource_id=quote(str(resource_id), safe=""),
			version=quote(str(version), safe=""),
		),
	}

	return _kwargs


def _parse_response(
	*, client: AuthenticatedClient | Client, response: httpx.Response
) -> HTTPValidationError | SchemaResponse | None:
	if response.status_code == 200:
		response_200 = SchemaResponse.from_dict(response.json())

		return response_200

	if response.status_code == 422:
		response_422 = HTTPValidationError.from_dict(response.json())

		return response_422

	if client.raise_on_unexpected_status:
		raise errors.UnexpectedStatus(response.status_code, response.content)
	else:
		return None


def _build_response(
	*, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[HTTPValidationError | SchemaResponse]:
	return Response(
		status_code=HTTPStatus(response.status_code),
		content=response.content,
		headers=response.headers,
		parsed=_parse_response(client=client, response=response),
	)


def sync_detailed(
	resource_id: int,
	version: int,
	*,
	client: AuthenticatedClient | Client,
) -> Response[HTTPValidationError | SchemaResponse]:
	"""Get Schema

	 Returns the schema of the specified version of the resource.

	Args:
	        resource_id: The ID of the resource.
	        version: The version number of the resource.
	        loader: The loader instance to use for fetching the data.

	Returns:
	        A SchemaResponse containing the data schema and a sample of the data.

	Args:
	    resource_id (int):
	    version (int):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Response[HTTPValidationError | SchemaResponse]
	"""

	kwargs = _get_kwargs(
		resource_id=resource_id,
		version=version,
	)

	response = client.get_httpx_client().request(
		**kwargs,
	)

	return _build_response(client=client, response=response)


def sync(
	resource_id: int,
	version: int,
	*,
	client: AuthenticatedClient | Client,
) -> HTTPValidationError | SchemaResponse | None:
	"""Get Schema

	 Returns the schema of the specified version of the resource.

	Args:
	        resource_id: The ID of the resource.
	        version: The version number of the resource.
	        loader: The loader instance to use for fetching the data.

	Returns:
	        A SchemaResponse containing the data schema and a sample of the data.

	Args:
	    resource_id (int):
	    version (int):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    HTTPValidationError | SchemaResponse
	"""

	return sync_detailed(
		resource_id=resource_id,
		version=version,
		client=client,
	).parsed


async def asyncio_detailed(
	resource_id: int,
	version: int,
	*,
	client: AuthenticatedClient | Client,
) -> Response[HTTPValidationError | SchemaResponse]:
	"""Get Schema

	 Returns the schema of the specified version of the resource.

	Args:
	        resource_id: The ID of the resource.
	        version: The version number of the resource.
	        loader: The loader instance to use for fetching the data.

	Returns:
	        A SchemaResponse containing the data schema and a sample of the data.

	Args:
	    resource_id (int):
	    version (int):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Response[HTTPValidationError | SchemaResponse]
	"""

	kwargs = _get_kwargs(
		resource_id=resource_id,
		version=version,
	)

	response = await client.get_async_httpx_client().request(**kwargs)

	return _build_response(client=client, response=response)


async def asyncio(
	resource_id: int,
	version: int,
	*,
	client: AuthenticatedClient | Client,
) -> HTTPValidationError | SchemaResponse | None:
	"""Get Schema

	 Returns the schema of the specified version of the resource.

	Args:
	        resource_id: The ID of the resource.
	        version: The version number of the resource.
	        loader: The loader instance to use for fetching the data.

	Returns:
	        A SchemaResponse containing the data schema and a sample of the data.

	Args:
	    resource_id (int):
	    version (int):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    HTTPValidationError | SchemaResponse
	"""

	return (
		await asyncio_detailed(
			resource_id=resource_id,
			version=version,
			client=client,
		)
	).parsed
