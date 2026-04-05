from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...models.message_response import MessageResponse
from ...types import Response


def _get_kwargs(
	resource_id: int,
	version: int,
) -> dict[str, Any]:

	_kwargs: dict[str, Any] = {
		"method": "delete",
		"url": "/raw/{resource_id}/versions/{version}".format(
			resource_id=quote(str(resource_id), safe=""),
			version=quote(str(version), safe=""),
		),
	}

	return _kwargs


def _parse_response(
	*, client: AuthenticatedClient | Client, response: httpx.Response
) -> HTTPValidationError | MessageResponse | None:
	if response.status_code == 200:
		response_200 = MessageResponse.from_dict(response.json())

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
) -> Response[HTTPValidationError | MessageResponse]:
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
) -> Response[HTTPValidationError | MessageResponse]:
	"""Delete Version

	 Delete a specific version of a Raw layer resource, removing it from S3 and
	the database.

	Args:
	        resource_id (int): The id of the Raw resource.
	        version (int): The version number to delete.

	Returns:
	        MessageResponse: Confirmation message. 404 if the version does not exist.

	Args:
	    resource_id (int):
	    version (int):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Response[HTTPValidationError | MessageResponse]
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
) -> HTTPValidationError | MessageResponse | None:
	"""Delete Version

	 Delete a specific version of a Raw layer resource, removing it from S3 and
	the database.

	Args:
	        resource_id (int): The id of the Raw resource.
	        version (int): The version number to delete.

	Returns:
	        MessageResponse: Confirmation message. 404 if the version does not exist.

	Args:
	    resource_id (int):
	    version (int):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    HTTPValidationError | MessageResponse
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
) -> Response[HTTPValidationError | MessageResponse]:
	"""Delete Version

	 Delete a specific version of a Raw layer resource, removing it from S3 and
	the database.

	Args:
	        resource_id (int): The id of the Raw resource.
	        version (int): The version number to delete.

	Returns:
	        MessageResponse: Confirmation message. 404 if the version does not exist.

	Args:
	    resource_id (int):
	    version (int):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Response[HTTPValidationError | MessageResponse]
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
) -> HTTPValidationError | MessageResponse | None:
	"""Delete Version

	 Delete a specific version of a Raw layer resource, removing it from S3 and
	the database.

	Args:
	        resource_id (int): The id of the Raw resource.
	        version (int): The version number to delete.

	Returns:
	        MessageResponse: Confirmation message. 404 if the version does not exist.

	Args:
	    resource_id (int):
	    version (int):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    HTTPValidationError | MessageResponse
	"""

	return (
		await asyncio_detailed(
			resource_id=resource_id,
			version=version,
			client=client,
		)
	).parsed
