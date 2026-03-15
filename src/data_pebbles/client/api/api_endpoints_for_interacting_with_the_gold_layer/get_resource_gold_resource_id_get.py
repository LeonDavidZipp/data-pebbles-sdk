from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.gold_metadata_response import GoldMetadataResponse
from ...models.http_validation_error import HTTPValidationError
from ...types import Response


def _get_kwargs(
	resource_id: int,
) -> dict[str, Any]:

	_kwargs: dict[str, Any] = {
		"method": "get",
		"url": "/gold/{resource_id}".format(
			resource_id=quote(str(resource_id), safe=""),
		),
	}

	return _kwargs


def _parse_response(
	*, client: AuthenticatedClient | Client, response: httpx.Response
) -> GoldMetadataResponse | HTTPValidationError | None:
	if response.status_code == 200:
		response_200 = GoldMetadataResponse.from_dict(response.json())

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
) -> Response[GoldMetadataResponse | HTTPValidationError]:
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
) -> Response[GoldMetadataResponse | HTTPValidationError]:
	"""Get Resource

	Args:
	    resource_id (int):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Response[GoldMetadataResponse | HTTPValidationError]
	"""

	kwargs = _get_kwargs(
		resource_id=resource_id,
	)

	response = client.get_httpx_client().request(
		**kwargs,
	)

	return _build_response(client=client, response=response)


def sync(
	resource_id: int,
	*,
	client: AuthenticatedClient | Client,
) -> GoldMetadataResponse | HTTPValidationError | None:
	"""Get Resource

	Args:
	    resource_id (int):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    GoldMetadataResponse | HTTPValidationError
	"""

	return sync_detailed(
		resource_id=resource_id,
		client=client,
	).parsed


async def asyncio_detailed(
	resource_id: int,
	*,
	client: AuthenticatedClient | Client,
) -> Response[GoldMetadataResponse | HTTPValidationError]:
	"""Get Resource

	Args:
	    resource_id (int):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Response[GoldMetadataResponse | HTTPValidationError]
	"""

	kwargs = _get_kwargs(
		resource_id=resource_id,
	)

	response = await client.get_async_httpx_client().request(**kwargs)

	return _build_response(client=client, response=response)


async def asyncio(
	resource_id: int,
	*,
	client: AuthenticatedClient | Client,
) -> GoldMetadataResponse | HTTPValidationError | None:
	"""Get Resource

	Args:
	    resource_id (int):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    GoldMetadataResponse | HTTPValidationError
	"""

	return (
		await asyncio_detailed(
			resource_id=resource_id,
			client=client,
		)
	).parsed
