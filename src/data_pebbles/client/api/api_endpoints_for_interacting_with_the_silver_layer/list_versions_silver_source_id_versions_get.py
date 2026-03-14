from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...models.silver_lineage_response import SilverLineageResponse
from ...types import Response


def _get_kwargs(
	source_id: int,
) -> dict[str, Any]:
	_kwargs: dict[str, Any] = {
		"method": "get",
		"url": "/silver/{source_id}/versions".format(
			source_id=quote(str(source_id), safe=""),
		),
	}

	return _kwargs


def _parse_response(
	*, client: AuthenticatedClient | Client, response: httpx.Response
) -> HTTPValidationError | list[SilverLineageResponse] | None:
	if response.status_code == 200:
		response_200 = []
		_response_200 = response.json()
		for response_200_item_data in _response_200:
			response_200_item = SilverLineageResponse.from_dict(response_200_item_data)

			response_200.append(response_200_item)

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
) -> Response[HTTPValidationError | list[SilverLineageResponse]]:
	return Response(
		status_code=HTTPStatus(response.status_code),
		content=response.content,
		headers=response.headers,
		parsed=_parse_response(client=client, response=response),
	)


def sync_detailed(
	source_id: int,
	*,
	client: AuthenticatedClient | Client,
) -> Response[HTTPValidationError | list[SilverLineageResponse]]:
	"""List Versions

	Args:
	    source_id (int):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Response[HTTPValidationError | list[SilverLineageResponse]]
	"""

	kwargs = _get_kwargs(
		source_id=source_id,
	)

	response = client.get_httpx_client().request(
		**kwargs,
	)

	return _build_response(client=client, response=response)


def sync(
	source_id: int,
	*,
	client: AuthenticatedClient | Client,
) -> HTTPValidationError | list[SilverLineageResponse] | None:
	"""List Versions

	Args:
	    source_id (int):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    HTTPValidationError | list[SilverLineageResponse]
	"""

	return sync_detailed(
		source_id=source_id,
		client=client,
	).parsed


async def asyncio_detailed(
	source_id: int,
	*,
	client: AuthenticatedClient | Client,
) -> Response[HTTPValidationError | list[SilverLineageResponse]]:
	"""List Versions

	Args:
	    source_id (int):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Response[HTTPValidationError | list[SilverLineageResponse]]
	"""

	kwargs = _get_kwargs(
		source_id=source_id,
	)

	response = await client.get_async_httpx_client().request(**kwargs)

	return _build_response(client=client, response=response)


async def asyncio(
	source_id: int,
	*,
	client: AuthenticatedClient | Client,
) -> HTTPValidationError | list[SilverLineageResponse] | None:
	"""List Versions

	Args:
	    source_id (int):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    HTTPValidationError | list[SilverLineageResponse]
	"""

	return (
		await asyncio_detailed(
			source_id=source_id,
			client=client,
		)
	).parsed
