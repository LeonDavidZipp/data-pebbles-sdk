from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...models.silver_metadata_response import SilverMetadataResponse
from ...models.update_silver_source_request import UpdateSilverSourceRequest
from ...types import Response


def _get_kwargs(
	source_id: int,
	*,
	body: UpdateSilverSourceRequest,
) -> dict[str, Any]:
	headers: dict[str, Any] = {}

	_kwargs: dict[str, Any] = {
		"method": "patch",
		"url": "/silver/{source_id}".format(
			source_id=quote(str(source_id), safe=""),
		),
	}

	_kwargs["json"] = body.to_dict()

	headers["Content-Type"] = "application/json"

	_kwargs["headers"] = headers
	return _kwargs


def _parse_response(
	*, client: AuthenticatedClient | Client, response: httpx.Response
) -> HTTPValidationError | SilverMetadataResponse | None:
	if response.status_code == 200:
		response_200 = SilverMetadataResponse.from_dict(response.json())

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
) -> Response[HTTPValidationError | SilverMetadataResponse]:
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
	body: UpdateSilverSourceRequest,
) -> Response[HTTPValidationError | SilverMetadataResponse]:
	"""Update Source

	Args:
	    source_id (int):
	    body (UpdateSilverSourceRequest):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Response[HTTPValidationError | SilverMetadataResponse]
	"""

	kwargs = _get_kwargs(
		source_id=source_id,
		body=body,
	)

	response = client.get_httpx_client().request(
		**kwargs,
	)

	return _build_response(client=client, response=response)


def sync(
	source_id: int,
	*,
	client: AuthenticatedClient | Client,
	body: UpdateSilverSourceRequest,
) -> HTTPValidationError | SilverMetadataResponse | None:
	"""Update Source

	Args:
	    source_id (int):
	    body (UpdateSilverSourceRequest):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    HTTPValidationError | SilverMetadataResponse
	"""

	return sync_detailed(
		source_id=source_id,
		client=client,
		body=body,
	).parsed


async def asyncio_detailed(
	source_id: int,
	*,
	client: AuthenticatedClient | Client,
	body: UpdateSilverSourceRequest,
) -> Response[HTTPValidationError | SilverMetadataResponse]:
	"""Update Source

	Args:
	    source_id (int):
	    body (UpdateSilverSourceRequest):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Response[HTTPValidationError | SilverMetadataResponse]
	"""

	kwargs = _get_kwargs(
		source_id=source_id,
		body=body,
	)

	response = await client.get_async_httpx_client().request(**kwargs)

	return _build_response(client=client, response=response)


async def asyncio(
	source_id: int,
	*,
	client: AuthenticatedClient | Client,
	body: UpdateSilverSourceRequest,
) -> HTTPValidationError | SilverMetadataResponse | None:
	"""Update Source

	Args:
	    source_id (int):
	    body (UpdateSilverSourceRequest):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    HTTPValidationError | SilverMetadataResponse
	"""

	return (
		await asyncio_detailed(
			source_id=source_id,
			client=client,
			body=body,
		)
	).parsed
