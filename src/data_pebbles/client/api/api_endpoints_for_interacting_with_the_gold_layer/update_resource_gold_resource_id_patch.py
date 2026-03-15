from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.gold_metadata_response import GoldMetadataResponse
from ...models.http_validation_error import HTTPValidationError
from ...models.update_gold_resource_request import UpdateGoldResourceRequest
from ...types import Response


def _get_kwargs(
	resource_id: int,
	*,
	body: UpdateGoldResourceRequest,
) -> dict[str, Any]:
	headers: dict[str, Any] = {}

	_kwargs: dict[str, Any] = {
		"method": "patch",
		"url": "/gold/{resource_id}".format(
			resource_id=quote(str(resource_id), safe=""),
		),
	}

	_kwargs["json"] = body.to_dict()

	headers["Content-Type"] = "application/json"

	_kwargs["headers"] = headers
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
	body: UpdateGoldResourceRequest,
) -> Response[GoldMetadataResponse | HTTPValidationError]:
	"""Update Resource

	Args:
	    resource_id (int):
	    body (UpdateGoldResourceRequest):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Response[GoldMetadataResponse | HTTPValidationError]
	"""

	kwargs = _get_kwargs(
		resource_id=resource_id,
		body=body,
	)

	response = client.get_httpx_client().request(
		**kwargs,
	)

	return _build_response(client=client, response=response)


def sync(
	resource_id: int,
	*,
	client: AuthenticatedClient | Client,
	body: UpdateGoldResourceRequest,
) -> GoldMetadataResponse | HTTPValidationError | None:
	"""Update Resource

	Args:
	    resource_id (int):
	    body (UpdateGoldResourceRequest):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    GoldMetadataResponse | HTTPValidationError
	"""

	return sync_detailed(
		resource_id=resource_id,
		client=client,
		body=body,
	).parsed


async def asyncio_detailed(
	resource_id: int,
	*,
	client: AuthenticatedClient | Client,
	body: UpdateGoldResourceRequest,
) -> Response[GoldMetadataResponse | HTTPValidationError]:
	"""Update Resource

	Args:
	    resource_id (int):
	    body (UpdateGoldResourceRequest):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Response[GoldMetadataResponse | HTTPValidationError]
	"""

	kwargs = _get_kwargs(
		resource_id=resource_id,
		body=body,
	)

	response = await client.get_async_httpx_client().request(**kwargs)

	return _build_response(client=client, response=response)


async def asyncio(
	resource_id: int,
	*,
	client: AuthenticatedClient | Client,
	body: UpdateGoldResourceRequest,
) -> GoldMetadataResponse | HTTPValidationError | None:
	"""Update Resource

	Args:
	    resource_id (int):
	    body (UpdateGoldResourceRequest):

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
			body=body,
		)
	).parsed
