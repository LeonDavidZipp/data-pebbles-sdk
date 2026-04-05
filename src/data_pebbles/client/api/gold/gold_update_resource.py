from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...models.metadata_response import MetadataResponse
from ...models.update_resource_request import UpdateResourceRequest
from ...types import Response


def _get_kwargs(
	resource_id: int,
	*,
	body: UpdateResourceRequest,
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
) -> HTTPValidationError | MetadataResponse | None:
	if response.status_code == 200:
		response_200 = MetadataResponse.from_dict(response.json())

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
) -> Response[HTTPValidationError | MetadataResponse]:
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
	body: UpdateResourceRequest,
) -> Response[HTTPValidationError | MetadataResponse]:
	"""Update Resource

	 Updates the metadata of the specified resource.

	Args:
	        resource_id: The ID of the resource to update.
	        body: An UpdateResourceRequest object containing the new name and
	                description for the resource.
	        loader: The loader instance to use for updating the metadata.

	Returns:
	        A MetadataResponse object containing the updated metadata of the resource.

	Args:
	    resource_id (int):
	    body (UpdateResourceRequest):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Response[HTTPValidationError | MetadataResponse]
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
	body: UpdateResourceRequest,
) -> HTTPValidationError | MetadataResponse | None:
	"""Update Resource

	 Updates the metadata of the specified resource.

	Args:
	        resource_id: The ID of the resource to update.
	        body: An UpdateResourceRequest object containing the new name and
	                description for the resource.
	        loader: The loader instance to use for updating the metadata.

	Returns:
	        A MetadataResponse object containing the updated metadata of the resource.

	Args:
	    resource_id (int):
	    body (UpdateResourceRequest):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    HTTPValidationError | MetadataResponse
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
	body: UpdateResourceRequest,
) -> Response[HTTPValidationError | MetadataResponse]:
	"""Update Resource

	 Updates the metadata of the specified resource.

	Args:
	        resource_id: The ID of the resource to update.
	        body: An UpdateResourceRequest object containing the new name and
	                description for the resource.
	        loader: The loader instance to use for updating the metadata.

	Returns:
	        A MetadataResponse object containing the updated metadata of the resource.

	Args:
	    resource_id (int):
	    body (UpdateResourceRequest):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Response[HTTPValidationError | MetadataResponse]
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
	body: UpdateResourceRequest,
) -> HTTPValidationError | MetadataResponse | None:
	"""Update Resource

	 Updates the metadata of the specified resource.

	Args:
	        resource_id: The ID of the resource to update.
	        body: An UpdateResourceRequest object containing the new name and
	                description for the resource.
	        loader: The loader instance to use for updating the metadata.

	Returns:
	        A MetadataResponse object containing the updated metadata of the resource.

	Args:
	    resource_id (int):
	    body (UpdateResourceRequest):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    HTTPValidationError | MetadataResponse
	"""

	return (
		await asyncio_detailed(
			resource_id=resource_id,
			client=client,
			body=body,
		)
	).parsed
