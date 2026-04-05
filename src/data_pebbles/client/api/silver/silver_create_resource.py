from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.create_resource_request import CreateResourceRequest
from ...models.create_resource_response import CreateResourceResponse
from ...models.http_validation_error import HTTPValidationError
from ...types import Response


def _get_kwargs(
	*,
	body: CreateResourceRequest,
) -> dict[str, Any]:
	headers: dict[str, Any] = {}

	_kwargs: dict[str, Any] = {
		"method": "post",
		"url": "/silver/",
	}

	_kwargs["json"] = body.to_dict()

	headers["Content-Type"] = "application/json"

	_kwargs["headers"] = headers
	return _kwargs


def _parse_response(
	*, client: AuthenticatedClient | Client, response: httpx.Response
) -> CreateResourceResponse | HTTPValidationError | None:
	if response.status_code == 201:
		response_201 = CreateResourceResponse.from_dict(response.json())

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
) -> Response[CreateResourceResponse | HTTPValidationError]:
	return Response(
		status_code=HTTPStatus(response.status_code),
		content=response.content,
		headers=response.headers,
		parsed=_parse_response(client=client, response=response),
	)


def sync_detailed(
	*,
	client: AuthenticatedClient | Client,
	body: CreateResourceRequest,
) -> Response[CreateResourceResponse | HTTPValidationError]:
	"""Create Resource

	 Creates a new resource in the layer.

	Args:
	        body: A CreateResourceRequest object containing the name, project ID, and
	                optional description for the new resource.
	        loader: The loader instance to use for creating the resource.

	Returns:
	        A CreateResourceResponse containing a success message and the ID of the
	        newly created resource.

	Args:
	    body (CreateResourceRequest):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Response[CreateResourceResponse | HTTPValidationError]
	"""

	kwargs = _get_kwargs(
		body=body,
	)

	response = client.get_httpx_client().request(
		**kwargs,
	)

	return _build_response(client=client, response=response)


def sync(
	*,
	client: AuthenticatedClient | Client,
	body: CreateResourceRequest,
) -> CreateResourceResponse | HTTPValidationError | None:
	"""Create Resource

	 Creates a new resource in the layer.

	Args:
	        body: A CreateResourceRequest object containing the name, project ID, and
	                optional description for the new resource.
	        loader: The loader instance to use for creating the resource.

	Returns:
	        A CreateResourceResponse containing a success message and the ID of the
	        newly created resource.

	Args:
	    body (CreateResourceRequest):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    CreateResourceResponse | HTTPValidationError
	"""

	return sync_detailed(
		client=client,
		body=body,
	).parsed


async def asyncio_detailed(
	*,
	client: AuthenticatedClient | Client,
	body: CreateResourceRequest,
) -> Response[CreateResourceResponse | HTTPValidationError]:
	"""Create Resource

	 Creates a new resource in the layer.

	Args:
	        body: A CreateResourceRequest object containing the name, project ID, and
	                optional description for the new resource.
	        loader: The loader instance to use for creating the resource.

	Returns:
	        A CreateResourceResponse containing a success message and the ID of the
	        newly created resource.

	Args:
	    body (CreateResourceRequest):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Response[CreateResourceResponse | HTTPValidationError]
	"""

	kwargs = _get_kwargs(
		body=body,
	)

	response = await client.get_async_httpx_client().request(**kwargs)

	return _build_response(client=client, response=response)


async def asyncio(
	*,
	client: AuthenticatedClient | Client,
	body: CreateResourceRequest,
) -> CreateResourceResponse | HTTPValidationError | None:
	"""Create Resource

	 Creates a new resource in the layer.

	Args:
	        body: A CreateResourceRequest object containing the name, project ID, and
	                optional description for the new resource.
	        loader: The loader instance to use for creating the resource.

	Returns:
	        A CreateResourceResponse containing a success message and the ID of the
	        newly created resource.

	Args:
	    body (CreateResourceRequest):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    CreateResourceResponse | HTTPValidationError
	"""

	return (
		await asyncio_detailed(
			client=client,
			body=body,
		)
	).parsed
