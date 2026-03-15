from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.body_upload_version_gold_resource_id_versions_post import (
	BodyUploadVersionGoldResourceIdVersionsPost,
)
from ...models.http_validation_error import HTTPValidationError
from ...types import UNSET, Response


def _get_kwargs(
	resource_id: int,
	*,
	body: BodyUploadVersionGoldResourceIdVersionsPost,
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
) -> Any | HTTPValidationError | None:
	if response.status_code == 200:
		response_200 = response.json()
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
) -> Response[Any | HTTPValidationError]:
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
	body: BodyUploadVersionGoldResourceIdVersionsPost,
	resources: list[int],
) -> Response[Any | HTTPValidationError]:
	"""Upload Version

	Args:
	    resource_id (int):
	    resources (list[int]):
	    body (BodyUploadVersionGoldResourceIdVersionsPost):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Response[Any | HTTPValidationError]
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
	body: BodyUploadVersionGoldResourceIdVersionsPost,
	resources: list[int],
) -> Any | HTTPValidationError | None:
	"""Upload Version

	Args:
	    resource_id (int):
	    resources (list[int]):
	    body (BodyUploadVersionGoldResourceIdVersionsPost):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Any | HTTPValidationError
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
	body: BodyUploadVersionGoldResourceIdVersionsPost,
	resources: list[int],
) -> Response[Any | HTTPValidationError]:
	"""Upload Version

	Args:
	    resource_id (int):
	    resources (list[int]):
	    body (BodyUploadVersionGoldResourceIdVersionsPost):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Response[Any | HTTPValidationError]
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
	body: BodyUploadVersionGoldResourceIdVersionsPost,
	resources: list[int],
) -> Any | HTTPValidationError | None:
	"""Upload Version

	Args:
	    resource_id (int):
	    resources (list[int]):
	    body (BodyUploadVersionGoldResourceIdVersionsPost):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Any | HTTPValidationError
	"""

	return (
		await asyncio_detailed(
			resource_id=resource_id,
			client=client,
			body=body,
			resources=resources,
		)
	).parsed
