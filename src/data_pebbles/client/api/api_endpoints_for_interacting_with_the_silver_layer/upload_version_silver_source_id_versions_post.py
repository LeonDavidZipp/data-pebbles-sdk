from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.body_upload_version_silver_source_id_versions_post import (
	BodyUploadVersionSilverSourceIdVersionsPost,
)
from ...models.http_validation_error import HTTPValidationError
from ...types import UNSET, Response


def _get_kwargs(
	source_id: int,
	*,
	body: BodyUploadVersionSilverSourceIdVersionsPost,
	from_source_id: int,
) -> dict[str, Any]:
	headers: dict[str, Any] = {}

	params: dict[str, Any] = {}

	params["from_source_id"] = from_source_id

	params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

	_kwargs: dict[str, Any] = {
		"method": "post",
		"url": "/silver/{source_id}/versions".format(
			source_id=quote(str(source_id), safe=""),
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
	source_id: int,
	*,
	client: AuthenticatedClient | Client,
	body: BodyUploadVersionSilverSourceIdVersionsPost,
	from_source_id: int,
) -> Response[Any | HTTPValidationError]:
	"""Upload Version

	Args:
	    source_id (int):
	    from_source_id (int):
	    body (BodyUploadVersionSilverSourceIdVersionsPost):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Response[Any | HTTPValidationError]
	"""

	kwargs = _get_kwargs(
		source_id=source_id,
		body=body,
		from_source_id=from_source_id,
	)

	response = client.get_httpx_client().request(
		**kwargs,
	)

	return _build_response(client=client, response=response)


def sync(
	source_id: int,
	*,
	client: AuthenticatedClient | Client,
	body: BodyUploadVersionSilverSourceIdVersionsPost,
	from_source_id: int,
) -> Any | HTTPValidationError | None:
	"""Upload Version

	Args:
	    source_id (int):
	    from_source_id (int):
	    body (BodyUploadVersionSilverSourceIdVersionsPost):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Any | HTTPValidationError
	"""

	return sync_detailed(
		source_id=source_id,
		client=client,
		body=body,
		from_source_id=from_source_id,
	).parsed


async def asyncio_detailed(
	source_id: int,
	*,
	client: AuthenticatedClient | Client,
	body: BodyUploadVersionSilverSourceIdVersionsPost,
	from_source_id: int,
) -> Response[Any | HTTPValidationError]:
	"""Upload Version

	Args:
	    source_id (int):
	    from_source_id (int):
	    body (BodyUploadVersionSilverSourceIdVersionsPost):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Response[Any | HTTPValidationError]
	"""

	kwargs = _get_kwargs(
		source_id=source_id,
		body=body,
		from_source_id=from_source_id,
	)

	response = await client.get_async_httpx_client().request(**kwargs)

	return _build_response(client=client, response=response)


async def asyncio(
	source_id: int,
	*,
	client: AuthenticatedClient | Client,
	body: BodyUploadVersionSilverSourceIdVersionsPost,
	from_source_id: int,
) -> Any | HTTPValidationError | None:
	"""Upload Version

	Args:
	    source_id (int):
	    from_source_id (int):
	    body (BodyUploadVersionSilverSourceIdVersionsPost):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Any | HTTPValidationError
	"""

	return (
		await asyncio_detailed(
			source_id=source_id,
			client=client,
			body=body,
			from_source_id=from_source_id,
		)
	).parsed
