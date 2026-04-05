from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.body_raw_upload_version import BodyRawUploadVersion
from ...models.http_validation_error import HTTPValidationError
from ...models.message_response import MessageResponse
from ...types import Response


def _get_kwargs(
	resource_id: int,
	*,
	body: BodyRawUploadVersion,
) -> dict[str, Any]:
	headers: dict[str, Any] = {}

	_kwargs: dict[str, Any] = {
		"method": "post",
		"url": "/raw/{resource_id}/versions".format(
			resource_id=quote(str(resource_id), safe=""),
		),
	}

	_kwargs["files"] = body.to_multipart()

	_kwargs["headers"] = headers
	return _kwargs


def _parse_response(
	*, client: AuthenticatedClient | Client, response: httpx.Response
) -> HTTPValidationError | MessageResponse | None:
	if response.status_code == 201:
		response_201 = MessageResponse.from_dict(response.json())

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
) -> Response[HTTPValidationError | MessageResponse]:
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
	body: BodyRawUploadVersion,
) -> Response[HTTPValidationError | MessageResponse]:
	"""Upload Version

	 Upload a new raw file as a new version of a Raw layer resource. Stored in S3;
	a version record is created in the database.

	Args:
	        resource_id (int): The id of the Raw resource to upload to.
	        file (UploadFile): The raw file to upload.

	Returns:
	        MessageResponse: Confirmation message.

	Args:
	    resource_id (int):
	    body (BodyRawUploadVersion):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Response[HTTPValidationError | MessageResponse]
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
	body: BodyRawUploadVersion,
) -> HTTPValidationError | MessageResponse | None:
	"""Upload Version

	 Upload a new raw file as a new version of a Raw layer resource. Stored in S3;
	a version record is created in the database.

	Args:
	        resource_id (int): The id of the Raw resource to upload to.
	        file (UploadFile): The raw file to upload.

	Returns:
	        MessageResponse: Confirmation message.

	Args:
	    resource_id (int):
	    body (BodyRawUploadVersion):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    HTTPValidationError | MessageResponse
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
	body: BodyRawUploadVersion,
) -> Response[HTTPValidationError | MessageResponse]:
	"""Upload Version

	 Upload a new raw file as a new version of a Raw layer resource. Stored in S3;
	a version record is created in the database.

	Args:
	        resource_id (int): The id of the Raw resource to upload to.
	        file (UploadFile): The raw file to upload.

	Returns:
	        MessageResponse: Confirmation message.

	Args:
	    resource_id (int):
	    body (BodyRawUploadVersion):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Response[HTTPValidationError | MessageResponse]
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
	body: BodyRawUploadVersion,
) -> HTTPValidationError | MessageResponse | None:
	"""Upload Version

	 Upload a new raw file as a new version of a Raw layer resource. Stored in S3;
	a version record is created in the database.

	Args:
	        resource_id (int): The id of the Raw resource to upload to.
	        file (UploadFile): The raw file to upload.

	Returns:
	        MessageResponse: Confirmation message.

	Args:
	    resource_id (int):
	    body (BodyRawUploadVersion):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    HTTPValidationError | MessageResponse
	"""

	return (
		await asyncio_detailed(
			resource_id=resource_id,
			client=client,
			body=body,
		)
	).parsed
