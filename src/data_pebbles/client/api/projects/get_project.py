from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...models.project_response import ProjectResponse
from ...types import Response


def _get_kwargs(
	project_id: int,
) -> dict[str, Any]:

	_kwargs: dict[str, Any] = {
		"method": "get",
		"url": "/projects/{project_id}".format(
			project_id=quote(str(project_id), safe=""),
		),
	}

	return _kwargs


def _parse_response(
	*, client: AuthenticatedClient | Client, response: httpx.Response
) -> HTTPValidationError | ProjectResponse | None:
	if response.status_code == 200:
		response_200 = ProjectResponse.from_dict(response.json())

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
) -> Response[HTTPValidationError | ProjectResponse]:
	return Response(
		status_code=HTTPStatus(response.status_code),
		content=response.content,
		headers=response.headers,
		parsed=_parse_response(client=client, response=response),
	)


def sync_detailed(
	project_id: int,
	*,
	client: AuthenticatedClient | Client,
) -> Response[HTTPValidationError | ProjectResponse]:
	"""Get Project

	 Return a single project by its id.

	Args:
	        project_id (int): The id of the project.

	Returns:
	        ProjectResponse: Project id, name, description, and created_at. 404 if
	                not found.

	Args:
	    project_id (int):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Response[HTTPValidationError | ProjectResponse]
	"""

	kwargs = _get_kwargs(
		project_id=project_id,
	)

	response = client.get_httpx_client().request(
		**kwargs,
	)

	return _build_response(client=client, response=response)


def sync(
	project_id: int,
	*,
	client: AuthenticatedClient | Client,
) -> HTTPValidationError | ProjectResponse | None:
	"""Get Project

	 Return a single project by its id.

	Args:
	        project_id (int): The id of the project.

	Returns:
	        ProjectResponse: Project id, name, description, and created_at. 404 if
	                not found.

	Args:
	    project_id (int):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    HTTPValidationError | ProjectResponse
	"""

	return sync_detailed(
		project_id=project_id,
		client=client,
	).parsed


async def asyncio_detailed(
	project_id: int,
	*,
	client: AuthenticatedClient | Client,
) -> Response[HTTPValidationError | ProjectResponse]:
	"""Get Project

	 Return a single project by its id.

	Args:
	        project_id (int): The id of the project.

	Returns:
	        ProjectResponse: Project id, name, description, and created_at. 404 if
	                not found.

	Args:
	    project_id (int):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    Response[HTTPValidationError | ProjectResponse]
	"""

	kwargs = _get_kwargs(
		project_id=project_id,
	)

	response = await client.get_async_httpx_client().request(**kwargs)

	return _build_response(client=client, response=response)


async def asyncio(
	project_id: int,
	*,
	client: AuthenticatedClient | Client,
) -> HTTPValidationError | ProjectResponse | None:
	"""Get Project

	 Return a single project by its id.

	Args:
	        project_id (int): The id of the project.

	Returns:
	        ProjectResponse: Project id, name, description, and created_at. 404 if
	                not found.

	Args:
	    project_id (int):

	Raises:
	    errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
	    httpx.TimeoutException: If the request takes longer than Client.timeout.

	Returns:
	    HTTPValidationError | ProjectResponse
	"""

	return (
		await asyncio_detailed(
			project_id=project_id,
			client=client,
		)
	).parsed
