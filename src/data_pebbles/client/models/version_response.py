from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="VersionResponse")


@_attrs_define
class VersionResponse:
	"""
	Attributes:
	    id (int):
	    source_id (int):
	    version (int):
	    status (str):
	    s3_key (str):
	    created_at (str):
	    updated_at (str):
	"""

	id: int
	source_id: int
	version: int
	status: str
	s3_key: str
	created_at: str
	updated_at: str
	additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

	def to_dict(self) -> dict[str, Any]:
		id = self.id

		source_id = self.source_id

		version = self.version

		status = self.status

		s3_key = self.s3_key

		created_at = self.created_at

		updated_at = self.updated_at

		field_dict: dict[str, Any] = {}
		field_dict.update(self.additional_properties)
		field_dict.update(
			{
				"id": id,
				"source_id": source_id,
				"version": version,
				"status": status,
				"s3_key": s3_key,
				"created_at": created_at,
				"updated_at": updated_at,
			}
		)

		return field_dict

	@classmethod
	def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
		d = dict(src_dict)
		id = d.pop("id")

		source_id = d.pop("source_id")

		version = d.pop("version")

		status = d.pop("status")

		s3_key = d.pop("s3_key")

		created_at = d.pop("created_at")

		updated_at = d.pop("updated_at")

		version_response = cls(
			id=id,
			source_id=source_id,
			version=version,
			status=status,
			s3_key=s3_key,
			created_at=created_at,
			updated_at=updated_at,
		)

		version_response.additional_properties = d
		return version_response

	@property
	def additional_keys(self) -> list[str]:
		return list(self.additional_properties.keys())

	def __getitem__(self, key: str) -> Any:
		return self.additional_properties[key]

	def __setitem__(self, key: str, value: Any) -> None:
		self.additional_properties[key] = value

	def __delitem__(self, key: str) -> None:
		del self.additional_properties[key]

	def __contains__(self, key: str) -> bool:
		return key in self.additional_properties
