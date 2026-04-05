from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="LineageResponse")


@_attrs_define
class LineageResponse:
	"""
	Attributes:
	    id (int):
	    resource_id (int):
	    delta_version (int):
	    from_resource_id (int):
	    created_at (str):
	"""

	id: int
	resource_id: int
	delta_version: int
	from_resource_id: int
	created_at: str
	additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

	def to_dict(self) -> dict[str, Any]:
		id = self.id

		resource_id = self.resource_id

		delta_version = self.delta_version

		from_resource_id = self.from_resource_id

		created_at = self.created_at

		field_dict: dict[str, Any] = {}
		field_dict.update(self.additional_properties)
		field_dict.update(
			{
				"id": id,
				"resource_id": resource_id,
				"delta_version": delta_version,
				"from_resource_id": from_resource_id,
				"created_at": created_at,
			}
		)

		return field_dict

	@classmethod
	def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
		d = dict(src_dict)
		id = d.pop("id")

		resource_id = d.pop("resource_id")

		delta_version = d.pop("delta_version")

		from_resource_id = d.pop("from_resource_id")

		created_at = d.pop("created_at")

		lineage_response = cls(
			id=id,
			resource_id=resource_id,
			delta_version=delta_version,
			from_resource_id=from_resource_id,
			created_at=created_at,
		)

		lineage_response.additional_properties = d
		return lineage_response

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
