from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
	from ..models.schema_response_data import SchemaResponseData
	from ..models.schema_response_data_schema import SchemaResponseDataSchema


T = TypeVar("T", bound="SchemaResponse")


@_attrs_define
class SchemaResponse:
	"""
	Attributes:
	    data_schema (SchemaResponseDataSchema):
	    data (SchemaResponseData):
	"""

	data_schema: SchemaResponseDataSchema
	data: SchemaResponseData
	additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

	def to_dict(self) -> dict[str, Any]:
		data_schema = self.data_schema.to_dict()

		data = self.data.to_dict()

		field_dict: dict[str, Any] = {}
		field_dict.update(self.additional_properties)
		field_dict.update(
			{
				"data_schema": data_schema,
				"data": data,
			}
		)

		return field_dict

	@classmethod
	def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
		from ..models.schema_response_data import SchemaResponseData
		from ..models.schema_response_data_schema import SchemaResponseDataSchema

		d = dict(src_dict)
		data_schema = SchemaResponseDataSchema.from_dict(d.pop("data_schema"))

		data = SchemaResponseData.from_dict(d.pop("data"))

		schema_response = cls(
			data_schema=data_schema,
			data=data,
		)

		schema_response.additional_properties = d
		return schema_response

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
