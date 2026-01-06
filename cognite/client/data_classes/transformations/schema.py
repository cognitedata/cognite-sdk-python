from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, cast

from typing_extensions import Self

from cognite.client.data_classes._base import CogniteObject, CogniteResource, CogniteResourceList
from cognite.client.utils._text import convert_all_keys_recursive

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class TransformationSchemaType(CogniteObject):
    def __init__(self, type: str) -> None:
        self.type = type

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        return cls(type=resource["type"])


class TransformationSchemaArrayType(TransformationSchemaType):
    def __init__(self, type: str, element_type: str | None, contains_null: bool) -> None:
        super().__init__(type=type)
        self.element_type = element_type
        self.contains_null = contains_null

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        return cls(
            type=resource["type"],
            element_type=resource.get("elementType"),
            contains_null=resource["containsNull"],
        )


class TransformationSchemaStructType(TransformationSchemaType):
    # TODO: Fields should probably be translated into an object
    def __init__(self, type: str, fields: list[dict[str, Any]] | None) -> None:
        super().__init__(type=type)
        self.fields = fields

    def dump(self, camel_case: bool = True) -> dict:
        dumped = super().dump(camel_case=camel_case)
        return convert_all_keys_recursive(dumped, camel_case=camel_case)  # <-- 'fields' is a nested object

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        return cls(type=resource["type"], fields=resource.get("fields"))


class TransformationSchemaMapType(TransformationSchemaType):
    def __init__(
        self,
        type: str,
        key_type: str | None = None,
        value_type: str | None = None,
        value_contains_null: bool = False,
    ) -> None:
        super().__init__(type=type)
        self.key_type = key_type
        self.value_type = value_type
        self.value_contains_null = value_contains_null

    @classmethod
    def _load(
        cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None
    ) -> TransformationSchemaMapType:
        return cls(
            type=resource["type"],
            key_type=resource.get("keyType"),
            value_type=resource.get("valueType"),
            value_contains_null=resource.get("valueContainsNull", False),
        )


class TransformationSchemaUnknownType(TransformationSchemaType):
    def __init__(self, raw_schema: dict[str, Any]) -> None:
        raw_schema = raw_schema.copy()
        super().__init__(type=raw_schema.pop("type"))  # type is required
        self.__raw_schema = raw_schema

    def dump(self, camel_case: Literal[True] = True) -> dict[str, Any]:  # type: ignore [override]
        return {"type": self.type, **self.__raw_schema}

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: AsyncCogniteClient | None = None) -> Self:
        return cls(raw_schema=resource)


class TransformationSchemaColumn(CogniteResource):
    """Represents a column of the expected sql structure for a destination type.

    Args:
        name (str): Column name
        sql_type (str): Type of the column in sql format.
        type (TransformationSchemaType): Type of the column in json format.
        nullable (bool): Values for the column can be null or not
        cognite_client (AsyncCogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        name: str,
        sql_type: str,
        type: TransformationSchemaType,
        nullable: bool,
        cognite_client: AsyncCogniteClient | None = None,
    ) -> None:
        self.name = name
        self.sql_type = sql_type
        self.type = type
        self.nullable = nullable
        self._cognite_client = cast("AsyncCogniteClient", cognite_client)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if isinstance(self.type, TransformationSchemaType):
            output["type"] = self.type.dump(camel_case=camel_case)
        return output

    @classmethod
    def _load(cls, resource: dict, cognite_client: AsyncCogniteClient | None = None) -> TransformationSchemaColumn:
        resource_type = resource["type"]
        match resource_type:
            case dict():
                type_classes: dict[str, type[TransformationSchemaType]] = {
                    "array": TransformationSchemaArrayType,
                    "map": TransformationSchemaMapType,
                    "struct": TransformationSchemaStructType,
                }
                type_ = type_classes.get(resource_type["type"], TransformationSchemaUnknownType)._load(resource_type)
            case str():
                # Basic types like 'integer', 'long' or 'string'
                type_ = TransformationSchemaType._load(resource)
            case _:
                raise ValueError(f"Unknown type for TransformationSchemaColumn: {resource_type}")

        return cls(
            name=resource["name"],
            sql_type=resource["sqlType"],
            type=type_,
            nullable=resource["nullable"],
            cognite_client=cognite_client,
        )


class TransformationSchemaColumnList(CogniteResourceList[TransformationSchemaColumn]):
    _RESOURCE = TransformationSchemaColumn
