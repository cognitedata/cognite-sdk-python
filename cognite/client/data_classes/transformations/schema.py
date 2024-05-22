from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, cast

from typing_extensions import Self

from cognite.client.data_classes._base import CogniteObject, CogniteResource, CogniteResourceList
from cognite.client.utils._text import convert_all_keys_recursive

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class TransformationSchemaType(CogniteObject):
    def __init__(self, type: str | None = None) -> None:
        self.type = type


class TransformationSchemaArrayType(TransformationSchemaType):
    def __init__(self, type: str | None = None, element_type: str | None = None, contains_null: bool = False) -> None:
        super().__init__(type=type)
        self.element_type = element_type
        self.contains_null = contains_null


class TransformationSchemaStructType(TransformationSchemaType):
    # TODO: Fields should probably be translated into an object
    def __init__(self, type: str | None = None, fields: list[dict[str, Any]] | None = None) -> None:
        super().__init__(type=type)
        self.fields = fields

    def dump(self, camel_case: bool = True) -> dict:
        dumped = super().dump(camel_case=camel_case)
        return convert_all_keys_recursive(dumped, camel_case=camel_case)  # <-- 'fields' is a nested object


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
        cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None
    ) -> TransformationSchemaMapType:
        return cls(
            type=resource["type"],
            key_type=resource.get("keyType"),
            value_type=resource.get("valueType"),
            value_contains_null=resource.get("valueContainsNull"),  # type: ignore[arg-type]
        )


class TransformationSchemaUnknownType(TransformationSchemaType):
    def __init__(self, raw_schema: dict[str, Any]) -> None:
        raw_schema = raw_schema.copy()
        super().__init__(type=raw_schema.pop("type"))  # type is required
        self.__raw_schema = raw_schema

    def dump(self, camel_case: Literal[True] = True) -> dict[str, Any]:  # type: ignore [override]
        return {"type": self.type, **self.__raw_schema}

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(raw_schema=resource)


class TransformationSchemaColumn(CogniteResource):
    """Represents a column of the expected sql structure for a destination type.

    Args:
        name (str | None): Column name
        sql_type (str | None): Type of the column in sql format.
        type (TransformationSchemaType | None): Type of the column in json format.
        nullable (bool): Values for the column can be null or not
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        name: str | None = None,
        sql_type: str | None = None,
        type: TransformationSchemaType | None = None,
        nullable: bool = False,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.name = name
        self.sql_type = sql_type
        self.type = type
        self.nullable = nullable
        self._cognite_client = cast("CogniteClient", cognite_client)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if isinstance(self.type, TransformationSchemaType):
            output["type"] = self.type.dump(camel_case=camel_case)
        return output

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> TransformationSchemaColumn:
        instance = super()._load(resource, cognite_client)
        if isinstance(instance.type, dict):
            instance.type = {
                "array": TransformationSchemaArrayType,
                "map": TransformationSchemaMapType,
                "struct": TransformationSchemaStructType,
            }.get(instance.type["type"], TransformationSchemaUnknownType)._load(instance.type)

        elif isinstance(instance.type, str):
            instance.type = TransformationSchemaType(type=instance.type)
        return instance


class TransformationSchemaColumnList(CogniteResourceList[TransformationSchemaColumn]):
    _RESOURCE = TransformationSchemaColumn
