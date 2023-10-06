from __future__ import annotations

from typing import TYPE_CHECKING, cast

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
from cognite.client.utils._text import convert_all_keys_to_snake_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class TransformationSchemaType:
    def __init__(self, type: str | None = None) -> None:
        self.type = type


class TransformationSchemaArrayType(TransformationSchemaType):
    def __init__(self, type: str | None = None, element_type: str | None = None, contains_null: bool = False) -> None:
        super().__init__(type=type)
        self.element_type = element_type
        self.contains_null = contains_null


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

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> TransformationSchemaColumn:
        instance = super()._load(resource, cognite_client)
        if isinstance(instance.type, dict):
            snake_dict = convert_all_keys_to_snake_case(instance.type)
            instance_type = instance.type.get("type")
            if instance_type == "array":
                instance.type = TransformationSchemaArrayType(**snake_dict)
            elif instance_type == "map":
                instance.type = TransformationSchemaMapType(**snake_dict)
        elif isinstance(instance.type, str):
            instance.type = TransformationSchemaType(type=instance.type)
        return instance


class TransformationSchemaColumnList(CogniteResourceList[TransformationSchemaColumn]):
    _RESOURCE = TransformationSchemaColumn
