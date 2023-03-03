from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
from cognite.client.utils._auxiliary import convert_all_keys_to_snake_case

if TYPE_CHECKING:
    pass


class TransformationSchemaType:
    def __init__(self, type=None):
        self.type = type


class TransformationSchemaArrayType(TransformationSchemaType):
    def __init__(self, type=None, element_type=None, contains_null=False):
        super().__init__(type=type)
        self.element_type = element_type
        self.contains_null = contains_null


class TransformationSchemaMapType(TransformationSchemaType):
    def __init__(self, type, key_type=None, value_type=None, value_contains_null=False):
        super().__init__(type=type)
        self.key_type = key_type
        self.value_type = value_type
        self.value_contains_null = value_contains_null


class TransformationSchemaColumn(CogniteResource):
    def __init__(self, name=None, sql_type=None, type=None, nullable=False, cognite_client=None):
        self.name = name
        self.sql_type = sql_type
        self.type = type
        self.nullable = nullable
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource, cognite_client=None):
        instance = super()._load(resource, cognite_client)
        if isinstance(instance.type, Dict):
            snake_dict = convert_all_keys_to_snake_case(instance.type)
            instance_type = instance.type.get("type")
            if instance_type == "array":
                instance.type = TransformationSchemaArrayType(**snake_dict)
            elif instance_type == "map":
                instance.type = TransformationSchemaMapType(**snake_dict)
        elif isinstance(instance.type, str):
            instance.type = TransformationSchemaType(type=instance.type)
        return instance


class TransformationSchemaColumnList(CogniteResourceList):
    _RESOURCE = TransformationSchemaColumn
