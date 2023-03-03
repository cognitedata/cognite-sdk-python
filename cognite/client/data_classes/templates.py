import json
from collections import UserDict

from cognite.client import utils
from cognite.client.data_classes._base import (
    EXCLUDE_VALUE,
    CogniteObjectUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
)


class TemplateGroup(CogniteResource):
    def __init__(
        self,
        external_id=None,
        description=None,
        owners=None,
        data_set_id=None,
        created_time=None,
        last_updated_time=None,
        cognite_client=None,
    ):
        self.external_id = external_id
        self.description = description
        self.owners = owners
        self.data_set_id = data_set_id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self._cognite_client = cognite_client


class TemplateGroupList(CogniteResourceList):
    _RESOURCE = TemplateGroup


class TemplateGroupVersion(CogniteResource):
    def __init__(
        self,
        schema=None,
        version=None,
        conflict_mode=None,
        created_time=None,
        last_updated_time=None,
        cognite_client=None,
    ):
        self.schema = schema
        self.version = version
        self.conflict_mode = conflict_mode
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self._cognite_client = cognite_client


class TemplateGroupVersionList(CogniteResourceList):
    _RESOURCE = TemplateGroupVersion


class ConstantResolver(CogniteResource):
    def __init__(self, value=None, cognite_client=None):
        self.type = "constant"
        self.value = value
        self._cognite_client = cognite_client


class RawResolver(CogniteResource):
    def __init__(self, db_name=None, table_name=None, row_key=None, column_name=None, cognite_client=None):
        self.type = "raw"
        self.db_name = db_name
        self.table_name = table_name
        self.row_key = row_key
        self.column_name = column_name
        self._cognite_client = cognite_client


class SyntheticTimeSeriesResolver(CogniteResource):
    def __init__(
        self,
        expression=None,
        name=None,
        description=None,
        metadata=None,
        is_step=None,
        is_string=None,
        unit=None,
        cognite_client=None,
    ):
        self.type = "syntheticTimeSeries"
        self.expression = expression
        self.name = name
        self.description = description
        self.metadata = metadata
        self.is_step = is_step
        self.is_string = is_string
        self.unit = unit
        self._cognite_client = cognite_client


class ViewResolver(CogniteResource):
    def __init__(self, external_id=None, input=None, cognite_client=None):
        self.type = "view"
        self.external_id = external_id
        self.input = input
        self._cognite_client = cognite_client


FieldResolvers = Union[(ConstantResolver, RawResolver, SyntheticTimeSeriesResolver, str, ViewResolver)]


class TemplateInstance(CogniteResource):
    def __init__(
        self,
        external_id=None,
        template_name=None,
        field_resolvers=None,
        data_set_id=None,
        created_time=None,
        last_updated_time=None,
        cognite_client=None,
    ):
        self.external_id = external_id
        self.template_name = template_name
        self.field_resolvers = field_resolvers
        self.data_set_id = data_set_id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self._cognite_client = cognite_client

    field_resolver_mapper: Dict[(str, Type[CogniteResource])] = {
        "constant": ConstantResolver,
        "syntheticTimeSeries": SyntheticTimeSeriesResolver,
        "raw": RawResolver,
        "view": ViewResolver,
    }

    def dump(self, camel_case=False):
        if camel_case:
            return {
                utils._auxiliary.to_camel_case(key): (
                    value
                    if (key != "field_resolvers")
                    else TemplateInstance._encode_field_resolvers(value, camel_case=camel_case)
                )
                for (key, value) in self.__dict__.items()
                if ((value not in EXCLUDE_VALUE) and (not key.startswith("_")))
            }
        return {
            key: (
                value
                if (key != "field_resolvers")
                else TemplateInstance._encode_field_resolvers(value, camel_case=camel_case)
            )
            for (key, value) in self.__dict__.items()
            if ((value not in EXCLUDE_VALUE) and (not key.startswith("_")))
        }

    @staticmethod
    def _encode_field_resolvers(field_resolvers, camel_case):
        return {
            key: (value.dump(camel_case=camel_case) if (not isinstance(value, str)) else value)
            for (key, value) in field_resolvers.items()
        }

    @classmethod
    def _load(cls, resource, cognite_client=None):
        if isinstance(resource, str):
            return cls._load(json.loads(resource), cognite_client=cognite_client)
        elif isinstance(resource, Dict):
            instance = cls(cognite_client=cognite_client)
            for (key, value) in resource.items():
                snake_case_key = utils._auxiliary.to_snake_case(key)
                if hasattr(instance, snake_case_key):
                    if key == "fieldResolvers":
                        setattr(
                            instance,
                            snake_case_key,
                            {
                                key: TemplateInstance._field_resolver_load(field_resolver)
                                for (key, field_resolver) in value.items()
                            },
                        )
                    else:
                        setattr(instance, snake_case_key, value)
            return instance
        raise TypeError(f"Resource must be json str or dict, not {type(resource)}")

    @staticmethod
    def _field_resolver_load(resource, cognite_client=None):
        return TemplateInstance.field_resolver_mapper[resource["type"]]._load(resource, cognite_client)


class TemplateInstanceUpdate(CogniteUpdate):
    class _ObjectAssetUpdate(CogniteObjectUpdate):
        def set(self, value):
            return self._set(value)

        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

    @property
    def field_resolvers(self):
        return TemplateInstanceUpdate._ObjectAssetUpdate(self, "fieldResolvers")


class Source(CogniteResource):
    def __init__(self, type=None, filter=None, mappings=None, cognite_client=None):
        self.type = type
        self.filter = filter
        self.mappings = mappings
        self._cognite_client = cognite_client


class View(CogniteResource):
    def __init__(
        self,
        external_id=None,
        source=None,
        data_set_id=None,
        created_time=None,
        last_updated_time=None,
        cognite_client=None,
    ):
        self.external_id = external_id
        self.source = source
        self.data_set_id = data_set_id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self._cognite_client = cognite_client

    def dump(self, camel_case=False):
        if camel_case:
            return {
                utils._auxiliary.to_camel_case(key): View.resolve_nested_classes(value, camel_case)
                for (key, value) in self.__dict__.items()
                if ((value not in EXCLUDE_VALUE) and (not key.startswith("_")))
            }
        return {
            key: View.resolve_nested_classes(value, camel_case)
            for (key, value) in self.__dict__.items()
            if ((value not in EXCLUDE_VALUE) and (not key.startswith("_")))
        }

    @staticmethod
    def resolve_nested_classes(value, camel_case):
        if isinstance(value, CogniteResource):
            return value.dump(camel_case)
        else:
            return value

    @classmethod
    def _load(cls, resource, cognite_client=None):
        if isinstance(resource, str):
            return cls._load(json.loads(resource), cognite_client=cognite_client)
        elif isinstance(resource, Dict):
            instance = cls(cognite_client=cognite_client)
            for (key, value) in resource.items():
                snake_case_key = utils._auxiliary.to_snake_case(key)
                if hasattr(instance, snake_case_key):
                    value = value if (key != "source") else Source._load(value, cognite_client)
                    setattr(instance, snake_case_key, value)
            return instance
        raise TypeError(f"Resource must be json str or dict, not {type(resource)}")


class ViewResolveItem(UserDict, CogniteResource):
    def __init__(self, data, cognite_client=None):
        super().__init__(data)
        self._cognite_client = cognite_client

    def dump(self, camel_case=False):
        return self.data

    @classmethod
    def _load(cls, data, cognite_client=None):
        if isinstance(data, str):
            return cls._load(json.loads(data), cognite_client=cognite_client)
        elif isinstance(data, Dict):
            return cls(data, cognite_client=cognite_client)


class GraphQlError(CogniteResource):
    def __init__(self, message=None, path=None, locations=None, cognite_client=None):
        self.message = message
        self.path = path
        self.locations = locations
        self._cognite_client = cognite_client


class GraphQlResponse(CogniteResource):
    def __init__(self, data=None, errors=None, cognite_client=None):
        self.data = data
        self.errors = errors
        self._cognite_client = cognite_client


class TemplateInstanceList(CogniteResourceList):
    _RESOURCE = TemplateInstance


class ViewList(CogniteResourceList):
    _RESOURCE = View


class ViewResolveList(CogniteResourceList):
    _RESOURCE = ViewResolveItem
