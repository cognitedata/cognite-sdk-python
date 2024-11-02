from __future__ import annotations

from abc import ABC
from collections import UserDict
from typing import TYPE_CHECKING, Any, ClassVar, TypeAlias, cast

from cognite.client.data_classes._base import (
    CogniteObjectUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    PropertySpec,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.utils._text import convert_all_keys_to_camel_case, to_snake_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class TemplateGroupCore(WriteableCogniteResource["TemplateGroupWrite"], ABC):
    """A template group is a high level concept encapsulating a schema and a set of template instances. It also has query capability support.

    Template groups are versioned, so there can be multiple template groups with the same external ID.
    The versioning is happening automatically whenever a template groups is changed.

    GraphQL schema definition language is used as the language to describe the structure of the templates and data types.

    Args:
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        description (str | None): The description of the template groups.
        owners (list[str] | None): The list of owners for the template groups.
        data_set_id (int | None): The dataSet which this Template Group belongs to
    """

    def __init__(
        self,
        external_id: str | None = None,
        description: str | None = None,
        owners: list[str] | None = None,
        data_set_id: int | None = None,
    ) -> None:
        self.external_id = external_id
        self.description = description
        self.owners = owners
        self.data_set_id = data_set_id


class TemplateGroup(TemplateGroupCore):
    """A template group is a high level concept encapsulating a schema and a set of template instances. It also has query capability support.

    Template groups are versioned, so there can be multiple template groups with the same external ID.
    The versioning is happening automatically whenever a template groups is changed.

    GraphQL schema definition language is used as the language to describe the structure of the templates and data types.

    Args:
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        description (str | None): The description of the template groups.
        owners (list[str] | None): The list of owners for the template groups.
        data_set_id (int | None): The dataSet which this Template Group belongs to
        created_time (int | None): No description.
        last_updated_time (int | None): No description.
        cognite_client (CogniteClient | None): No description.
    """

    def __init__(
        self,
        external_id: str | None = None,
        description: str | None = None,
        owners: list[str] | None = None,
        data_set_id: int | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        super().__init__(external_id, description, owners, data_set_id)
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self._cognite_client = cast("CogniteClient", cognite_client)

    def as_write(self) -> TemplateGroupWrite:
        return TemplateGroupWrite(
            external_id=self.external_id,
            description=self.description,
            owners=self.owners,
            data_set_id=self.data_set_id,
        )


class TemplateGroupWrite(TemplateGroupCore):
    """A template group is a high level concept encapsulating a schema and a set of template instances. It also has query capability support.
    This is the write format of a template group.

    Template groups are versioned, so there can be multiple template groups with the same external ID.
    The versioning is happening automatically whenever a template groups is changed.

    GraphQL schema definition language is used as the language to describe the structure of the templates and data types.

    Args:
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        description (str | None): The description of the template groups.
        owners (list[str] | None): The list of owners for the template groups.
        data_set_id (int | None): The dataSet which this Template Group belongs to
    """

    def __init__(
        self,
        external_id: str | None = None,
        description: str | None = None,
        owners: list[str] | None = None,
        data_set_id: int | None = None,
    ) -> None:
        super().__init__(external_id, description, owners, data_set_id)

    def as_write(self) -> TemplateGroupWrite:
        return self


class TemplateGroupWriteList(CogniteResourceList[TemplateGroupWrite]):
    _RESOURCE = TemplateGroupWrite


class TemplateGroupList(WriteableCogniteResourceList[TemplateGroupWrite, TemplateGroup]):
    _RESOURCE = TemplateGroup

    def as_write(self) -> TemplateGroupWriteList:
        return TemplateGroupWriteList([item.as_write() for item in self], cognite_client=self._get_cognite_client())


class TemplateGroupVersion(CogniteResource):
    """
    A Template Group Version supports specifying different conflict modes, which is used when an existing schema already exists.

    Patch -> It diffs the new schema with the old schema and fails if there are breaking changes.
    Update -> It sets the new schema as schema of a new version.
    Force -> It ignores breaking changes and replaces the old schema with the new schema.
    The default mode is "patch".

    Args:
        schema (str | None): The GraphQL schema.
        version (int | None): Incremented by the server whenever the schema of a template groups changes.
        conflict_mode (str | None): Can be set to 'Patch', 'Update' or 'Force'.
        created_time (int | None): No description.
        last_updated_time (int | None): No description.
        cognite_client (CogniteClient | None): No description.
    """

    def __init__(
        self,
        schema: str | None = None,
        version: int | None = None,
        conflict_mode: str | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.schema = schema
        self.version = version
        self.conflict_mode = conflict_mode
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self._cognite_client = cast("CogniteClient", cognite_client)


class TemplateGroupVersionList(CogniteResourceList[TemplateGroupVersion]):
    _RESOURCE = TemplateGroupVersion


class ConstantResolver(CogniteResource):
    """Resolves a field to a constant value. The value can be of any supported JSON type.

    Args:
        value (Any | None): The value of the field.
        cognite_client (CogniteClient | None): No description.
    """

    def __init__(self, value: Any | None = None, cognite_client: CogniteClient | None = None) -> None:
        self.type = "constant"
        self.value = value
        self._cognite_client = cast("CogniteClient", cognite_client)


class RawResolver(CogniteResource):
    """Resolves a field to a RAW column.

    Args:
        db_name (str | None): The database name.
        table_name (str | None): The table name.
        row_key (str | None): The row key.
        column_name (str | None): The column to fetch the value from.
        cognite_client (CogniteClient | None): No description.
    """

    def __init__(
        self,
        db_name: str | None = None,
        table_name: str | None = None,
        row_key: str | None = None,
        column_name: str | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.type = "raw"
        self.db_name = db_name
        self.table_name = table_name
        self.row_key = row_key
        self.column_name = column_name
        self._cognite_client = cast("CogniteClient", cognite_client)


class SyntheticTimeSeriesResolver(CogniteResource):
    """Resolves a field of type 'SyntheticTimeSeries' to a Synthetic Time Series.

    Args:
        expression (str | None): The synthetic time series expression. See this for syntax https://docs.cognite.com/api/v1/#tag/Synthetic-Time-Series.
        name (str | None): The name of the Time Series.
        description (str | None): The description for the Time Series.
        metadata (dict[str, str] | None): Specifies metadata for the Time Series.
        is_step (bool | None): Specifies if the synthetic time series is step based.
        is_string (bool | None): Specifies if the synthetic time series returned contains string values.
        unit (str | None): The unit of the time series.
        cognite_client (CogniteClient | None): No description.
    """

    def __init__(
        self,
        expression: str | None = None,
        name: str | None = None,
        description: str | None = None,
        metadata: dict[str, str] | None = None,
        is_step: bool | None = None,
        is_string: bool | None = None,
        unit: str | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.type = "syntheticTimeSeries"
        self.expression = expression
        self.name = name
        self.description = description
        self.metadata = metadata
        self.is_step = is_step
        self.is_string = is_string
        self.unit = unit
        self._cognite_client = cast("CogniteClient", cognite_client)


class ViewResolver(CogniteResource):
    """Resolves the field by loading the data from a view.

    Args:
        external_id (str | None): The external id of the view.
        input (dict[str, Any] | None): The input used to resolve the view.
        cognite_client (CogniteClient | None): No description.
    """

    def __init__(
        self,
        external_id: str | None = None,
        input: dict[str, Any] | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.type = "view"
        self.external_id = external_id
        self.input = input
        self._cognite_client = cast("CogniteClient", cognite_client)


FieldResolvers: TypeAlias = ConstantResolver | RawResolver | SyntheticTimeSeriesResolver | str | ViewResolver


class TemplateInstanceCore(WriteableCogniteResource["TemplateInstanceWrite"], ABC):
    """A template instance that implements a template by specifying a resolver per field.

    Args:
        external_id (str | None): The id of the template instance.
        template_name (str | None): The template name to implement.
        field_resolvers (dict[str, FieldResolvers] | None): A set of field resolvers where the dictionary key correspond to the field name.
        data_set_id (int | None): The id of the dataset this instance belongs to.
    """

    def __init__(
        self,
        external_id: str | None = None,
        template_name: str | None = None,
        field_resolvers: dict[str, FieldResolvers] | None = None,
        data_set_id: int | None = None,
    ) -> None:
        self.external_id = external_id
        self.template_name = template_name
        self.field_resolvers = field_resolvers
        self.data_set_id = data_set_id

    field_resolver_mapper: ClassVar[dict[str, type[FieldResolvers]]] = {
        "constant": ConstantResolver,
        "syntheticTimeSeries": SyntheticTimeSeriesResolver,
        "raw": RawResolver,
        "view": ViewResolver,
    }

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to True.

        Returns:
            dict[str, Any]: A dictionary representation of the instance.
        """
        dumped = {
            key: value if key != "field_resolvers" else self._encode_field_resolvers(value, camel_case=camel_case)
            for key, value in self.__dict__.items()
            if value is not None and not key.startswith("_")
        }
        if camel_case:
            return convert_all_keys_to_camel_case(dumped)
        return dumped

    @staticmethod
    def _encode_field_resolvers(field_resolvers: dict[str, FieldResolvers], camel_case: bool) -> dict[str, Any]:
        return {
            key: value if isinstance(value, str) else value.dump(camel_case=camel_case)
            for key, value in field_resolvers.items()
        }

    @classmethod
    def _field_resolver_load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> FieldResolvers:
        resolver = cls.field_resolver_mapper[resource["type"]]
        if isinstance(resolver, str):
            return resolver
        elif issubclass(resolver, (ConstantResolver, RawResolver, SyntheticTimeSeriesResolver, ViewResolver)):
            return resolver._load(resource, cognite_client)
        else:
            raise ValueError(f"Unknown field resolver type {resolver}")


class TemplateInstance(TemplateInstanceCore):
    """A template instance that implements a template by specifying a resolver per field.

    Args:
        external_id (str | None): The id of the template instance.
        template_name (str | None): The template name to implement.
        field_resolvers (dict[str, FieldResolvers] | None): A set of field resolvers where the dictionary key correspond to the field name.
        data_set_id (int | None): The id of the dataset this instance belongs to.
        created_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        cognite_client (CogniteClient | None): No description.
    """

    def __init__(
        self,
        external_id: str | None = None,
        template_name: str | None = None,
        field_resolvers: dict[str, FieldResolvers] | None = None,
        data_set_id: int | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        super().__init__(external_id, template_name, field_resolvers, data_set_id)
        # created_time/last_updated_time are required when using the class to read,
        # but don't make sense passing in when creating a new object. So in order to make the typing
        # correct here (i.e. int and not Optional[int]), we force the type to be int rather than
        # Optional[int].
        # TODO: In the next major version we can make these properties required in the constructor
        self.created_time: int = created_time  # type: ignore
        self.last_updated_time: int = last_updated_time  # type: ignore
        self._cognite_client = cast("CogniteClient", cognite_client)

    def as_write(self) -> TemplateInstanceWrite:
        return TemplateInstanceWrite(
            external_id=self.external_id,
            template_name=self.template_name,
            field_resolvers=self.field_resolvers,
            data_set_id=self.data_set_id,
        )

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> TemplateInstance:
        return cls(
            external_id=resource.get("externalId"),
            template_name=resource.get("templateName"),
            field_resolvers={
                key: cls._field_resolver_load(field_resolver)
                for key, field_resolver in resource.get("fieldResolvers", {}).items()
            }
            or None,
            data_set_id=resource.get("dataSetId"),
            created_time=resource.get("createdTime"),
            last_updated_time=resource.get("lastUpdatedTime"),
            cognite_client=cognite_client,
        )


class TemplateInstanceWrite(TemplateInstanceCore):
    """A template instance that implements a template by specifying a resolver per field.

    Args:
        external_id (str | None): The id of the template instance.
        template_name (str | None): The template name to implement.
        field_resolvers (dict[str, FieldResolvers] | None): A set of field resolvers where the dictionary key correspond to the field name.
        data_set_id (int | None): The id of the dataset this instance belongs to.
    """

    def __init__(
        self,
        external_id: str | None = None,
        template_name: str | None = None,
        field_resolvers: dict[str, FieldResolvers] | None = None,
        data_set_id: int | None = None,
    ) -> None:
        super().__init__(external_id, template_name, field_resolvers, data_set_id)

    def as_write(self) -> TemplateInstanceWrite:
        return self

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> TemplateInstanceWrite:
        return cls(
            external_id=resource.get("externalId"),
            template_name=resource.get("templateName"),
            field_resolvers={
                key: cls._field_resolver_load(field_resolver)
                for key, field_resolver in resource.get("fieldResolvers", {}).items()
            }
            or None,
            data_set_id=resource.get("dataSetId"),
        )


class TemplateInstanceUpdate(CogniteUpdate):
    """Changes applied to template instance

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
    """

    class _ObjectAssetUpdate(CogniteObjectUpdate):
        def set(self, value: dict) -> TemplateInstanceUpdate:
            return self._set(value)

        def add(self, value: dict) -> TemplateInstanceUpdate:
            return self._add(value)

        def remove(self, value: list) -> TemplateInstanceUpdate:
            return self._remove(value)

    @property
    def field_resolvers(self) -> _ObjectAssetUpdate:
        return TemplateInstanceUpdate._ObjectAssetUpdate(self, "fieldResolvers")

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            PropertySpec("field_resolvers"),
        ]


class Source(CogniteResource):
    """
    A source defines the data source with filters and a mapping table.

    Args:
        type (str | None): The type of source. Possible values are: "events", "assets", "sequences", "timeSeries", "files".
        filter (dict[str, Any] | None): The filter to apply to the source when resolving the source. A filter also supports binding view input to the filter, by prefixing the input name with '$'.
        mappings (dict[str, str] | None): The mapping between source result and expected schema.
        cognite_client (CogniteClient | None): No description.
    """

    def __init__(
        self,
        type: str | None = None,
        filter: dict[str, Any] | None = None,
        mappings: dict[str, str] | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.type = type
        self.filter = filter
        self.mappings = mappings
        self._cognite_client = cast("CogniteClient", cognite_client)


class ViewCore(WriteableCogniteResource["ViewWrite"], ABC):
    """
    A view is used to map existing data to a type in the template group. A view supports input, that can be bound to the underlying filter.

    Args:
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        source (Source | None): Defines the data source for the view.
        data_set_id (int | None): The dataSetId of the view
    """

    def __init__(
        self,
        external_id: str | None = None,
        source: Source | None = None,
        data_set_id: int | None = None,
    ) -> None:
        self.external_id = external_id
        self.source = source
        self.data_set_id = data_set_id

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to True.

        Returns:
            dict[str, Any]: A dictionary representation of the instance.
        """
        dumped = {
            key: self.resolve_nested_classes(value, camel_case)
            for key, value in self.__dict__.items()
            if value is not None and not key.startswith("_")
        }
        if camel_case:
            return convert_all_keys_to_camel_case(dumped)
        return dumped

    @staticmethod
    def resolve_nested_classes(value: CogniteResource | dict, camel_case: bool) -> dict:
        if isinstance(value, CogniteResource):
            return value.dump(camel_case)
        else:
            return value


class View(ViewCore):
    """
    A view is used to map existing data to a type in the template group. A view supports input, that can be bound to the underlying filter.

    Args:
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        source (Source | None): Defines the data source for the view.
        data_set_id (int | None): The dataSetId of the view
        created_time (int | None): No description.
        last_updated_time (int | None): No description.
        cognite_client (CogniteClient | None): No description.
    """

    def __init__(
        self,
        external_id: str | None = None,
        source: Source | None = None,
        data_set_id: int | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        super().__init__(external_id=external_id, source=source, data_set_id=data_set_id)
        # created_time/last_updated_time are required when using the class to read,
        # but don't make sense passing in when creating a new object. So in order to make the typing
        # correct here (i.e. int and not Optional[int]), we force the type to be int rather than
        # Optional[int].
        # TODO: In the next major version we can make these properties required in the constructor
        self.created_time: int = created_time  # type: ignore
        self.last_updated_time: int = last_updated_time  # type: ignore
        self._cognite_client = cast("CogniteClient", cognite_client)

    def as_write(self) -> ViewWrite:
        return ViewWrite(
            external_id=self.external_id,
            source=self.source,
            data_set_id=self.data_set_id,
        )

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> View:
        instance = cls(cognite_client=cognite_client)
        for key, value in resource.items():
            snake_case_key = to_snake_case(key)
            if hasattr(instance, snake_case_key):
                value = value if key != "source" else Source._load(value, cognite_client)
                setattr(instance, snake_case_key, value)
        return instance


class ViewWrite(ViewCore):
    """
    A view is used to map existing data to a type in the template group. A view supports input, that can be bound to the underlying filter.
    This is the write format of a view.

    Args:
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        source (Source | None): Defines the data source for the view.
        data_set_id (int | None): The dataSetId of the view
    """

    def __init__(
        self,
        external_id: str | None = None,
        source: Source | None = None,
        data_set_id: int | None = None,
    ) -> None:
        super().__init__(external_id, source, data_set_id)

    def as_write(self) -> ViewWrite:
        return self

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> ViewWrite:
        return cls(
            external_id=resource.get("externalId"),
            source=Source._load(source, cognite_client) if (source := resource.get("source")) else None,
            data_set_id=resource.get("dataSetId"),
        )


class ViewResolveItem(UserDict, CogniteResource):
    def __init__(self, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> None:
        super().__init__(data)
        self._cognite_client = cast("CogniteClient", cognite_client)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return self.data.copy()

    @classmethod
    def _load(cls, data: dict[str, Any], cognite_client: CogniteClient | None = None) -> ViewResolveItem:
        return cls(data, cognite_client=cognite_client)


class GraphQlError(CogniteResource):
    def __init__(
        self,
        message: str | None = None,
        path: list[str] | None = None,
        locations: list[dict[str, Any]] | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.message = message
        self.path = path
        self.locations = locations
        self._cognite_client = cast("CogniteClient", cognite_client)


class GraphQlResponse(CogniteResource):
    def __init__(
        self,
        data: Any | None = None,
        errors: list[GraphQlError] | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.data = data
        self.errors = errors
        self._cognite_client = cast("CogniteClient", cognite_client)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if self.errors:
            output["errors"] = [error.dump(camel_case) for error in self.errors]
        return output

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> GraphQlResponse:
        return cls(
            data=resource.get("data"),
            errors=[GraphQlError._load(error) for error in resource.get("errors", [])] or None,
            cognite_client=cognite_client,
        )


class TemplateInstanceWriteList(CogniteResourceList[TemplateInstanceWrite]):
    _RESOURCE = TemplateInstanceWrite


class TemplateInstanceList(WriteableCogniteResourceList[TemplateInstanceWrite, TemplateInstance]):
    _RESOURCE = TemplateInstance

    def as_write(self) -> TemplateInstanceWriteList:
        return TemplateInstanceWriteList([item.as_write() for item in self], cognite_client=self._get_cognite_client())


class ViewWriteList(CogniteResourceList[ViewWrite]):
    _RESOURCE = ViewWrite


class ViewList(WriteableCogniteResourceList[ViewWrite, View]):
    _RESOURCE = View

    def as_write(self) -> ViewWriteList:
        return ViewWriteList([item.as_write() for item in self], cognite_client=self._get_cognite_client())


class ViewResolveList(CogniteResourceList[ViewResolveItem]):
    _RESOURCE = ViewResolveItem
