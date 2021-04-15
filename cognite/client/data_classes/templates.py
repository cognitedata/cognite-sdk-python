from typing import List, Optional

from cognite.client.data_classes._base import *


class TemplateGroup(CogniteResource):
    """A template group is a high level concept encapsulating a schema and a set of template instances. It also has query capability support.

    Template groups are versioned, so there can be multiple template groups with the same external ID.
    The versioning is happening automatically whenever a template groups is changed.

    GraphQL schema definition language is used as the language to describe the structure of the templates and data types.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        description (str): The description of the template groups.
        owners (List[str]): The list of owners for the template groups.
    """

    def __init__(
        self,
        external_id: str = None,
        description: str = None,
        owners: Optional[List[str]] = None,
        created_time: int = None,
        last_updated_time: int = None,
        cognite_client=None,
    ):
        self.external_id = external_id
        self.description = description
        self.owners = owners
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self._cognite_client = cognite_client


class TemplateGroupList(CogniteResourceList):
    _RESOURCE = TemplateGroup
    _UPDATE = CogniteUpdate


class TemplateGroupVersion(CogniteResource):
    """
    A Template Group Version supports specifying different conflict modes, which is used when an existing schema already exists.

    Patch -> It diffs the new schema with the old schema and fails if there are breaking changes.
    Update -> It sets the new schema as schema of a new version.
    Force -> It ignores breaking changes and replaces the old schema with the new schema.
    The default mode is "patch".

    Args:
        schema (str): The GraphQL schema.
        version (int): Incremented by the server whenever the schema of a template groups changes.
        conflict_mode (str): Can be set to 'Patch', 'Update' or 'Force'.
    """

    def __init__(
        self,
        schema: str = None,
        version: int = None,
        conflict_mode: str = None,
        created_time: int = None,
        last_updated_time: int = None,
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
    _UPDATE = CogniteUpdate


class ConstantResolver(CogniteResource):
    """Resolves a field to a constant value. The value can be of any supported JSON type.

    Args:
        value (any): The value of the field.
    """

    def __init__(self, value: any = None, cognite_client=None):
        self.type = "constant"
        self.value = value
        self._cognite_client = cognite_client


class RawResolver(CogniteResource):
    """Resolves a field to a RAW column.

    Args:
        db_name (str): The database name.
        table_name (str): The table name.
        row_key (str): The row key.
        column_name (str): The column to fetch the value from.
    """

    def __init__(
        self,
        db_name: str = None,
        table_name: str = None,
        row_key: str = None,
        column_name: str = None,
        cognite_client=None,
    ):
        self.type = "raw"
        self.db_name = db_name
        self.table_name = table_name
        self.row_key = row_key
        self.column_name = column_name
        self._cognite_client = cognite_client


class SyntheticTimeSeriesResolver(CogniteResource):
    """Resolves a field of type 'SyntheticTimeSeries' to a Synthetic Time Series.

    Args:
        expression (str): The synthetic time series expression. See this for syntax https://docs.cognite.com/api/v1/#tag/Synthetic-Time-Series.
        name (Optional[str]): The name of the Time Series.
        description (Optional[str]): The description for the Time Series.
        metadata (Optional[Dict[str, str]]): Specifies metadata for the Time Series.
        is_step (Optional[bool]): Specifies if the synthetic time series is step based.
        is_string (Optional[bool]): Specifies if the synthetic time series returned contains string values.
        unit (Optional[str]): The unit of the time series.
    """

    def __init__(
        self,
        expression: str = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        is_step: Optional[bool] = None,
        is_string: Optional[bool] = None,
        unit: Optional[str] = None,
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


FieldResolvers = Union[ConstantResolver, RawResolver, SyntheticTimeSeriesResolver, str]


class TemplateInstance(CogniteResource):
    field_resolver_mapper = {
        "constant": ConstantResolver,
        "syntheticTimeSeries": SyntheticTimeSeriesResolver,
        "raw": RawResolver,
    }
    """A template instance that implements a template by specificing a resolver per field.

    Args:
        external_id (str): The id of the template instance.
        template_name (str): The template name to implement.
        field_resolvers (Dict[str, FieldResolvers]): A set of field resolvers where the dictionary key correspond to the field name.
    """

    def __init__(
        self,
        external_id: str = None,
        template_name: str = None,
        field_resolvers: Dict[str, FieldResolvers] = None,
        data_set_id: Optional[int] = None,
        created_time: int = None,
        last_updated_time: int = None,
        cognite_client=None,
    ):
        self.external_id = external_id
        self.template_name = template_name
        self.field_resolvers = field_resolvers
        self.data_set_id = data_set_id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self._cognite_client = cognite_client

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to False.

        Returns:
            Dict[str, Any]: A dictionary representation of the instance.
        """
        if camel_case:
            return {
                utils._auxiliary.to_camel_case(key): value
                if key != "field_resolvers"
                else TemplateInstance._encode_field_resolvers(value, camel_case=camel_case)
                for key, value in self.__dict__.items()
                if value not in EXCLUDE_VALUE and not key.startswith("_")
            }
        return {
            key: value
            if key != "field_resolvers"
            else TemplateInstance._encode_field_resolvers(value, camel_case=camel_case)
            for key, value in self.__dict__.items()
            if value not in EXCLUDE_VALUE and not key.startswith("_")
        }

    def _encode_field_resolvers(field_resolvers: Dict[str, FieldResolvers], camel_case):
        return {key: value.dump(camel_case=camel_case) for key, value in field_resolvers.items()}

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client=None):
        if isinstance(resource, str):
            return cls._load(json.loads(resource), cognite_client=cognite_client)
        elif isinstance(resource, Dict):
            instance = cls(cognite_client=cognite_client)
            for key, value in resource.items():
                snake_case_key = utils._auxiliary.to_snake_case(key)
                if hasattr(instance, snake_case_key):
                    if key == "fieldResolvers":
                        setattr(
                            instance,
                            snake_case_key,
                            {
                                key: TemplateInstance._field_resolver_load(field_resolver)
                                for key, field_resolver in value.items()
                            },
                        )
                    else:
                        setattr(instance, snake_case_key, value)
            return instance
        raise TypeError("Resource must be json str or Dict, not {}".format(type(resource)))

    def _field_resolver_load(resource: Union[Dict, str], cognite_client=None):
        return TemplateInstance.field_resolver_mapper[resource["type"]]._load(resource, cognite_client)


class GraphQlError(CogniteResource):
    def __init__(
        self, message: str = None, path: List[str] = None, locations: List[Dict[str, Any]] = None, cognite_client=None
    ):
        self.message = message
        self.path = path
        self.locations = locations
        self._cognite_client = cognite_client


class GraphQlResponse(CogniteResource):
    def __init__(self, data: any = None, errors: List[GraphQlError] = None, cognite_client=None):
        self.data = data
        self.errors = errors
        self._cognite_client = cognite_client


class TemplateInstanceList(CogniteResourceList):
    _RESOURCE = TemplateInstance
    _UPDATE = CogniteUpdate
