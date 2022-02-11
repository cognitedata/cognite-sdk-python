from cognite.client.data_classes._base import *
from cognite.client.data_classes.transformations.common import *
from cognite.client.data_classes.transformations.jobs import TransformationJob, TransformationJobList
from cognite.client.data_classes.transformations.schedules import TransformationSchedule
from cognite.client.data_classes.transformations.schema import TransformationSchemaColumnList


class Transformation(CogniteResource):
    """The transformations resource allows transforming data in CDF.

    Args:
        id (int): A server-generated ID for the object.
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        name (str): The name of the Transformation.
        query (str): SQL query of the transformation.
        destination (TransformationDestination): see TransformationDestination for options.
        conflict_mode (str): What to do in case of id collisions: either "abort", "upsert", "update" or "delete"
        is_public (bool): Indicates if the transformation is visible to all in project or only to the owner.
        ignore_null_fields (bool): Indicates how null values are handled on updates: ignore or set null.
        source_api_key (str): Configures the transformation to authenticate with the given api key on the source.
        destination_api_key (str): Configures the transformation to authenticate with the given api key on the destination.
        source_oidc_credentials (Optional[OidcCredentials]): Configures the transformation to authenticate with the given oidc credentials key on the destination.
        destination_oidc_credentials (Optional[OidcCredentials]): Configures the transformation to authenticate with the given oidc credentials on the destination.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        owner (str): Owner of the transformation: requester's identity.
        owner_is_current_user (bool): Indicates if the transformation belongs to the current user.
        has_source_api_key (bool): Indicates if the transformation is configured with a source api key.
        has_destination_api_key (bool): Indicates if the transformation is configured with a destination api key.
        has_source_oidc_credentials (bool): Indicates if the transformation is configured with a source oidc credentials set.
        has_destination_oidc_credentials (bool): Indicates if the transformation is configured with a destination oidc credentials set.
        running_job (TransformationJob): Details for the job of this transformation currently running.
        last_finished_job (TransformationJob): Details for the last finished job of this transformation.
        blocked (TransformationBlockedInfo): Provides reason and time if the transformation is blocked.
        schedule (TransformationSchedule): Details for the schedule if the transformation is scheduled.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        id: int = None,
        external_id: str = None,
        name: str = None,
        query: str = None,
        destination: TransformationDestination = None,
        conflict_mode: str = None,
        is_public: bool = True,
        ignore_null_fields: bool = False,
        source_api_key: str = None,
        destination_api_key: str = None,
        source_oidc_credentials: Optional[OidcCredentials] = None,
        destination_oidc_credentials: Optional[OidcCredentials] = None,
        created_time: Optional[int] = None,
        last_updated_time: Optional[int] = None,
        owner: str = None,
        owner_is_current_user: bool = True,
        has_source_api_key: Optional[bool] = None,
        has_destination_api_key: Optional[bool] = None,
        has_source_oidc_credentials: Optional[bool] = None,
        has_destination_oidc_credentials: Optional[bool] = None,
        running_job: "TransformationJob" = None,
        last_finished_job: "TransformationJob" = None,
        blocked: TransformationBlockedInfo = None,
        schedule: "TransformationSchedule" = None,
        data_set_id: int = None,
        cognite_client=None,
    ):
        self.id = id
        self.external_id = external_id
        self.name = name
        self.query = query
        self.destination = destination
        self.conflict_mode = conflict_mode
        self.is_public = is_public
        self.ignore_null_fields = ignore_null_fields
        self.source_api_key = source_api_key
        self.has_source_api_key = has_source_api_key or source_api_key is not None
        self.destination_api_key = destination_api_key
        self.has_destination_api_key = has_destination_api_key or destination_api_key is not None
        self.source_oidc_credentials = source_oidc_credentials
        self.has_source_oidc_credentials = has_source_oidc_credentials or source_oidc_credentials is not None
        self.destination_oidc_credentials = destination_oidc_credentials
        self.has_destination_oidc_credentials = (
            has_destination_oidc_credentials or destination_oidc_credentials is not None
        )
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.owner = owner
        self.owner_is_current_user = owner_is_current_user
        self.running_job = running_job
        self.last_finished_job = last_finished_job
        self.blocked = blocked
        self.schedule = schedule
        self.data_set_id = data_set_id
        self._cognite_client = cognite_client

    def run(self, wait: bool = True, timeout: Optional[float] = None) -> "TransformationJob":
        return self._cognite_client.transformations.run(transformation_id=self.id, wait=wait, timeout=timeout)

    def cancel(self):
        if self.id is None:
            self._cognite_client.transformations.cancel(transformation_external_id=self.external_id)
        else:
            self._cognite_client.transformations.cancel(transformation_id=self.id)

    def run_async(self, timeout: Optional[float] = None) -> Awaitable["TransformationJob"]:
        return self._cognite_client.transformations.run_async(transformation_id=self.id, timeout=timeout)

    def jobs(self) -> "TransformationJobList":
        return self._cognite_client.transformations.jobs.list(transformation_id=self.id)

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client=None):
        instance = super(Transformation, cls)._load(resource, cognite_client)
        if isinstance(instance.destination, Dict):
            snake_dict = {utils._auxiliary.to_snake_case(key): value for (key, value) in instance.destination.items()}
            if instance.destination.get("type") == "raw":
                snake_dict.pop("type")
                instance.destination = RawTable(**snake_dict)
            else:
                instance.destination = TransformationDestination(**snake_dict)
        if isinstance(instance.running_job, Dict):
            snake_dict = {utils._auxiliary.to_snake_case(key): value for (key, value) in instance.running_job.items()}
            instance.running_job = TransformationJob._load(snake_dict, cognite_client=cognite_client)
        if isinstance(instance.last_finished_job, Dict):
            snake_dict = {
                utils._auxiliary.to_snake_case(key): value for (key, value) in instance.last_finished_job.items()
            }
            instance.last_finished_job = TransformationJob._load(snake_dict, cognite_client=cognite_client)
        if isinstance(instance.blocked, Dict):
            snake_dict = {utils._auxiliary.to_snake_case(key): value for (key, value) in instance.blocked.items()}
            snake_dict.pop("time")
            instance.blocked = TransformationBlockedInfo(**snake_dict)
        if isinstance(instance.schedule, Dict):
            snake_dict = {utils._auxiliary.to_snake_case(key): value for (key, value) in instance.schedule.items()}
            instance.schedule = TransformationSchedule._load(snake_dict, cognite_client=cognite_client)
        return instance

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to False.

        Returns:
            Dict[str, Any]: A dictionary representation of the instance.
        """
        ret = super().dump(camel_case=camel_case)

        if self.source_oidc_credentials:
            source_key = "sourceOidcCredentials" if camel_case else "source_oidc_credentials"
            ret[source_key] = self.source_oidc_credentials.dump(camel_case=camel_case)
        if self.destination_oidc_credentials:
            destination_key = "destinationOidcCredentials" if camel_case else "destination_oidc_credentials"
            ret[destination_key] = self.destination_oidc_credentials.dump(camel_case=camel_case)

        return ret

    def __hash__(self):
        return hash(self.external_id)


class TransformationUpdate(CogniteUpdate):
    """Changes applied to transformation

    Args:
        id (int): A server-generated ID for the object.
        external_id (str): External Id provided by client. Should be unique within the project.
    """

    class _PrimitiveTransformationUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> "TransformationUpdate":
            return self._set(value)

    @property
    def external_id(self):
        return TransformationUpdate._PrimitiveTransformationUpdate(self, "externalId")

    @property
    def name(self):
        return TransformationUpdate._PrimitiveTransformationUpdate(self, "name")

    @property
    def destination(self):
        return TransformationUpdate._PrimitiveTransformationUpdate(self, "destination")

    @property
    def conflict_mode(self):
        return TransformationUpdate._PrimitiveTransformationUpdate(self, "conflictMode")

    @property
    def query(self):
        return TransformationUpdate._PrimitiveTransformationUpdate(self, "query")

    @property
    def source_oidc_credentials(self):
        return TransformationUpdate._PrimitiveTransformationUpdate(self, "sourceOidcCredentials")

    @property
    def destination_oidc_credentials(self):
        return TransformationUpdate._PrimitiveTransformationUpdate(self, "destinationOidcCredentials")

    @property
    def source_api_key(self):
        return TransformationUpdate._PrimitiveTransformationUpdate(self, "sourceApiKey")

    @property
    def destination_api_key(self):
        return TransformationUpdate._PrimitiveTransformationUpdate(self, "destinationApiKey")

    @property
    def is_public(self):
        return TransformationUpdate._PrimitiveTransformationUpdate(self, "isPublic")

    @property
    def ignore_null_fields(self):
        return TransformationUpdate._PrimitiveTransformationUpdate(self, "ignoreNullFields")

    @property
    def data_set_id(self):
        return TransformationUpdate._PrimitiveTransformationUpdate(self, "dataSetId")


class TransformationList(CogniteResourceList):
    _RESOURCE = Transformation
    _UPDATE = TransformationUpdate


class TransformationFilter(CogniteFilter):
    """No description.

    Args:
        include_public (bool): Whether public transformations should be included in the results. The default is true.
    """

    def __init__(self, include_public: bool = True):
        self.include_public = include_public


class TransformationPreviewResult(CogniteResource):
    """Allows previewing the result of a sql transformation before executing it.

    Args:
        schema (TransformationSchemaColumnList): List of column descriptions.
        results (List[Dict]): List of resulting rows. Each row is a dictionary where the key is the column name and the value is the entrie.
    """

    def __init__(
        self, schema: "TransformationSchemaColumnList" = None, results: List[Dict] = None, cognite_client=None
    ):
        self.schema = schema
        self.results = results
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client=None):
        instance = super(TransformationPreviewResult, cls)._load(resource, cognite_client)
        if isinstance(instance.schema, Dict):
            items = instance.schema.get("items")
            if items is not None:
                instance.schema = TransformationSchemaColumnList._load(items, cognite_client=cognite_client)
        if isinstance(instance.results, Dict):
            items = instance.results.get("items")
            if items is not None:
                instance.results = items
        return instance

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to False.

        Returns:
            Dict[str, Any]: A dictionary representation of the instance.
        """
        ret = super().dump(camel_case=camel_case)
        ret["schema"] = ret["schema"].dump(camel_case=camel_case)
        return ret
