from cognite.client.data_classes._base import *


class TransformationDestination:
    """TransformationDestination has static methods to define the target resource type of a transformation
    """

    def __init__(self, type: str = None):
        self.type = type

    @staticmethod
    def assets():
        """`To be used when the transformation is meant to produce assets.`_
        """
        return TransformationDestination("assets")

    @staticmethod
    def timeseries():
        """`To be used when the transformation is meant to produce time series.`_
        """
        return TransformationDestination("timeseries")

    @staticmethod
    def assethierarchy():
        """`To be used when the transformation is meant to produce asset hierarchies.`_
        """
        return TransformationDestination("assethierarchy")

    @staticmethod
    def events():
        """`To be used when the transformation is meant to produce events.`_
        """
        return TransformationDestination("events")

    @staticmethod
    def datapoints():
        """`To be used when the transformation is meant to produce numeric data points.`_
        """
        return TransformationDestination("datapoints")

    @staticmethod
    def stringdatapoints():
        """`To be used when the transformation is meant to produce string data points.`_
        """
        return TransformationDestination("stringdatapoints")

    @staticmethod
    def sequences():
        """`To be used when the transformation is meant to produce sequences.`_
        """
        return TransformationDestination("sequences")

    @staticmethod
    def files():
        """`To be used when the transformation is meant to produce files.`_
        """
        return TransformationDestination("files")

    @staticmethod
    def labels():
        """`To be used when the transformation is meant to produce labels.`_
        """
        return TransformationDestination("labels")

    @staticmethod
    def relationships():
        """`To be used when the transformation is meant to produce relationships.`_
        """
        return TransformationDestination("relationships")

    @staticmethod
    def raw(database: str = "", table: str = ""):
        """`To be used when the transformation is meant to produce raw table rows.`_

        Args:
            database (str) – database name of the target raw table.
            table (str) – name of the target raw table

        Returns:
            TransformationDestination pointing to the target table
        """
        return RawTable(type="raw_table", raw_type="plain_raw", database=database, table=table)


class RawTable(TransformationDestination):
    def __init__(self, type: str = None, raw_type: str = None, database: str = None, table: str = None):
        super().__init__(type=type)
        self.rawType = raw_type
        self.database = database
        self.table = table


class OidcCredentials:
    def __init__(
        self,
        client_id: str = None,
        client_secret: str = None,
        scopes: str = None,
        token_uri: str = None,
        cdf_project_name: str = None,
    ):

        self.client_id = client_id
        self.client_secret = client_secret
        self.scopes = scopes
        self.token_uri = token_uri
        self.cdf_project_name = cdf_project_name


class TransformationJobBlockade:
    def __init__(self, reason: str = None, created_time: Optional[int] = None):
        self.reason = reason
        self.created_time = created_time


class Transformation(CogniteResource):
    """The transformations resource allows transforming data in CDF.

    Args:
        id (int): A server-generated ID for the object.
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        name (str): The name of the ExtractionPipeline.
        description (str): The description of the ExtractionPipeline.
        data_set_id (int): The id of the dataset this ExtractionPipeline related with.
        raw_tables (List[Dict[str, str]): list of raw tables in list format: [{"dbName": "value", "tableName" : "value"}].
        last_success (int): Milliseconds value of last success status.
        last_failure (int): Milliseconds value of last failure status.
        last_message (str): Message of last failure.
        last_seen (int): Milliseconds value of last seen status.
        schedule (str): undefined/triggered/streamed/cron regex.
        contacts (List[Dict[str, Any]]): list of contacts [{"name": "value", "email": "value", "role": "value", "sendNotification": boolean},...]
        metadata (Dict[str, str]): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 10240 bytes, up to 256 key-value pairs, of total size at most 10240.
        source (str): Source text value for ExtractionPipeline.
        documentation (str): Documentation text value for ExtractionPipeline.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        created_by (str): ExtractionPipeline creator, usually email.
        skip_notifications_in_minutes (int): Number value for system to skip sending email notification in minutes after last sending.
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
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client=None):
        instance = super(Transformation, cls)._load(resource, cognite_client)
        if isinstance(instance.destination, Dict):
            snake_dict = {utils._auxiliary.to_snake_case(key): value for (key, value) in instance.destination.items()}
            if instance.destination.get("type") == "raw_table":
                instance.destination = RawTable(**snake_dict)
            else:
                instance.destination = TransformationDestination(**snake_dict)
        return instance

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
