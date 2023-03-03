from abc import abstractmethod
from cognite.client import utils
from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteListUpdate,
    CognitePrimitiveUpdate,
    CognitePropertyClassUtil,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
)
from cognite.client.data_classes.iam import ClientCredentials
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.data_classes.transformations.common import (
    DataModelInstances,
    NonceCredentials,
    OidcCredentials,
    RawTable,
    SequenceRows,
    TransformationBlockedInfo,
    TransformationDestination,
    _load_destination_dct,
)
from cognite.client.data_classes.transformations.jobs import TransformationJob, TransformationJobList
from cognite.client.data_classes.transformations.schedules import TransformationSchedule
from cognite.client.data_classes.transformations.schema import TransformationSchemaColumnList
from cognite.client.utils._auxiliary import convert_all_keys_to_snake_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class SessionDetails:
    def __init__(self, session_id=None, client_id=None, project_name=None):
        self.session_id = session_id
        self.client_id = client_id
        self.project_name = project_name

    def dump(self, camel_case=False):
        ret = self.__dict__
        if camel_case:
            return {utils._auxiliary.to_camel_case(key): value for (key, value) in ret.items()}
        return ret


class Transformation(CogniteResource):
    def __init__(
        self,
        id=None,
        external_id=None,
        name=None,
        query=None,
        destination=None,
        conflict_mode=None,
        is_public=True,
        ignore_null_fields=False,
        source_api_key=None,
        destination_api_key=None,
        source_oidc_credentials=None,
        destination_oidc_credentials=None,
        created_time=None,
        last_updated_time=None,
        owner=None,
        owner_is_current_user=True,
        has_source_api_key=None,
        has_destination_api_key=None,
        has_source_oidc_credentials=None,
        has_destination_oidc_credentials=None,
        running_job=None,
        last_finished_job=None,
        blocked=None,
        schedule=None,
        data_set_id=None,
        cognite_client=None,
        source_nonce=None,
        destination_nonce=None,
        source_session=None,
        destination_session=None,
        tags=None,
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
        self.has_source_api_key = has_source_api_key or (source_api_key is not None)
        self.destination_api_key = destination_api_key
        self.has_destination_api_key = has_destination_api_key or (destination_api_key is not None)
        self.source_oidc_credentials = source_oidc_credentials
        self.has_source_oidc_credentials = has_source_oidc_credentials or (source_oidc_credentials is not None)
        self.destination_oidc_credentials = destination_oidc_credentials
        self.has_destination_oidc_credentials = has_destination_oidc_credentials or (
            destination_oidc_credentials is not None
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
        self.source_nonce = source_nonce
        self.destination_nonce = destination_nonce
        self.source_session = source_session
        self.destination_session = destination_session
        self.tags = tags
        self._cognite_client = cast("CogniteClient", cognite_client)

    def copy(self):
        return Transformation(
            self.id,
            self.external_id,
            self.name,
            self.query,
            self.destination,
            self.conflict_mode,
            self.is_public,
            self.ignore_null_fields,
            self.source_api_key,
            self.destination_api_key,
            self.source_oidc_credentials,
            self.destination_oidc_credentials,
            self.created_time,
            self.last_updated_time,
            self.owner,
            self.owner_is_current_user,
            self.has_source_api_key,
            self.has_destination_api_key,
            self.has_source_oidc_credentials,
            self.has_destination_oidc_credentials,
            self.running_job,
            self.last_finished_job,
            self.blocked,
            self.schedule,
            self.data_set_id,
            None,
            self.source_nonce,
            self.destination_nonce,
            self.source_session,
            self.destination_session,
            self.tags,
        )

    def _process_credentials(self, sessions_cache=None, keep_none=False):
        if sessions_cache is None:
            sessions_cache = {}

        def try_get_or_create_nonce(oidc_credentials: Optional[OidcCredentials]) -> Optional[NonceCredentials]:
            if keep_none and (oidc_credentials is None):
                return None
            assert sessions_cache is not None
            key = (
                f"{oidc_credentials.client_id}:{hash(oidc_credentials.client_secret)}"
                if oidc_credentials
                else "DEFAULT"
            )
            ret = sessions_cache.get(key)
            if not ret:
                if oidc_credentials and oidc_credentials.client_id and oidc_credentials.client_secret:
                    credentials = ClientCredentials(oidc_credentials.client_id, oidc_credentials.client_secret)
                else:
                    credentials = None
                try:
                    session = self._cognite_client.iam.sessions.create(credentials)
                    ret = NonceCredentials(session.id, session.nonce, self._cognite_client._config.project)
                    sessions_cache[key] = ret
                except Exception:
                    ret = None
            return ret

        if (self.source_api_key is None) and (self.source_nonce is None):
            self.source_nonce = try_get_or_create_nonce(self.source_oidc_credentials)
            if self.source_nonce:
                self.source_oidc_credentials = None
        if (self.destination_api_key is None) and (self.destination_nonce is None):
            self.destination_nonce = try_get_or_create_nonce(self.destination_oidc_credentials)
            if self.destination_nonce:
                self.destination_oidc_credentials = None

    def run(self, wait=True, timeout=None):
        return self._cognite_client.transformations.run(transformation_id=self.id, wait=wait, timeout=timeout)

    def cancel(self):
        if self.id is None:
            self._cognite_client.transformations.cancel(transformation_external_id=self.external_id)
        else:
            self._cognite_client.transformations.cancel(transformation_id=self.id)

    def run_async(self, timeout=None):
        return self._cognite_client.transformations.run_async(transformation_id=self.id, timeout=timeout)

    def jobs(self):
        return self._cognite_client.transformations.jobs.list(transformation_id=self.id)

    @classmethod
    def _load(cls, resource, cognite_client=None):
        instance = super()._load(resource, cognite_client)
        if isinstance(instance.destination, Dict):
            instance.destination = _load_destination_dct(instance.destination)
        if isinstance(instance.running_job, Dict):
            snake_dict = convert_all_keys_to_snake_case(instance.running_job)
            instance.running_job = TransformationJob._load(snake_dict, cognite_client=cognite_client)
        if isinstance(instance.last_finished_job, Dict):
            snake_dict = convert_all_keys_to_snake_case(instance.last_finished_job)
            instance.last_finished_job = TransformationJob._load(snake_dict, cognite_client=cognite_client)
        if isinstance(instance.blocked, Dict):
            snake_dict = convert_all_keys_to_snake_case(instance.blocked)
            snake_dict.pop("time")
            instance.blocked = TransformationBlockedInfo(**snake_dict)
        if isinstance(instance.schedule, Dict):
            snake_dict = convert_all_keys_to_snake_case(instance.schedule)
            instance.schedule = TransformationSchedule._load(snake_dict, cognite_client=cognite_client)
        if isinstance(instance.source_session, Dict):
            snake_dict = convert_all_keys_to_snake_case(instance.source_session)
            instance.source_session = SessionDetails(**snake_dict)
        if isinstance(instance.destination_session, Dict):
            snake_dict = convert_all_keys_to_snake_case(instance.destination_session)
            instance.destination_session = SessionDetails(**snake_dict)
        return instance

    def dump(self, camel_case=False):
        ret = super().dump(camel_case=camel_case)
        for (name, prop) in ret.items():
            if isinstance(
                prop,
                (OidcCredentials, NonceCredentials, TransformationDestination, SessionDetails, TransformationSchedule),
            ):
                ret[name] = prop.dump(camel_case=camel_case)
        return ret

    def __hash__(self):
        return hash(self.external_id)


class TransformationUpdate(CogniteUpdate):
    class _PrimitiveTransformationUpdate(CognitePrimitiveUpdate):
        def set(self, value):
            return self._set(value)

    class _ListTransformationUpdate(CogniteListUpdate):
        def set(self, value):
            return self._set(value)

        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

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
    def source_nonce(self):
        return TransformationUpdate._PrimitiveTransformationUpdate(self, "sourceNonce")

    @property
    def destination_nonce(self):
        return TransformationUpdate._PrimitiveTransformationUpdate(self, "destinationNonce")

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

    @property
    def tags(self):
        return TransformationUpdate._ListTransformationUpdate(self, "tags")

    def dump(self, camel_case=True):
        obj = super().dump()
        for update in obj.get("update", {}).values():
            item = update.get("set")
            if isinstance(item, (TransformationDestination, OidcCredentials, NonceCredentials)):
                update["set"] = item.dump(camel_case=camel_case)
        return obj


class TransformationList(CogniteResourceList):
    _RESOURCE = Transformation


class TagsFilter:
    @abstractmethod
    def dump(self):
        ...


class ContainsAny(TagsFilter):
    def __init__(self, tags=None):
        self.tags = tags

    def dump(self, camel_case=True):
        contains_any_key = "containsAny" if camel_case else "contains_any"
        return {contains_any_key: self.tags}


class TransformationFilter(CogniteFilter):
    def __init__(
        self,
        include_public=True,
        name_regex=None,
        query_regex=None,
        destination_type=None,
        conflict_mode=None,
        cdf_project_name=None,
        has_blocked_error=None,
        created_time=None,
        last_updated_time=None,
        data_set_ids=None,
        tags=None,
    ):
        self.include_public = include_public
        self.name_regex = name_regex
        self.query_regex = query_regex
        self.destination_type = destination_type
        self.conflict_mode = conflict_mode
        self.has_blocked_error = has_blocked_error
        self.cdf_project_name = cdf_project_name
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.data_set_ids = data_set_ids
        self.tags = tags

    @classmethod
    def _load(cls, resource):
        instance = super()._load(resource)
        if isinstance(resource, Dict):
            if instance.created_time is not None:
                instance.created_time = TimestampRange(**instance.created_time)
            if instance.last_updated_time is not None:
                instance.last_updated_time = TimestampRange(**instance.last_updated_time)
        return instance

    def dump(self, camel_case=True):
        obj = super().dump(camel_case=camel_case)
        if obj.get("includePublic") is not None:
            is_public = obj.pop("includePublic")
            obj["isPublic"] = is_public
        if obj.get("tags"):
            tags = obj.pop("tags")
            obj["tags"] = tags.dump(camel_case)
        return obj


class TransformationPreviewResult(CogniteResource):
    def __init__(self, schema=None, results=None, cognite_client=None):
        self.schema = schema
        self.results = results
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource, cognite_client=None):
        instance = super()._load(resource, cognite_client)
        if isinstance(instance.schema, Dict):
            items = instance.schema.get("items")
            if items is not None:
                instance.schema = TransformationSchemaColumnList._load(items, cognite_client=cognite_client)
        if isinstance(instance.results, Dict):
            items = instance.results.get("items")
            if items is not None:
                instance.results = items
        return instance

    def dump(self, camel_case=False):
        ret = super().dump(camel_case=camel_case)
        ret["schema"] = ret["schema"].dump(camel_case=camel_case)
        return ret
