from __future__ import annotations

import logging
from abc import abstractmethod
from copy import deepcopy
from typing import TYPE_CHECKING, Any, Awaitable, Literal, cast

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteListUpdate,
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    IdTransformerMixin,
    PropertySpec,
)
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.data_classes.transformations.common import (
    NonceCredentials,
    OidcCredentials,
    TransformationBlockedInfo,
    TransformationDestination,
    _load_destination_dct,
)
from cognite.client.data_classes.transformations.jobs import TransformationJob, TransformationJobList
from cognite.client.data_classes.transformations.schedules import TransformationSchedule
from cognite.client.data_classes.transformations.schema import TransformationSchemaColumnList
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._text import convert_all_keys_to_camel_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient


logger = logging.getLogger(__name__)


class SessionDetails:
    """Details of a source session.

    Args:
        session_id (int | None): CDF source session ID
        client_id (str | None): Idp source client ID
        project_name (str | None): CDF source project name
    """

    def __init__(
        self,
        session_id: int | None = None,
        client_id: str | None = None,
        project_name: str | None = None,
    ) -> None:
        self.session_id = session_id
        self.client_id = client_id
        self.project_name = project_name

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> SessionDetails:
        return cls(
            session_id=resource.get("sessionId"),
            client_id=resource.get("clientId"),
            project_name=resource.get("projectName"),
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to False.

        Returns:
            dict[str, Any]: A dictionary representation of the instance.
        """
        ret = vars(self)

        if camel_case:
            return convert_all_keys_to_camel_case(ret)
        return ret


class Transformation(CogniteResource):
    """The transformations resource allows transforming data in CDF.

    Args:
        id (int | None): A server-generated ID for the object.
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        name (str | None): The name of the Transformation.
        query (str | None): SQL query of the transformation.
        destination (TransformationDestination | None): see TransformationDestination for options.
        conflict_mode (str | None): What to do in case of id collisions: either "abort", "upsert", "update" or "delete"
        is_public (bool): Indicates if the transformation is visible to all in project or only to the owner.
        ignore_null_fields (bool): Indicates how null values are handled on updates: ignore or set null.
        source_oidc_credentials (OidcCredentials | None): Configures the transformation to authenticate with the given oidc credentials key on the destination.
        destination_oidc_credentials (OidcCredentials | None): Configures the transformation to authenticate with the given oidc credentials on the destination.
        created_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        owner (str | None): Owner of the transformation: requester's identity.
        owner_is_current_user (bool): Indicates if the transformation belongs to the current user.
        has_source_oidc_credentials (bool | None): Indicates if the transformation is configured with a source oidc credentials set.
        has_destination_oidc_credentials (bool | None): Indicates if the transformation is configured with a destination oidc credentials set.
        running_job (TransformationJob | None): Details for the job of this transformation currently running.
        last_finished_job (TransformationJob | None): Details for the last finished job of this transformation.
        blocked (TransformationBlockedInfo | None): Provides reason and time if the transformation is blocked.
        schedule (TransformationSchedule | None): Details for the schedule if the transformation is scheduled.
        data_set_id (int | None): No description.
        cognite_client (CogniteClient | None): The client to associate with this object.
        source_nonce (NonceCredentials | None): Single use credentials to bind to a CDF session for reading.
        destination_nonce (NonceCredentials | None): Single use credentials to bind to a CDF session for writing.
        source_session (SessionDetails | None): Details for the session used to read from the source project.
        destination_session (SessionDetails | None): Details for the session used to write to the destination project.
        tags (list[str] | None): No description.
    """

    def __init__(
        self,
        id: int | None = None,
        external_id: str | None = None,
        name: str | None = None,
        query: str | None = None,
        destination: TransformationDestination | None = None,
        conflict_mode: str | None = None,
        is_public: bool = True,
        ignore_null_fields: bool = False,
        source_oidc_credentials: OidcCredentials | None = None,
        destination_oidc_credentials: OidcCredentials | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        owner: str | None = None,
        owner_is_current_user: bool = True,
        has_source_oidc_credentials: bool | None = None,
        has_destination_oidc_credentials: bool | None = None,
        running_job: TransformationJob | None = None,
        last_finished_job: TransformationJob | None = None,
        blocked: TransformationBlockedInfo | None = None,
        schedule: TransformationSchedule | None = None,
        data_set_id: int | None = None,
        cognite_client: CogniteClient | None = None,
        source_nonce: NonceCredentials | None = None,
        destination_nonce: NonceCredentials | None = None,
        source_session: SessionDetails | None = None,
        destination_session: SessionDetails | None = None,
        tags: list[str] | None = None,
    ) -> None:
        self.id = id
        self.external_id = external_id
        self.name = name
        self.query = query
        self.destination = destination
        self.conflict_mode = conflict_mode
        self.is_public = is_public
        self.ignore_null_fields = ignore_null_fields
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
        self.source_nonce = source_nonce
        self.destination_nonce = destination_nonce
        self.source_session = source_session
        self.destination_session = destination_session
        self.tags = tags
        self._cognite_client = cast("CogniteClient", cognite_client)

    def copy(self) -> Transformation:
        return Transformation(
            self.id,
            self.external_id,
            self.name,
            self.query,
            self.destination,
            self.conflict_mode,
            self.is_public,
            self.ignore_null_fields,
            self.source_oidc_credentials,
            self.destination_oidc_credentials,
            self.created_time,
            self.last_updated_time,
            self.owner,
            self.owner_is_current_user,
            self.has_source_oidc_credentials,
            self.has_destination_oidc_credentials,
            self.running_job,
            self.last_finished_job,
            self.blocked,
            self.schedule,
            self.data_set_id,
            None,  # skip cognite client
            self.source_nonce,
            self.destination_nonce,
            self.source_session,
            self.destination_session,
            self.tags,
        )

    def _process_credentials(
        self, sessions_cache: dict[str, NonceCredentials] | None = None, keep_none: bool = False
    ) -> None:
        if sessions_cache is None:
            sessions_cache = {}

        if self.source_nonce is None and self.source_oidc_credentials:
            self.source_nonce = self._try_get_or_create_nonce(
                self.source_oidc_credentials,
                sessions_cache,
                keep_none,
                credentials_name="source",
            )
            if self.source_nonce:
                self.source_oidc_credentials = None

        if self.destination_nonce is None and self.destination_oidc_credentials:
            self.destination_nonce = self._try_get_or_create_nonce(
                self.destination_oidc_credentials,
                sessions_cache,
                keep_none,
                credentials_name="destination",
            )
            if self.destination_nonce:
                self.destination_oidc_credentials = None

    def _try_get_or_create_nonce(
        self,
        oidc_credentials: OidcCredentials,
        sessions_cache: dict[str, NonceCredentials],
        keep_none: bool,
        credentials_name: Literal["source", "destination"],
    ) -> NonceCredentials | None:
        if keep_none and oidc_credentials is None:
            return None

        project = oidc_credentials.cdf_project_name or self._cognite_client.config.project

        key = "DEFAULT"
        if oidc_credentials:
            key = f"{oidc_credentials.client_id}:{hash(oidc_credentials.client_secret)}:{project}"

        if (ret := sessions_cache.get(key)) is None:
            credentials = None
            if oidc_credentials and oidc_credentials.client_id and oidc_credentials.client_secret:
                credentials = oidc_credentials.as_client_credentials()

            # We want to create a session using the supplied 'oidc_credentials' (either 'source_oidc_credentials'
            # or 'destination_oidc_credentials') and send the nonce to the Transformations backend, to avoid sending
            # (and it having to store) the full set of client credentials. However, there is no easy way to do this
            # without instantiating a new 'CogniteClient' with the given credentials:
            from cognite.client import CogniteClient

            config = deepcopy(self._cognite_client.config)
            config.project = project
            config.credentials = oidc_credentials.as_credential_provider()
            other_client = CogniteClient(config)
            try:
                session = other_client.iam.sessions.create(credentials)
                ret = sessions_cache[key] = NonceCredentials(session.id, session.nonce, project)
            except CogniteAPIError as err:
                # This is fine, we might be missing SessionsACL. The OIDC credentials will then be passed
                # directly to the backend service. We do however not catch CogniteAuthError as that would
                # mean the credentials are invalid.
                logger.debug(
                    f"Unable to create a session and get a nonce towards {project=} using the provided "
                    f"{credentials_name} credentials: {err!r}"
                )
        return ret

    def run(self, wait: bool = True, timeout: float | None = None) -> TransformationJob:
        return self._cognite_client.transformations.run(transformation_id=self.id, wait=wait, timeout=timeout)

    def cancel(self) -> None:
        if self.id is None:
            self._cognite_client.transformations.cancel(transformation_external_id=self.external_id)
        else:
            self._cognite_client.transformations.cancel(transformation_id=self.id)

    def run_async(self, timeout: float | None = None) -> Awaitable[TransformationJob]:
        return self._cognite_client.transformations.run_async(transformation_id=self.id, timeout=timeout)

    def jobs(self) -> TransformationJobList:
        return self._cognite_client.transformations.jobs.list(transformation_id=self.id)

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> Transformation:
        instance = super()._load(resource, cognite_client)
        if isinstance(instance.destination, dict):
            instance.destination = _load_destination_dct(instance.destination)

        if isinstance(instance.running_job, dict):
            instance.running_job = TransformationJob._load(instance.running_job, cognite_client=cognite_client)

        if isinstance(instance.last_finished_job, dict):
            instance.last_finished_job = TransformationJob._load(
                instance.last_finished_job, cognite_client=cognite_client
            )

        if isinstance(instance.blocked, dict):
            instance.blocked = TransformationBlockedInfo._load(instance.blocked)

        if isinstance(instance.schedule, dict):
            instance.schedule = TransformationSchedule._load(instance.schedule, cognite_client=cognite_client)

        if isinstance(instance.source_session, dict):
            instance.source_session = SessionDetails._load(instance.source_session)

        if isinstance(instance.destination_session, dict):
            instance.destination_session = SessionDetails._load(instance.destination_session)
        return instance

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to False.

        Returns:
            dict[str, Any]: A dictionary representation of the instance.
        """

        ret = super().dump(camel_case=camel_case)

        for name, prop in ret.items():
            if hasattr(prop, "dump"):
                ret[name] = prop.dump(camel_case=camel_case)
        return ret

    def __hash__(self) -> int:
        return hash(self.external_id)


class TransformationUpdate(CogniteUpdate):
    """Changes applied to transformation

    Args:
        id (int): A server-generated ID for the object.
        external_id (str): External Id provided by client. Should be unique within the project.
    """

    class _PrimitiveTransformationUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> TransformationUpdate:
            return self._set(value)

    class _ListTransformationUpdate(CogniteListUpdate):
        def set(self, value: list) -> TransformationUpdate:
            return self._set(value)

        def add(self, value: list) -> TransformationUpdate:
            return self._add(value)

        def remove(self, value: list) -> TransformationUpdate:
            return self._remove(value)

    @property
    def external_id(self) -> _PrimitiveTransformationUpdate:
        return TransformationUpdate._PrimitiveTransformationUpdate(self, "externalId")

    @property
    def name(self) -> _PrimitiveTransformationUpdate:
        return TransformationUpdate._PrimitiveTransformationUpdate(self, "name")

    @property
    def destination(self) -> _PrimitiveTransformationUpdate:
        return TransformationUpdate._PrimitiveTransformationUpdate(self, "destination")

    @property
    def conflict_mode(self) -> _PrimitiveTransformationUpdate:
        return TransformationUpdate._PrimitiveTransformationUpdate(self, "conflictMode")

    @property
    def query(self) -> _PrimitiveTransformationUpdate:
        return TransformationUpdate._PrimitiveTransformationUpdate(self, "query")

    @property
    def source_oidc_credentials(self) -> _PrimitiveTransformationUpdate:
        return TransformationUpdate._PrimitiveTransformationUpdate(self, "sourceOidcCredentials")

    @property
    def destination_oidc_credentials(self) -> _PrimitiveTransformationUpdate:
        return TransformationUpdate._PrimitiveTransformationUpdate(self, "destinationOidcCredentials")

    @property
    def source_nonce(self) -> _PrimitiveTransformationUpdate:
        return TransformationUpdate._PrimitiveTransformationUpdate(self, "sourceNonce")

    @property
    def destination_nonce(self) -> _PrimitiveTransformationUpdate:
        return TransformationUpdate._PrimitiveTransformationUpdate(self, "destinationNonce")

    @property
    def is_public(self) -> _PrimitiveTransformationUpdate:
        return TransformationUpdate._PrimitiveTransformationUpdate(self, "isPublic")

    @property
    def ignore_null_fields(self) -> _PrimitiveTransformationUpdate:
        return TransformationUpdate._PrimitiveTransformationUpdate(self, "ignoreNullFields")

    @property
    def data_set_id(self) -> _PrimitiveTransformationUpdate:
        return TransformationUpdate._PrimitiveTransformationUpdate(self, "dataSetId")

    @property
    def tags(self) -> _ListTransformationUpdate:
        return TransformationUpdate._ListTransformationUpdate(self, "tags")

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        obj = super().dump()

        for update in obj.get("update", {}).values():
            item = update.get("set")
            if isinstance(item, (TransformationDestination, OidcCredentials, NonceCredentials)):
                update["set"] = item.dump(camel_case=camel_case)
        return obj

    @classmethod
    def _get_update_properties(cls) -> list[PropertySpec]:
        return [
            PropertySpec("external_id", is_nullable=False),
            PropertySpec("name", is_nullable=False),
            PropertySpec("destination", is_nullable=False),
            PropertySpec("conflict_mode", is_nullable=False),
            PropertySpec("query", is_nullable=False),
            PropertySpec("source_oidc_credentials"),
            PropertySpec("destination_oidc_credentials"),
            PropertySpec("source_nonce"),
            PropertySpec("destination_nonce"),
            PropertySpec("is_public", is_nullable=False),
            PropertySpec("ignore_null_fields", is_nullable=False),
            PropertySpec("data_set_id"),
            PropertySpec("tags", is_container=True),
        ]


class TransformationList(CogniteResourceList[Transformation], IdTransformerMixin):
    _RESOURCE = Transformation


class TagsFilter:
    @abstractmethod
    def dump(self) -> dict[str, Any]:
        ...


class ContainsAny(TagsFilter):
    """Return transformations that has one of the tags specified.

    Args:
        tags (list[str] | None): The resource item contains at least one of the listed tags. The tags are defined by a list of external ids.

    Examples:

            List only resources marked as a PUMP or as a VALVE::

                >>> from cognite.client.data_classes.transformations import ContainsAny
                >>> my_tag_filter = ContainsAny(tags=["PUMP", "VALVE"])
    """

    def __init__(self, tags: list[str] | None = None) -> None:
        self.tags = tags

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        contains_any_key = "containsAny" if camel_case else "contains_any"
        return {contains_any_key: self.tags}


class TransformationFilter(CogniteFilter):
    """No description.

    Args:
        include_public (bool): Whether public transformations should be included in the results. The default is true.
        name_regex (str | None): Regex expression to match the transformation name
        query_regex (str | None): Regex expression to match the transformation query
        destination_type (str | None): Transformation destination resource name to filter by.
        conflict_mode (str | None): Filters by a selected transformation action type: abort/create, upsert, update, delete
        cdf_project_name (str | None): Project name to filter by configured source and destination project
        has_blocked_error (bool | None): Whether only the blocked transformations should be included in the results.
        created_time (dict[str, Any] | TimestampRange | None): Range between two timestamps
        last_updated_time (dict[str, Any] | TimestampRange | None): Range between two timestamps
        data_set_ids (list[dict[str, Any]] | None): Return only transformations in the specified data sets with these ids.
        tags (TagsFilter | None): Return only the resource matching the specified tags constraints. It only supports ContainsAny as of now.
    """

    def __init__(
        self,
        include_public: bool = True,
        name_regex: str | None = None,
        query_regex: str | None = None,
        destination_type: str | None = None,
        conflict_mode: str | None = None,
        cdf_project_name: str | None = None,
        has_blocked_error: bool | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        data_set_ids: list[dict[str, Any]] | None = None,
        tags: TagsFilter | None = None,
    ) -> None:
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

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        obj = super().dump(camel_case=camel_case)
        if (value := obj.pop("includePublic" if camel_case else "include_public", None)) is not None:
            obj["isPublic" if camel_case else "is_public"] = value

        if (value := obj.pop("tags", None)) is not None:
            obj["tags"] = value.dump(camel_case)
        return obj


class TransformationPreviewResult(CogniteResource):
    """Allows previewing the result of a sql transformation before executing it.

    Args:
        schema (TransformationSchemaColumnList | None): List of column descriptions.
        results (list[dict] | None): List of resulting rows. Each row is a dictionary where the key is the column name and the value is the entry.
        cognite_client (CogniteClient | None): No description.
    """

    def __init__(
        self,
        schema: TransformationSchemaColumnList | None = None,
        results: list[dict] | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.schema = schema
        self.results = results
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> TransformationPreviewResult:
        instance = super()._load(resource, cognite_client)
        if isinstance(instance.schema, dict):
            items = instance.schema.get("items")
            if items is not None:
                instance.schema = TransformationSchemaColumnList._load(items, cognite_client=cognite_client)
        if isinstance(instance.results, dict):
            items = instance.results.get("items")
            if items is not None:
                instance.results = items
        return instance

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to False.

        Returns:
            dict[str, Any]: A dictionary representation of the instance.
        """
        ret = super().dump(camel_case=camel_case)
        ret["schema"] = ret["schema"].dump(camel_case=camel_case)
        return ret
