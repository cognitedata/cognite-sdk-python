from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NoReturn

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    ExternalIDTransformerMixin,
    PropertySpec,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)

if TYPE_CHECKING:
    from cognite.client import CogniteClient


@dataclass
class SessionWrite(CogniteObject):
    nonce: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(nonce=resource["nonce"])


class _DestinationCore(WriteableCogniteResource["DestinationWrite"], ABC):
    def __init__(self, external_id: str, target_data_set_id: int | None = None) -> None:
        self.external_id = external_id
        self.target_data_set_id = target_data_set_id


class DestinationWrite(_DestinationCore):
    """A hosted extractor writes to a destination.

    The destination contains credentials for CDF, and additional information about where the data should land,
    such as data set ID. Multiple jobs can share a single destination,
    in which case requests will be combined, reducing the number of requests made to CDF APIs.
    Metrics are still reported for each individual job.

    This is the write/request format of the destination.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        credentials (SessionWrite): Credentials for authenticating towards CDF using a CDF session.
        target_data_set_id (int | None): Data set ID the created items are inserted into, if applicable.

    """

    def __init__(self, external_id: str, credentials: SessionWrite, target_data_set_id: int | None = None) -> None:
        super().__init__(external_id, target_data_set_id)
        self.credentials = credentials

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> DestinationWrite:
        return cls(
            external_id=resource["externalId"],
            credentials=SessionWrite._load(resource["credentials"]),
            target_data_set_id=resource.get("targetDataSetId"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if isinstance(self.credentials, SessionWrite):
            output["credentials"] = self.credentials.dump(camel_case)

        return output

    def as_write(self) -> DestinationWrite:
        return self


class Destination(_DestinationCore):
    """A hosted extractor writes to a destination.

    The destination contains credentials for CDF, and additional information about where the data should land,
    such as data set ID. Multiple jobs can share a single destination,
    in which case requests will be combined, reducing the number of requests made to CDF APIs.
    Metrics are still reported for each individual job.

    This is the write/request format of the destination.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        session_id (int | None): ID of the session tied to this destination.
        target_data_set_id (int | None): Data set ID the created items are inserted into, if applicable.

    """

    def __init__(
        self,
        external_id: str,
        created_time: int,
        last_updated_time: int,
        session_id: int | None = None,
        target_data_set_id: int | None = None,
    ) -> None:
        super().__init__(external_id, target_data_set_id)
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.session_id = session_id

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Destination:
        return cls(
            external_id=resource["externalId"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            session_id=resource.get("sessionId"),
            target_data_set_id=resource.get("targetDataSetId"),
        )

    def as_write(self) -> NoReturn:
        raise TypeError(f"{self.__class__.__name__} cannot be converted to a write object")


class DestinationUpdate(CogniteUpdate):
    def __init__(self, external_id: str) -> None:
        super().__init__(external_id=external_id)

    class _CredentialsUpdate(CognitePrimitiveUpdate):
        def set(self, value: SessionWrite | None) -> DestinationUpdate:
            return self._set(value.dump(camel_case=True) if isinstance(value, SessionWrite) else value)

    class _TargetDataSetIdUpdate(CognitePrimitiveUpdate):
        def set(self, value: int | None) -> DestinationUpdate:
            return self._set(value)

    @property
    def credentials(self) -> DestinationUpdate._CredentialsUpdate:
        return self._CredentialsUpdate(self, "credentials")

    @property
    def target_data_set_id(self) -> DestinationUpdate._TargetDataSetIdUpdate:
        return self._TargetDataSetIdUpdate(self, "targetDataSetId")

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [PropertySpec("credentials", is_nullable=True), PropertySpec("target_data_set_id", is_nullable=True)]


class DestinationWriteList(CogniteResourceList[DestinationWrite], ExternalIDTransformerMixin):
    _RESOURCE = DestinationWrite


class DestinationList(WriteableCogniteResourceList[DestinationWrite, Destination], ExternalIDTransformerMixin):
    _RESOURCE = Destination

    def as_write(self) -> NoReturn:
        raise TypeError(f"{self.__class__.__name__} cannot be converted to a write object")
