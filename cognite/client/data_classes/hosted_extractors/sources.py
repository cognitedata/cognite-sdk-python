from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from typing_extensions import Self, TypeAlias

from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResource,
    CogniteResourceList,
    ExternalIDTransformerMixin,
    T_WriteClass,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)

if TYPE_CHECKING:
    from cognite.client import CogniteClient


SourceType: TypeAlias = Literal["mqtt5", "mqtt3", "eventhub"]


class SourceWrite(CogniteResource, ABC):
    """A hosted extractor source represents an external source system on the internet.
    The source resource in CDF contains all the information the extractor needs to
    connect to the external source system.

    This is the write/request format of the source resource.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
    """

    _type: ClassVar[str]

    def __init__(self, external_id: str) -> None:
        self.external_id = external_id


class Source(WriteableCogniteResource[T_WriteClass], ABC):
    """A hosted extractor source represents an external source system on the internet.
    The source resource in CDF contains all the information the extractor needs to
    connect to the external source system.

    This is the read/response format of the source resource.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
    """

    _type: ClassVar[str]

    def __init__(self, external_id: str) -> None:
        self.external_id = external_id


class EventHubSourceWrite(SourceWrite):
    """A hosted extractor source represents an external source system on the internet.
    The source resource in CDF contains all the information the extractor needs to
    connect to the external source system.

    This is the write/request format of the source resource.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        host (str): URL of the event hub consumer endpoint.
        event_hub_name (str): Name of the event hub
        key_name (str): The name of the Event Hub key to use.
        key_value (str): Value of the Event Hub key to use for authentication.
        consumer_group (str | None): The event hub consumer group to use. Microsoft recommends having a distinct consumer group for each application consuming data from event hub. If left out, this uses the default consumer group.
    """

    def __init__(
        self,
        external_id: str,
        host: str,
        event_hub_name: str,
        key_name: str,
        key_value: str,
        consumer_group: str | None = None,
    ) -> None:
        super().__init__(external_id)
        self.host = host
        self.event_hub_name = event_hub_name
        self.key_name = key_name
        self.key_value = key_value
        self.consumer_group = consumer_group

    def as_write(self) -> SourceWrite:
        return self


class EventHubSource(Source):
    """A hosted extractor source represents an external source system on the internet.
    The source resource in CDF contains all the information the extractor needs to
    connect to the external source system.

    This is the read/response format of the source resource.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        host (str): URL of the event hub consumer endpoint.
        event_hub_name (str): Name of the event hub
        key_name (str): The name of the Event Hub key to use.
        created_time (int): No description.
        last_updated_time (int): No description.
        consumer_group (str | None): The event hub consumer group to use. Microsoft recommends having a distinct consumer group for each application consuming data from event hub. If left out, this uses the default consumer group.
    """

    def __init__(
        self,
        external_id: str,
        host: str,
        event_hub_name: str,
        key_name: str,
        created_time: int,
        last_updated_time: int,
        consumer_group: str | None = None,
    ) -> None:
        super().__init__(external_id)
        self.host = host
        self.event_hub_name = event_hub_name
        self.key_name = key_name
        self.consumer_group = consumer_group
        self.created_time = created_time
        self.last_updated_time = last_updated_time

    def as_write(self, key_value: str | None = None) -> EventHubSourceWrite:
        if key_value is None:
            raise ValueError("key_value must be provided")
        return EventHubSourceWrite(
            external_id=self.external_id,
            host=self.host,
            event_hub_name=self.event_hub_name,
            key_name=self.key_name,
            key_value=key_value,
            consumer_group=self.consumer_group,
        )


@dataclass
class MQTTAuthenticationWrite(CogniteObject, ABC):
    _type: ClassVar[str]


@dataclass
class BasicMQTTAuthenticationWrite(MQTTAuthenticationWrite):
    username: str
    password: str | None


@dataclass
class CACertificateWrite(CogniteObject):
    type: Literal["der", "pem"]
    certificate: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(type=resource["type"], certificate=resource["certificate"])


@dataclass
class AuthCertificateWrite(CogniteObject):
    type: Literal["der", "pem"]
    certificate: str
    key: str
    key_password: str | None

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            type=resource["type"],
            certificate=resource["certificate"],
            key=resource["key"],
            key_password=resource.get("keyPassword"),
        )


class MQTTSourceWrite(SourceWrite):
    def __init__(
        self,
        external_id: str,
        host: str,
        port: int | None = None,
        authentication: MQTTAuthenticationWrite | None = None,
        useTls: bool = False,
        ca_certificate: CACertificateWrite | None = None,
        auth_certificate: AuthCertificateWrite | None = None,
    ) -> None:
        super().__init__(external_id)
        self.host = host
        self.port = port
        self.authentication = authentication
        self.useTls = useTls
        self.ca_certificate = ca_certificate
        self.auth_certificate = auth_certificate

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            host=resource["host"],
            port=resource.get("port"),
            authentication=MQTTAuthenticationWrite._load(resource["authentication"])
            if "authentication" in resource
            else None,
            useTls=resource.get("useTls", False),
            ca_certificate=CACertificateWrite._load(resource["caCertificate"]) if "caCertificate" in resource else None,
            auth_certificate=AuthCertificateWrite._load(resource["authCertificate"])
            if "authCertificate" in resource
            else None,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if self.authentication:
            output["authentication"] = self.authentication.dump(camel_case)
        if self.ca_certificate:
            output["caCertificate" if camel_case else "ca_certificate"] = self.ca_certificate.dump(camel_case)
        if self.auth_certificate:
            output["authCertificate" if camel_case else "auth_certificate"] = self.auth_certificate.dump(camel_case)
        return output


@dataclass
class MQTTAuthentication(CogniteObject, ABC):
    _type: ClassVar[str]


@dataclass
class BasicMQTTAuthentication(MQTTAuthentication):
    username: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(username=resource["username"])


@dataclass
class CACertificate(CogniteObject):
    thumbprint: str
    expires_at: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(thumbprint=resource["thumbprint"], expires_at=resource["expiresAt"])


@dataclass
class AuthCertificate(CogniteObject):
    thumbprint: str
    expires_at: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(thumbprint=resource["thumbprint"], expires_at=resource["expiresAt"])


class MQTTSource(Source):
    def __init__(
        self,
        external_id: str,
        host: str,
        created_time: int,
        last_updated_time: int,
        port: int | None = None,
        authentication: MQTTAuthentication | None = None,
        useTls: bool = False,
        ca_certificate: CACertificate | None = None,
        auth_certificate: AuthCertificate | None = None,
    ) -> None:
        super().__init__(external_id)
        self.host = host
        self.port = port
        self.authentication = authentication
        self.useTls = useTls
        self.ca_certificate = ca_certificate
        self.auth_certificate = auth_certificate
        self.created_time = created_time
        self.last_updated_time = last_updated_time

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            host=resource["host"],
            port=resource.get("port"),
            authentication=MQTTAuthentication._load(resource["authentication"])
            if "authentication" in resource
            else None,
            useTls=resource.get("useTls", False),
            ca_certificate=CACertificate._load(resource["caCertificate"]) if "caCertificate" in resource else None,
            auth_certificate=AuthCertificate._load(resource["authCertificate"])
            if "authCertificate" in resource
            else None,
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
        )

    def as_write(self) -> MQTTSourceWrite:
        raise TypeError(f"{type(self).__name__} cannot be converted to write as id does not contain the secrets")

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if self.authentication:
            output["authentication"] = self.authentication.dump(camel_case)
        if self.ca_certificate:
            output["caCertificate" if camel_case else "ca_certificate"] = self.ca_certificate.dump(camel_case)
        if self.auth_certificate:
            output["authCertificate" if camel_case else "auth_certificate"] = self.auth_certificate.dump(camel_case)
        return output


class SourceWriteList(CogniteResourceList[SourceWrite], ExternalIDTransformerMixin):
    _RESOURCE = SourceWrite


class SourceList(WriteableCogniteResourceList[SourceWrite, Source], ExternalIDTransformerMixin):
    _RESOURCE = Source

    def as_write(
        self,
    ) -> SourceWriteList:
        raise TypeError(f"{type(self).__name__} cannot be converted to write")
