from __future__ import annotations

import itertools
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NoReturn, cast

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    ExternalIDTransformerMixin,
    PropertySpec,
    T_WriteClass,
    UnknownCogniteObject,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)

if TYPE_CHECKING:
    from cognite.client import CogniteClient


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

    @classmethod
    @abstractmethod
    def _load_source(cls, resource: dict[str, Any]) -> Self:
        raise NotImplementedError()

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        type_ = resource.get("type")
        if type_ is None and hasattr(cls, "_type"):
            type_ = cls._type
        elif type_ is None:
            raise KeyError("type")
        try:
            return cast(Self, _SOURCE_WRITE_CLASS_BY_TYPE[type_]._load_source(resource))
        except KeyError:
            raise TypeError(f"Unknown source type: {type_}")

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        output["type"] = self._type
        return output


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

    @classmethod
    @abstractmethod
    def _load_source(cls, resource: dict[str, Any]) -> Self:
        raise NotImplementedError()

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        type_ = resource.get("type")
        if type_ is None and hasattr(cls, "_type"):
            type_ = cls._type
        elif type_ is None:
            raise KeyError("type")
        source_class = _SOURCE_CLASS_BY_TYPE.get(type_)
        if source_class is None:
            return UnknownCogniteObject(resource)  # type: ignore[return-value]
        return cast(Self, source_class._load_source(resource))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        output["type"] = self._type
        return output


class SourceUpdate(CogniteUpdate, ABC):
    _type: ClassVar[str]

    def __init__(self, external_id: str) -> None:
        super().__init__(external_id=external_id)

    def dump(self, camel_case: Literal[True] = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        output["type"] = self._type
        return output

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        if item is None or not isinstance(item, SourceWrite):
            return []
        return _SOURCE_UPDATE_BY_TYPE[item._type]._get_update_properties(item)


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

    _type = "eventhub"

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

    @classmethod
    def _load_source(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource["externalId"],
            host=resource["host"],
            event_hub_name=resource["eventHubName"],
            key_name=resource["keyName"],
            key_value=resource["keyValue"],
            consumer_group=resource.get("consumerGroup"),
        )


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

    _type = "eventhub"

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

    def as_write(self) -> NoReturn:
        raise TypeError(f"{type(self).__name__} cannot be converted to write as id does not contain the secrets")

    @classmethod
    def _load_source(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource["externalId"],
            host=resource["host"],
            event_hub_name=resource["eventHubName"],
            key_name=resource["keyName"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            consumer_group=resource.get("consumerGroup"),
        )


class EventHubSourceUpdate(SourceUpdate):
    _type = "eventhub"

    class _PrimitiveEventHubSourceUpdate(CognitePrimitiveUpdate):
        def set(self, value: str) -> EventHubSourceUpdate:
            return self._set(value)

    class _PrimitiveNullableEventHubSourceUpdate(CognitePrimitiveUpdate):
        def set(self, value: str | None) -> EventHubSourceUpdate:
            return self._set(value)

    @property
    def host(self) -> _PrimitiveEventHubSourceUpdate:
        return EventHubSourceUpdate._PrimitiveEventHubSourceUpdate(self, "host")

    @property
    def event_hub_name(self) -> _PrimitiveEventHubSourceUpdate:
        return EventHubSourceUpdate._PrimitiveEventHubSourceUpdate(self, "eventHubName")

    @property
    def key_name(self) -> _PrimitiveEventHubSourceUpdate:
        return EventHubSourceUpdate._PrimitiveEventHubSourceUpdate(self, "keyName")

    @property
    def key_value(self) -> _PrimitiveEventHubSourceUpdate:
        return EventHubSourceUpdate._PrimitiveEventHubSourceUpdate(self, "keyValue")

    @property
    def consumer_group(self) -> _PrimitiveNullableEventHubSourceUpdate:
        return EventHubSourceUpdate._PrimitiveNullableEventHubSourceUpdate(self, "consumerGroup")

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            PropertySpec("host", is_nullable=False),
            PropertySpec("event_hub_name", is_nullable=False),
            PropertySpec("key_name", is_nullable=False),
            PropertySpec("key_value", is_nullable=False),
            PropertySpec("consumer_group", is_nullable=True),
        ]


@dataclass
class MQTTAuthenticationWrite(CogniteObject, ABC):
    _type: ClassVar[str]

    @classmethod
    @abstractmethod
    def _load_authentication(cls, resource: dict[str, Any]) -> Self:
        raise NotImplementedError()

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        type_ = resource.get("type")
        if type_ is None and hasattr(cls, "_type"):
            type_ = cls._type
        elif type_ is None:
            raise KeyError("type is required")
        try:
            return cast(Self, _MQTTAUTHENTICATION_WRITE_CLASS_BY_TYPE[type_]._load_authentication(resource))
        except KeyError:
            raise TypeError(f"Unknown authentication type: {type_}")

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        output["type"] = self._type
        return output


@dataclass
class BasicMQTTAuthenticationWrite(MQTTAuthenticationWrite):
    _type = "basic"
    username: str
    password: str | None

    @classmethod
    def _load_authentication(cls, resource: dict[str, Any]) -> Self:
        return cls(
            username=resource["username"],
            password=resource.get("password"),
        )


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


class _MQTTSourceWrite(SourceWrite, ABC):
    def __init__(
        self,
        external_id: str,
        host: str,
        port: int | None = None,
        authentication: MQTTAuthenticationWrite | None = None,
        use_tls: bool = False,
        ca_certificate: CACertificateWrite | None = None,
        auth_certificate: AuthCertificateWrite | None = None,
    ) -> None:
        super().__init__(external_id)
        self.host = host
        self.port = port
        self.authentication = authentication
        self.use_tls = use_tls
        self.ca_certificate = ca_certificate
        self.auth_certificate = auth_certificate

    @classmethod
    def _load_source(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource["externalId"],
            host=resource["host"],
            port=resource.get("port"),
            authentication=MQTTAuthenticationWrite._load(resource["authentication"])
            if "authentication" in resource
            else None,
            use_tls=resource.get("useTls", False),
            ca_certificate=CACertificateWrite._load(resource["caCertificate"]) if "caCertificate" in resource else None,
            auth_certificate=AuthCertificateWrite._load(resource["authCertificate"])
            if "authCertificate" in resource
            else None,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if isinstance(self.authentication, MQTTAuthenticationWrite):
            output["authentication"] = self.authentication.dump(camel_case)
        if isinstance(self.ca_certificate, CACertificateWrite):
            output["caCertificate" if camel_case else "ca_certificate"] = self.ca_certificate.dump(camel_case)
        if isinstance(self.auth_certificate, AuthCertificateWrite):
            output["authCertificate" if camel_case else "auth_certificate"] = self.auth_certificate.dump(camel_case)
        return output


@dataclass
class MQTTAuthentication(CogniteObject, ABC):
    _type: ClassVar[str]

    @classmethod
    @abstractmethod
    def _load_authentication(cls, resource: dict[str, Any]) -> Self:
        raise NotImplementedError()

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        type_ = resource.get("type")
        if type_ is None and hasattr(cls, "_type"):
            type_ = cls._type
        elif type_ is None:
            raise KeyError("type")

        authentication_class = _MQTTAUTHENTICATION_CLASS_BY_TYPE.get(type_)
        if authentication_class is None:
            return UnknownCogniteObject(resource)  # type: ignore[return-value]
        return cast(Self, authentication_class._load_authentication(resource))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        output["type"] = self._type
        return output


@dataclass
class BasicMQTTAuthentication(MQTTAuthentication):
    _type = "basic"
    username: str

    @classmethod
    def _load_authentication(cls, resource: dict[str, Any]) -> Self:
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


class _MQTTSource(Source, ABC):
    def __init__(
        self,
        external_id: str,
        host: str,
        created_time: int,
        last_updated_time: int,
        port: int | None = None,
        authentication: MQTTAuthentication | None = None,
        use_tls: bool = False,
        ca_certificate: CACertificate | None = None,
        auth_certificate: AuthCertificate | None = None,
    ) -> None:
        super().__init__(external_id)
        self.host = host
        self.port = port
        self.authentication = authentication
        self.use_tls = use_tls
        self.ca_certificate = ca_certificate
        self.auth_certificate = auth_certificate
        self.created_time = created_time
        self.last_updated_time = last_updated_time

    @classmethod
    def _load_source(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource["externalId"],
            host=resource["host"],
            port=resource.get("port"),
            authentication=MQTTAuthentication._load(resource["authentication"])
            if "authentication" in resource
            else None,
            use_tls=resource.get("useTls", False),
            ca_certificate=CACertificate._load(resource["caCertificate"]) if "caCertificate" in resource else None,
            auth_certificate=AuthCertificate._load(resource["authCertificate"])
            if "authCertificate" in resource
            else None,
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
        )

    def as_write(self) -> NoReturn:
        raise TypeError(f"{type(self).__name__} cannot be converted to write as id does not contain the secrets")

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        if isinstance(self.authentication, MQTTAuthentication):
            output["authentication"] = self.authentication.dump(camel_case)
        if isinstance(self.ca_certificate, CACertificate):
            output["caCertificate" if camel_case else "ca_certificate"] = self.ca_certificate.dump(camel_case)
        if isinstance(self.auth_certificate, AuthCertificate):
            output["authCertificate" if camel_case else "auth_certificate"] = self.auth_certificate.dump(camel_case)
        return output


class _MQTTUpdate(SourceUpdate, ABC):
    class _HostUpdate(CognitePrimitiveUpdate):
        def set(self, value: str) -> _MQTTUpdate:
            return self._set(value)

    class _PortUpdate(CognitePrimitiveUpdate):
        def set(self, value: int | None) -> _MQTTUpdate:
            return self._set(value)

    class _AuthenticationUpdate(CognitePrimitiveUpdate):
        def set(self, value: MQTTAuthentication | None) -> _MQTTUpdate:
            return self._set(value.dump() if value else None)

    class _UseTlsUpdate(CognitePrimitiveUpdate):
        def set(self, value: bool) -> _MQTTUpdate:
            return self._set(value)

    class _CACertificateUpdate(CognitePrimitiveUpdate):
        def set(self, value: CACertificate | None) -> _MQTTUpdate:
            return self._set(value.dump() if value else None)

    class _AuthCertificateUpdate(CognitePrimitiveUpdate):
        def set(self, value: AuthCertificate | None) -> _MQTTUpdate:
            return self._set(value.dump() if value else None)

    @property
    def host(self) -> _HostUpdate:
        return _MQTTUpdate._HostUpdate(self, "host")

    @property
    def port(self) -> _PortUpdate:
        return _MQTTUpdate._PortUpdate(self, "port")

    @property
    def authentication(self) -> _AuthenticationUpdate:
        return _MQTTUpdate._AuthenticationUpdate(self, "authentication")

    @property
    def useTls(self) -> _UseTlsUpdate:
        return _MQTTUpdate._UseTlsUpdate(self, "useTls")

    @property
    def ca_certificate(self) -> _CACertificateUpdate:
        return _MQTTUpdate._CACertificateUpdate(self, "caCertificate")

    @property
    def auth_certificate(self) -> _AuthCertificateUpdate:
        return _MQTTUpdate._AuthCertificateUpdate(self, "authCertificate")

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            PropertySpec("host", is_nullable=False),
            PropertySpec("port", is_nullable=True),
            PropertySpec("authentication", is_nullable=True, is_object=True),
            PropertySpec("useTls", is_nullable=False),
            PropertySpec("ca_certificate", is_nullable=True, is_object=True),
            PropertySpec("auth_certificate", is_nullable=True, is_object=True),
        ]


class MQTT3SourceWrite(_MQTTSourceWrite):
    _type = "mqtt3"


class MQTT5SourceWrite(_MQTTSourceWrite):
    _type = "mqtt5"


class MQTT3Source(_MQTTSource):
    _type = "mqtt3"


class MQTT5Source(_MQTTSource):
    _type = "mqtt5"


class MQTT3SourceUpdate(_MQTTUpdate):
    _type = "mqtt3"


class MQTT5SourceUpdate(_MQTTUpdate):
    _type = "mqtt5"


class SourceWriteList(CogniteResourceList[SourceWrite], ExternalIDTransformerMixin):
    _RESOURCE = SourceWrite


class SourceList(WriteableCogniteResourceList[SourceWrite, Source], ExternalIDTransformerMixin):
    _RESOURCE = Source

    def as_write(
        self,
    ) -> NoReturn:
        raise TypeError(f"{type(self).__name__} cannot be converted to write")


_SOURCE_WRITE_CLASS_BY_TYPE: dict[str, type[SourceWrite]] = {
    subclass._type: subclass  # type: ignore[misc]
    for subclass in itertools.chain(SourceWrite.__subclasses__(), _MQTTSourceWrite.__subclasses__())
    if hasattr(subclass, "_type")
}

_SOURCE_CLASS_BY_TYPE: dict[str, type[Source]] = {
    subclass._type: subclass  # type: ignore[misc]
    for subclass in itertools.chain(Source.__subclasses__(), _MQTTSource.__subclasses__())
    if hasattr(subclass, "_type")
}

_SOURCE_UPDATE_BY_TYPE: dict[str, type[SourceUpdate]] = {
    subclass._type: subclass
    for subclass in itertools.chain(SourceUpdate.__subclasses__(), _MQTTUpdate.__subclasses__())
    if hasattr(subclass, "_type")
}

_MQTTAUTHENTICATION_WRITE_CLASS_BY_TYPE: dict[str, type[MQTTAuthenticationWrite]] = {
    subclass._type: subclass  # type: ignore[type-abstract]
    for subclass in MQTTAuthenticationWrite.__subclasses__()
    if hasattr(subclass, "_type")
}

_MQTTAUTHENTICATION_CLASS_BY_TYPE: dict[str, type[MQTTAuthentication]] = {
    subclass._type: subclass  # type: ignore[type-abstract]
    for subclass in MQTTAuthentication.__subclasses__()
    if hasattr(subclass, "_type")
}
