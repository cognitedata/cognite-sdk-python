from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, cast

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResource,
    CogniteResourceList,
)

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class Principal(CogniteResource, ABC):
    _type: ClassVar[str]

    def __init__(self, id: str) -> None:
        self.id = id

    @classmethod
    @abstractmethod
    def _load_principal(cls, resource: dict[str, Any]) -> Self:
        """Load a principal from a resource dictionary."""
        raise NotImplementedError("This method should be implemented in subclasses.")

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        type_ = resource.get("type")
        if type_ is None and hasattr(cls, "_type"):
            type_ = cls._type
        elif type_ is None:
            raise KeyError("type")
        principal_cls = _PRINCIPAL_CLS_BY_TYPE.get(type_.lower())
        if principal_cls is None:
            return UnknownPrincipal._load_principal(resource)  # type: ignore[return-value]
        return cast(
            Self,
            principal_cls._load_principal(
                resource,
            ),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the principal to a dictionary."""
        output = super().dump(camel_case=camel_case)
        output["type"] = self._type.upper()
        return output


class UserPrincipal(Principal):
    _type = "user"

    def __init__(
        self,
        id: str,
        name: str,
        picture_url: str,
        email: str | None = None,
        given_name: str | None = None,
        middle_name: str | None = None,
        family_name: str | None = None,
    ) -> None:
        super().__init__(id)
        self.name = name
        self.picture_url = picture_url
        self.email = email
        self.given_name = given_name
        self.middle_name = middle_name
        self.family_name = family_name

    @classmethod
    def _load_principal(cls, resource: dict[str, Any]) -> Self:
        return cls(
            id=resource["id"],
            name=resource["name"],
            picture_url=resource["pictureUrl"],
            email=resource.get("email"),
            given_name=resource.get("givenName"),
            middle_name=resource.get("middleName"),
            family_name=resource.get("familyName"),
        )


@dataclass
class ServiceAccountCreator(CogniteObject):
    org_id: str
    user_id: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(org_id=resource["orgId"], user_id=resource["userId"])


class ServicePrincipal(Principal):
    _type = "service_account"

    def __init__(
        self,
        id: str,
        name: str,
        created_by: ServiceAccountCreator,
        created_time: int,
        last_updated_time: int,
        picture_url: str,
        external_id: str | None = None,
        description: str | None = None,
    ) -> None:
        super().__init__(id)
        self.name = name
        self.created_by = created_by
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.picture_url = picture_url
        self.external_id = external_id
        self.description = description

    @classmethod
    def _load_principal(cls, resource: dict[str, Any]) -> Self:
        return cls(
            id=resource["id"],
            name=resource["name"],
            created_by=ServiceAccountCreator._load(resource["createdBy"]),
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            picture_url=resource["pictureUrl"],
            external_id=resource.get("externalId"),
            description=resource.get("description"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump()
        output["createdBy" if camel_case else "created_by"] = self.created_by.dump(camel_case=camel_case)
        return output


class UnknownPrincipal(Principal):
    _type = "unknown"

    def __init__(self, id: str, type: str, data: dict[str, Any]) -> None:
        super().__init__(id)
        self.type = type
        self.__data = data

    @classmethod
    def _load_principal(cls, resource: dict[str, Any]) -> Self:
        return cls(
            id=resource["id"],
            type=resource["type"],
            data={k: v for k, v in resource.items() if k not in {"id", "type"}},
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = {
            "id": self.id,
            "type": self.type,
        }
        output.update(self.__data)
        return output


class PrincipalList(CogniteResourceList[Principal]):
    _RESOURCE = Principal

    def as_ids(self) -> list[str]:
        """Returns a list of principal IDs."""
        return [principal.id for principal in self]


# Build the mapping AFTER all classes are defined
_PRINCIPAL_CLS_BY_TYPE: dict[str, type[Principal]] = {
    subclass._type: subclass  # type: ignore[type-abstract]
    for subclass in Principal.__subclasses__()
    if hasattr(subclass, "_type") and not getattr(subclass, "__abstractmethods__", None)
}
