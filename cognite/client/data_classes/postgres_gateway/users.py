from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, NoReturn

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    PropertySpec,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)

if TYPE_CHECKING:
    from cognite.client import CogniteClient


@dataclass
class SessionCredentials(CogniteObject):
    nonce: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            nonce=resource["nonce"],
        )


class _UserCore(WriteableCogniteResource["UserWrite"], ABC): ...


class UserWrite(_UserCore):
    """A postgres gateway **user** (also a typical postgres user) owns the foreign tables (built in or custom).

    The created postgres user only has access to use foreign tables and cannot directly create tables users. To create
    foreign tables use the Postgres Gateway Tables APIs

    This is the write/request format of the user.

    Args:
        credentials (SessionCredentials | None): Credentials for authenticating towards CDF using a CDF session.

    """

    def __init__(self, credentials: SessionCredentials | None = None) -> None:
        self.credentials = credentials

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            credentials=SessionCredentials._load(resource["credentials"], cognite_client)
            if "credentials" in resource
            else None,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case=camel_case)
        if isinstance(self.credentials, SessionCredentials):
            output["credentials"] = self.credentials.dump(camel_case=camel_case)

        return output

    def as_write(self) -> UserWrite:
        return self


class User(_UserCore):
    """A user.

    This is the read/response format of the user.

    Args:
        username (str): Username to authenticate the user on the DB.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        session_id (int): ID of the session tied to this user.

    """

    def __init__(self, username: str, created_time: int, last_updated_time: int, session_id: int) -> None:
        self.username = username
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.session_id = session_id

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            username=resource["username"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            session_id=resource["sessionId"],
        )

    def as_write(self) -> NoReturn:
        raise TypeError(f"{type(self).__name__} cannot be converted to a write object")


class UserUpdate(CogniteUpdate):
    def __init__(
        self,
        username: str,
    ) -> None:
        super().__init__()
        self.username = username

    class _UpdateItemSessionCredentialsUpdate(CognitePrimitiveUpdate):
        def set(self, value: SessionCredentials | None) -> UserUpdate:
            return self._set(value.dump() if isinstance(value, SessionCredentials) else value)

    @property
    def credentials(self) -> UserUpdate._UpdateItemSessionCredentialsUpdate:
        return self._UpdateItemSessionCredentialsUpdate(self, "credentials")

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            PropertySpec("credentials", is_nullable=True),
        ]

    def dump(self, camel_case: Literal[True] = True) -> dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (Literal[True]): No description.
        Returns:
            dict[str, Any]: A dictionary representation of the instance.
        """
        return {"update": self._update_object, "username": self.username}


class UserWriteList(CogniteResourceList[UserWrite]):
    _RESOURCE = UserWrite


class UserList(WriteableCogniteResourceList[UserWrite, User]):
    _RESOURCE = User

    def as_write(self) -> NoReturn:
        raise TypeError(f"{type(self).__name__} cannot be converted to a write object")
