from __future__ import annotations

import numbers
from abc import ABC
from typing import (
    Generic,
    Literal,
    NoReturn,
    Protocol,
    Sequence,
    TypeVar,
    cast,
    overload,
)

from cognite.client._constants import MAX_VALID_INTERNAL_ID
from cognite.client.utils._auxiliary import split_into_chunks

T_ID = TypeVar("T_ID", int, str)


class IdentifierCore(Protocol):
    def as_dict(self, camel_case: bool = True) -> dict:
        ...

    def as_primitive(self) -> str | int:
        ...


class Identifier(Generic[T_ID]):
    def __init__(self, value: T_ID) -> None:
        self.__value: T_ID = value

    @classmethod
    def of_either(cls, id: int | None, external_id: str | None) -> Identifier:
        if id is external_id is None:
            raise ValueError("Exactly one of id or external id must be specified, got neither")
        elif id is not None:
            if external_id is not None:
                raise ValueError("Exactly one of id or external id must be specified, got both")
            elif not 1 <= id <= MAX_VALID_INTERNAL_ID:
                raise ValueError(f"Invalid id, must satisfy: 1 <= id <= {MAX_VALID_INTERNAL_ID}")
        return Identifier(id or external_id)

    @classmethod
    def load(cls, id: int | None = None, external_id: str | None = None) -> Identifier:
        if id is not None:
            return Identifier(id)
        if external_id is not None:
            return Identifier(external_id)
        raise ValueError("At least one of id and external id must be specified")

    def name(self, camel_case: bool = False) -> str:
        if self.is_id:
            return "id"
        return "externalId" if camel_case else "external_id"

    def as_primitive(self) -> T_ID:
        return self.__value

    @property
    def is_id(self) -> bool:
        return isinstance(self.__value, int)

    @property
    def is_external_id(self) -> bool:
        return isinstance(self.__value, str)

    def as_dict(self, camel_case: bool = True) -> dict[str, T_ID]:
        return {self.name(camel_case): self.__value}

    def as_tuple(self, camel_case: bool = True) -> tuple[str, T_ID]:
        return self.name(camel_case), self.__value


class UserIdentifier:
    def __init__(self, value: str) -> None:
        self.__value: str = value

    def name(self, camel_case: bool = False) -> str:
        return "userIdentifier" if camel_case else "user_identifier"

    def as_dict(self, camel_case: bool = True) -> dict[str, str]:
        return {self.name(camel_case): self.__value}

    def as_primitive(self) -> str:
        return self.__value


class WorkflowVersionIdentifier:
    def __init__(self, version: str, workflow_external_id: str) -> None:
        self.__version: str = version
        self.__workflow_external_id: str = workflow_external_id

    def as_dict(self, camel_case: bool = True) -> dict[str, str]:
        return {
            "version": self.__version,
            ("workflowExternalId" if camel_case else "workflow_external_id"): self.__workflow_external_id,
        }

    def as_primitive(self) -> NoReturn:
        raise AttributeError(f"Not supported for {type(self).__name__} implementation")


class DataModelingIdentifier:
    def __init__(
        self,
        space: str,
        external_id: str | None = None,
        version: str | None = None,
        instance_type: Literal["node", "edge"] | None = None,
    ):
        self.__space = space
        self.__external_id = external_id
        self.__version = version
        self.__instance_type = instance_type

    def as_dict(self, camel_case: bool = True) -> dict[str, str]:
        output = {"space": self.__space}
        if self.__external_id is not None:
            key = "externalId" if camel_case else "external_id"
            output[key] = self.__external_id
        if self.__version is not None:
            output["version"] = self.__version
        if self.__instance_type is not None:
            output["instanceType"] = self.__instance_type
        return output

    def as_primitive(self) -> NoReturn:
        raise AttributeError(f"Not supported for {type(self).__name__} implementation")


class ExternalId(Identifier[str]):
    ...


class InternalId(Identifier[int]):
    ...


T_Identifier = TypeVar("T_Identifier", bound=IdentifierCore)


class IdentifierSequenceCore(Generic[T_Identifier], ABC):
    def __init__(self, identifiers: list[T_Identifier], is_singleton: bool) -> None:
        if not identifiers:
            raise ValueError("No identifiers specified")
        self._identifiers = identifiers
        self.__is_singleton = is_singleton

    def __len__(self) -> int:
        return len(self._identifiers)

    def __getitem__(self, item: int) -> T_Identifier:
        return self._identifiers[item]

    def is_singleton(self) -> bool:
        return self.__is_singleton

    def assert_singleton(self) -> None:
        if not self.is_singleton():
            raise ValueError("Exactly one of id or external id must be specified")

    def as_singleton(self) -> SingletonIdentifierSequence:
        self.assert_singleton()
        return cast(SingletonIdentifierSequence, self)

    def chunked(self: T_IdentifierSequenceCore, chunk_size: int) -> list[T_IdentifierSequenceCore]:
        return [
            type(self)(chunk, is_singleton=self.is_singleton())
            for chunk in split_into_chunks(self._identifiers, chunk_size)
        ]

    def as_dicts(self) -> list[dict[str, int | str]]:
        return [identifier.as_dict() for identifier in self._identifiers]

    def as_primitives(self) -> list[int | str]:
        return [identifier.as_primitive() for identifier in self._identifiers]

    def are_unique(self) -> bool:
        return len(self) == len(set(self.as_primitives()))

    @staticmethod
    def unwrap_identifier(identifier: str | int | dict) -> str | int:
        if isinstance(identifier, (str, int)):
            return identifier
        if "externalId" in identifier:
            return identifier["externalId"]
        if "id" in identifier:
            return identifier["id"]
        if "space" in identifier:
            return identifier["space"]
        raise ValueError(f"{identifier} does not contain 'id' or 'externalId' or 'space'")


T_IdentifierSequenceCore = TypeVar("T_IdentifierSequenceCore", bound=IdentifierSequenceCore)


class IdentifierSequence(IdentifierSequenceCore[Identifier]):
    @overload
    @classmethod
    def of(cls, *ids: list[int | str]) -> IdentifierSequence:
        ...

    @overload
    @classmethod
    def of(cls, *ids: int | str) -> IdentifierSequence:
        ...

    @classmethod
    def of(cls, *ids: int | str | Sequence[int | str]) -> IdentifierSequence:
        if len(ids) == 1 and isinstance(ids[0], Sequence) and not isinstance(ids[0], str):
            return cls([Identifier(val) for val in ids[0]], is_singleton=False)
        else:
            return cls([Identifier(val) for val in ids], is_singleton=len(ids) == 1)

    @classmethod
    def load(
        cls,
        ids: int | Sequence[int] | None = None,
        external_ids: str | Sequence[str] | None = None,
        *,
        id_name: str = "",
    ) -> IdentifierSequence:
        if id_name and not id_name.endswith("_"):
            id_name += "_"
        value_passed_as_primitive = False
        all_identifiers: list[int | str] = []

        if ids is not None:
            if isinstance(ids, numbers.Integral):
                value_passed_as_primitive = True
                all_identifiers.append(int(ids))
            elif isinstance(ids, Sequence) and not isinstance(ids, str):
                all_identifiers.extend([int(id_) for id_ in ids])
            else:
                raise TypeError(f"{id_name}ids must be of type int or Sequence[int]. Found {type(ids)}")

        if external_ids is not None:
            if isinstance(external_ids, str):
                value_passed_as_primitive = True
                all_identifiers.append(external_ids)
            elif isinstance(external_ids, Sequence):
                all_identifiers.extend([str(extid) for extid in external_ids])
            else:
                raise TypeError(
                    f"{id_name}external_ids must be of type str or Sequence[str]. Found {type(external_ids)}"
                )

        is_singleton = value_passed_as_primitive and len(all_identifiers) == 1
        return cls(identifiers=[Identifier(val) for val in all_identifiers], is_singleton=is_singleton)


class SingletonIdentifierSequence(IdentifierSequenceCore[Identifier]):
    ...


class DataModelingIdentifierSequence(IdentifierSequenceCore[DataModelingIdentifier]):
    ...


class UserIdentifierSequence(IdentifierSequenceCore[UserIdentifier]):
    # TODO: Inferred type from inherited methods 'as_dicts' and 'as_primitives' wrongly include 'int'
    @classmethod
    def load(cls, user_identifiers: str | Sequence[str]) -> UserIdentifierSequence:
        if isinstance(user_identifiers, str):
            return cls([UserIdentifier(user_identifiers)], is_singleton=True)

        elif isinstance(user_identifiers, Sequence):
            return cls(list(map(UserIdentifier, map(str, user_identifiers))), is_singleton=False)

        raise TypeError(f"user_identifiers must be of type str or Sequence[str]. Found {type(user_identifiers)}")

    def assert_singleton(self) -> None:
        if not self.is_singleton():
            raise ValueError("Exactly one user identifier (string) must be specified")


class WorkflowVersionIdentifierSequence(IdentifierSequenceCore[WorkflowVersionIdentifier]):
    @classmethod
    def load(cls, workflow_ids: Sequence[dict]) -> WorkflowVersionIdentifierSequence:
        if len(workflow_ids) == 1 and isinstance(workflow_ids[0], dict):
            return cls(
                [
                    WorkflowVersionIdentifier(
                        version=workflow_ids[0]["version"], workflow_external_id=workflow_ids[0]["workflowExternalId"]
                    )
                ],
                is_singleton=True,
            )
        elif isinstance(workflow_ids, Sequence) and workflow_ids and isinstance(workflow_ids[0], dict):
            return cls(
                [WorkflowVersionIdentifier(entry["version"], entry["workflowExternalId"]) for entry in workflow_ids],
                is_singleton=False,
            )
        raise TypeError(f"WorkflowIdentifier must be of type str or Sequence[str]. Found {type(workflow_ids)}")

    def assert_singleton(self) -> None:
        if not self.is_singleton():
            raise ValueError("Exactly one workflow version must be specified")

    @staticmethod
    def unwrap_identifier(identifier: str | dict) -> str | tuple[str, str]:  # type: ignore[override]
        if isinstance(identifier, str):
            return identifier
        if "workflowExternalId" in identifier and "version" in identifier:
            return identifier["workflowExternalId"], identifier["version"]
        raise ValueError(f"{identifier} does not contain both 'workflowExternalId' and 'version''")
