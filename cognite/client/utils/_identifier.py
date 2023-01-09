from __future__ import annotations

import numbers
from typing import Dict, Generic, Iterable, List, Optional, Sequence, Tuple, TypeVar, Union, cast, overload

T_ID = TypeVar("T_ID", int, str)


class Identifier(Generic[T_ID]):
    def __init__(self, value: T_ID) -> None:
        self.__value: T_ID = value

    @classmethod
    def of_either(cls, id: Optional[int], external_id: Optional[str]) -> Identifier:
        if id is external_id is None:
            raise ValueError("Exactly one of id or external id must be specified, got neither")
        elif id is not None and external_id is not None:
            raise ValueError("Exactly one of id or external id must be specified, got both")
        return Identifier(id or external_id)

    @classmethod
    def load(cls, id: Optional[int] = None, external_id: Optional[str] = None) -> Identifier:
        if id is not None:
            return Identifier(id)
        if external_id is not None:
            return Identifier(external_id)
        raise ValueError("At least one of id and external id must be specified")

    def as_primitive(self) -> T_ID:
        return self.__value

    def as_dict(self, camel_case: bool = True) -> Dict[str, T_ID]:
        if isinstance(self.__value, str):
            if camel_case:
                return {"externalId": self.__value}
            return {"external_id": self.__value}
        else:
            return {"id": self.__value}

    def as_tuple(self, camel_case: bool = True) -> Tuple[str, T_ID]:
        if isinstance(self.__value, str):
            if camel_case:
                return ("externalId", self.__value)
            return ("external_id", self.__value)
        else:
            return ("id", self.__value)

    def __str__(self) -> str:
        return repr(self)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.__value!r})"


class ExternalId(Identifier[str]):
    ...


class InternalId(Identifier[int]):
    ...


class IdentifierSequence:
    def __init__(self, identifiers: List[Identifier], is_singleton: bool) -> None:
        if len(identifiers) == 0:
            raise ValueError("No ids or external_ids specified")
        self._identifiers = identifiers
        self.__is_singleton = is_singleton

    def __len__(self) -> int:
        return len(self._identifiers)

    def __getitem__(self, item: int) -> Identifier:
        return self._identifiers[item]

    def is_singleton(self) -> bool:
        return self.__is_singleton

    def assert_singleton(self) -> None:
        if not self.is_singleton():
            raise ValueError("Exactly one of id or external id must be specified")

    def as_singleton(self) -> SingletonIdentifierSequence:
        self.assert_singleton()
        return cast(SingletonIdentifierSequence, self)

    def chunked(self, chunk_size: int) -> Iterable[IdentifierSequence]:
        return [
            IdentifierSequence(self._identifiers[i : i + chunk_size], is_singleton=self.is_singleton())
            for i in range(0, len(self), chunk_size)
        ]

    def as_dicts(self) -> List[Dict[str, Union[int, str]]]:
        return [identifier.as_dict() for identifier in self._identifiers]

    def as_primitives(self) -> List[Union[int, str]]:
        return [identifier.as_primitive() for identifier in self._identifiers]

    @overload
    @classmethod
    def of(cls, *ids: List[Union[int, str]]) -> IdentifierSequence:
        ...

    @overload
    @classmethod
    def of(cls, *ids: Union[int, str]) -> IdentifierSequence:
        ...

    @classmethod
    def of(cls, *ids: Union[int, str, Sequence[Union[int, str]]]) -> IdentifierSequence:
        if len(ids) == 1 and isinstance(ids[0], Sequence) and not isinstance(ids[0], str):
            return cls([Identifier(val) for val in ids[0]], is_singleton=False)
        else:
            return cls([Identifier(val) for val in ids], is_singleton=len(ids) == 1)

    @classmethod
    def load(
        cls, ids: Optional[Union[int, Sequence[int]]] = None, external_ids: Optional[Union[str, Sequence[str]]] = None
    ) -> IdentifierSequence:
        value_passed_as_primitive = False
        all_identifiers: List[Union[int, str]] = []

        if ids is not None:
            if isinstance(ids, numbers.Integral):
                value_passed_as_primitive = True
                all_identifiers.append(int(ids))
            elif isinstance(ids, Sequence) and not isinstance(ids, str):
                all_identifiers.extend([int(id_) for id_ in ids])
            else:
                raise TypeError(f"ids must be of type int or Sequence[int]. Found {type(ids)}")

        if external_ids is not None:
            if isinstance(external_ids, str):
                value_passed_as_primitive = True
                all_identifiers.append(external_ids)
            elif isinstance(external_ids, Sequence):
                all_identifiers.extend([str(extid) for extid in external_ids])
            else:
                raise TypeError(f"external_ids must be of type str or Sequence[str]. Found {type(external_ids)}")

        is_singleton = value_passed_as_primitive and len(all_identifiers) == 1
        return cls(identifiers=[Identifier(val) for val in all_identifiers], is_singleton=is_singleton)


class SingletonIdentifierSequence(IdentifierSequence):
    ...
