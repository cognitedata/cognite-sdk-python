from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Any, Protocol, SupportsIndex, TypeGuard, TypeVar, overload, runtime_checkable

_T_co = TypeVar("_T_co", covariant=True)


# Source from https://github.com/python/typing/issues/256#issuecomment-1442633430
# This works because str.__contains__ does not accept object (either in typeshed or at runtime)
@runtime_checkable  # TODO: remove; does not accepts tuple, change usage to 'is_sequence_not_str' below
class SequenceNotStr(Protocol[_T_co]):
    @overload
    def __getitem__(self, index: SupportsIndex, /) -> _T_co: ...

    @overload
    def __getitem__(self, index: slice, /) -> Sequence[_T_co]: ...

    def __contains__(self, value: object, /) -> bool: ...

    def __len__(self) -> int: ...

    def __iter__(self) -> Iterator[_T_co]: ...

    def index(self, value: Any, start: int = 0, stop: int = ..., /) -> int: ...

    def count(self, value: Any, /) -> int: ...

    def __reversed__(self) -> Iterator[_T_co]: ...


def is_sequence_not_str(obj: Any) -> TypeGuard[SequenceNotStr]:
    return isinstance(obj, Sequence) and not isinstance(obj, str)


class SupportsRead(Protocol[_T_co]):
    def read(self, length: int = ..., /) -> _T_co: ...
