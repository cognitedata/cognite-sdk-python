from __future__ import annotations

import importlib
from collections.abc import Callable, Iterable, Iterator
from types import ModuleType
from typing import TYPE_CHECKING, TypeVar, overload

if TYPE_CHECKING:
    from concurrent.futures import Future


_T = TypeVar("_T")


@overload
def local_import(m1: str, /) -> ModuleType: ...


@overload
def local_import(m1: str, m2: str, /) -> tuple[ModuleType, ModuleType]: ...


@overload
def local_import(m1: str, m2: str, m3: str, /) -> tuple[ModuleType, ModuleType, ModuleType]: ...


@overload
def local_import(m1: str, m2: str, m3: str, m4: str, /) -> tuple[ModuleType, ModuleType, ModuleType, ModuleType]: ...


def local_import(*module: str) -> ModuleType | tuple[ModuleType, ...]:
    from cognite.client.exceptions import CogniteImportError

    if len(module) == 1:
        name = module[0]
        try:
            return importlib.import_module(name)
        except ImportError as e:
            raise CogniteImportError(name.split(".")[0]) from e

    modules = []
    for name in module:
        try:
            modules.append(importlib.import_module(name))
        except ImportError as e:
            raise CogniteImportError(name.split(".")[0]) from e
    return tuple(modules)


def import_as_completed() -> Callable[[Iterable[Future[_T]]], Iterator[Future[_T]]]:
    from cognite.client._constants import _RUNNING_IN_BROWSER

    if not _RUNNING_IN_BROWSER:
        from concurrent.futures import as_completed
    else:
        from copy import copy

        def as_completed(fs: Iterable[Future[_T]], timeout: float | None = None) -> Iterator[Future[_T]]:  # type: ignore [misc]
            return iter(copy(fs))

    return as_completed
