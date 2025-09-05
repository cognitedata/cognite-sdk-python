import sys
from collections.abc import Callable
from inspect import isfunction
from typing import Any, TypeVar, cast

from beartype import beartype
from beartype.roar import BeartypeCallHintParamViolation

from cognite.client.exceptions import CogniteTypeError

T_Callable = TypeVar("T_Callable", bound=Callable)
T_Class = TypeVar("T_Class", bound=type)


class Settings:
    enable_runtime_type_checking: bool = False


def runtime_type_checked_method(f: T_Callable) -> T_Callable:
    if (sys.version_info < (3, 10)) or not Settings.enable_runtime_type_checking:
        return f
    beartyped_f = beartype(f)

    def f_wrapped(*args: Any, **kwargs: Any) -> Any:
        try:
            return beartyped_f(*args, **kwargs)
        except BeartypeCallHintParamViolation as e:
            raise CogniteTypeError(e.args[0])

    return cast(T_Callable, f_wrapped)


def runtime_type_checked(c: T_Class) -> T_Class:
    for name in dir(c):
        if not name.startswith("_") or (
            (name == "__init__" and isfunction(getattr(c, name))) and callable(getattr(c, name))
        ):
            setattr(c, name, runtime_type_checked_method(getattr(c, name)))
    return c
