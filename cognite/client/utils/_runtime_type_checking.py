from typing import Any, Callable, TypeVar

from beartype import beartype
from beartype.roar import BeartypeCallHintParamViolation

from cognite.client.exceptions import CogniteTypeError

T_Callable = TypeVar("T_Callable", bound=Callable)
T_Class = TypeVar("T_Class", bound=type)


def runtime_type_checked(f: T_Callable) -> T_Callable:
    beartyped_f = beartype(f)

    def f_wrapped(*args: Any, **kwargs: Any) -> Any:
        try:
            return beartyped_f(*args, **kwargs)
        except BeartypeCallHintParamViolation as e:
            raise CogniteTypeError(e.args[0])

    return f_wrapped  # type: ignore [return-value]


def runtime_type_checked_public_methods(c: T_Class) -> T_Class:
    for name in dir(c):
        if not name.startswith("_"):
            setattr(c, name, runtime_type_checked(getattr(c, name)))
    return c
