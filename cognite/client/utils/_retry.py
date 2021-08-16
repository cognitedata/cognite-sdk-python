from typing import Callable, Tuple, Type, TypeVar

T = TypeVar("T")


def retry_exceptions(callable: Callable[[], T], exceptions: Tuple[Type[BaseException], ...], attempts: int = 3) -> T:
    while True:
        try:
            return callable()
        except exceptions:
            attempts -= 1
            if attempts == 0:
                raise
