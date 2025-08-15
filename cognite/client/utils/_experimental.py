from __future__ import annotations

import functools
import warnings
from collections.abc import Callable
from typing import Any, Literal, TypeVar, cast


class FeaturePreviewWarning(FutureWarning):
    def __init__(
        self,
        api_maturity: Literal["alpha", "beta", "General Availability"],
        sdk_maturity: Literal["alpha", "beta"],
        feature_name: str,
    ):
        self.api_version = api_maturity
        self.sdk_version = sdk_maturity
        self.feature_name = feature_name

    def __str__(self) -> str:
        if self.api_version == "alpha" or self.sdk_version == "alpha":
            return (
                f"{self.feature_name} is in alpha and is subject to breaking changes without prior notice. "
                f"API maturity={self.api_version}, SDK maturity={self.sdk_version}. "
                "See https://cognite-sdk-python.readthedocs-hosted.com/en/latest/appendix.html for more information."
            )
        else:
            return (
                f"{self.feature_name} is in beta, breaking changes may occur but will be preceded by a DeprecationWarning. "
                f"API version={self.api_version}, SDK version={self.sdk_version}. "
                "See https://cognite-sdk-python.readthedocs-hosted.com/en/latest/appendix.html for more information."
            )

    def warn(self) -> None:
        from cognite.client import global_config

        if not global_config.silence_feature_preview_warnings:
            warnings.warn(self, stacklevel=2)

    def __reduce__(self) -> tuple:
        # This is needed to make the cognite client picklable as warings are stored on APIClass objects.
        return self.__class__, (self.api_version, self.sdk_version, self.feature_name)


T_Class = TypeVar("T_Class", bound=type)
T_Callable = TypeVar("T_Callable", bound=Callable)


def warn_on_all_method_invocations(warning: FeaturePreviewWarning) -> Callable[[T_Class], T_Class]:
    def _with_warning(c: T_Callable) -> T_Callable:
        @functools.wraps(c)
        def warning_wrapper(*args: Any, **kwargs: Any) -> Any:
            warning.warn()
            return c(*args, **kwargs)

        return cast(T_Callable, warning_wrapper)

    def _warn_on_all_method_invocations(cls: T_Class) -> T_Class:
        for name in dir(cls):
            if not name.startswith("_"):
                attr = getattr(cls, name)
                if callable(attr):
                    setattr(cls, name, _with_warning(attr))
        return cls

    return _warn_on_all_method_invocations
