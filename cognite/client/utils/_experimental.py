from __future__ import annotations

import warnings
from typing import Literal


class FeaturePreviewWarning(FutureWarning):
    def __init__(
        self, api_maturity: Literal["alpha", "beta"], sdk_maturity: Literal["alpha", "beta"], feature_name: str
    ):
        self.api_version = api_maturity
        self.sdk_version = sdk_maturity
        self.feature_name = feature_name

    def __str__(self) -> str:
        if self.api_version == "alpha" or self.sdk_version == "alpha":
            return (
                f"{self.feature_name} is in alpha and is subject to breaking changes without notice. API maturity={self.api_version}, SDK maturity={self.sdk_version}. "
                "See https://cognite-sdk-python.readthedocs-hosted.com/en/latest/appendix.html for more information."
            )
        else:
            return (
                f"{self.feature_name} is in beta, breaking changes may occur but will be preceded by a DeprecationWarning. "
                f"API version={self.api_version}, SDK version={self.sdk_version}. "
                "See https://cognite-sdk-python.readthedocs-hosted.com/en/latest/appendix.html for more information."
            )

    def warn(self) -> None:
        warnings.warn(self, stacklevel=2)

    def __reduce__(self) -> tuple:
        # This is needed to make the cognite client picklable as warings are stored on APIClass objects.
        return self.__class__, (self.api_version, self.sdk_version, self.feature_name)
