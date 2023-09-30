import warnings
from typing import Literal


class FeaturePreviewWarning(FutureWarning):
    def __init__(self, api_version: Literal["alpha", "beta"], sdk_version: Literal["alpha", "beta"], feature_name: str):
        self.api_version = api_version
        self.sdk_version = sdk_version
        self.feature_name = feature_name

    def __str__(self) -> str:
        if self.api_version == "alpha" or self.sdk_version == "alpha":
            return (
                f"{self.feature_name} is in alpha and is subject to breaking changes without notice. API version={self.api_version}, SDK version={self.sdk_version}. "
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
