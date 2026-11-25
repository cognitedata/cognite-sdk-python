from __future__ import annotations

import warnings
from typing import Any

from typing_extensions import Self

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
from cognite.client.data_classes.user_profiles import UserProfilesConfiguration
from cognite.client.utils._auxiliary import drop_none_values


class Project(CogniteResource):
    """A CDF project. Projects are used to isolate data in CDF from each other.

    Args:
        url_name (str): The URL name of the project. This is used as part of the request path in API calls.
        organization (str): The ID of the organization the project belongs to.
        name (str | None): The user-friendly name of the project.
        oidc_configuration (dict[str, Any] | None): The OIDC configuration of the project.
        user_profiles_configuration (UserProfilesConfiguration | None): The user profiles configuration of the project.
    """

    def __init__(
        self,
        url_name: str,
        organization: str,
        name: str | None = None,
        oidc_configuration: dict[str, Any] | None = None,
        user_profiles_configuration: UserProfilesConfiguration | None = None,
    ) -> None:
        self.url_name = url_name
        self.organization = organization
        self.name = name
        self.oidc_configuration = oidc_configuration
        self.user_profiles_configuration = user_profiles_configuration

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        if (org := resource.get("organization")) is None:
            warnings.warn(
                f"The API did not return an organization for project {resource['urlName']!r}. "
                "The SDK treats it as required as it can only be missing for internal Cognite projects. "
                "NOTE: Using an empty string as the organization and continuing.",
                UserWarning,
                stacklevel=2,
            )
            org = ""

        return cls(
            url_name=resource["urlName"],
            organization=org,
            name=resource.get("name"),
            oidc_configuration=resource.get("oidcConfiguration"),
            user_profiles_configuration=UserProfilesConfiguration._load_if(resource.get("userProfilesConfiguration")),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        # The OIDC configuration is passed through as-is from the API, so we avoid recursive key conversion:
        output = {
            "urlName" if camel_case else "url_name": self.url_name,
            "organization": self.organization,
            "name": self.name,
            "oidcConfiguration" if camel_case else "oidc_configuration": self.oidc_configuration,
            "userProfilesConfiguration" if camel_case else "user_profiles_configuration": (
                self.user_profiles_configuration.dump(camel_case) if self.user_profiles_configuration else None
            ),
        }
        return drop_none_values(output)


class ProjectList(CogniteResourceList[Project]):
    _RESOURCE = Project
