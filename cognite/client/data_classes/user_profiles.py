from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Sequence, cast

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
from cognite.client.utils._text import to_camel_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class UserProfile(CogniteResource):
    """User profiles is an authoritative source of core user profile information (email, name, job title, etc.)
    for principals based on data from the identity provider configured for the CDF project.

    Args:
        user_identifier (str): Uniquely identifies the principal the profile is associated with. This property is guaranteed to be immutable.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        given_name (str | None): The user's first name.
        surname (str | None): The user's last name.
        email (str | None): The user's email address (if any). The email address is is returned directly from the identity provider and not guaranteed to be verified. Note that the email is mutable and can be updated in the identity provider. It should not be used to uniquely identify as a user. Use the user_identifier property instead.
        display_name (str | None): The display name for the user.
        job_title (str | None): The user's job title.
        cognite_client (CogniteClient | None): No description.
    """

    def __init__(
        self,
        user_identifier: str,
        last_updated_time: int,
        given_name: str | None = None,
        surname: str | None = None,
        email: str | None = None,
        display_name: str | None = None,
        job_title: str | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.user_identifier = user_identifier
        self.last_updated_time = last_updated_time
        self.given_name = given_name
        self.surname = surname
        self.email = email
        self.display_name = display_name
        self.job_title = job_title
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: dict[str, Any] | str, cognite_client: CogniteClient | None = None) -> UserProfile:
        if isinstance(resource, str):
            resource = cast(dict[str, Any], json.loads(resource))
        to_load = {
            "user_identifier": resource["userIdentifier"],
            "last_updated_time": resource["lastUpdatedTime"],
        }
        for param in ["given_name", "surname", "email", "display_name", "job_title"]:
            if (value := resource.get(to_camel_case(param))) is not None:
                to_load[param] = value
        return cls(**to_load, cognite_client=cognite_client)


class UserProfileList(CogniteResourceList[UserProfile]):
    _RESOURCE = UserProfile

    def __init__(self, resources: Sequence[UserProfile], cognite_client: CogniteClient | None = None) -> None:
        super().__init__(resources, cognite_client)

        del self._id_to_item, self._external_id_to_item
        self._user_identifier_to_item = {item.user_identifier: item for item in self.data or []}

    def get(self, user_identifier: str) -> UserProfile | None:  # type: ignore [override]
        """Get an item from this list by user_identifier.
        Args:
            user_identifier (str): The user_identifier of the item to get.
        Returns:
            UserProfile | None: The requested item or None if not found.
        """
        return self._user_identifier_to_item.get(user_identifier)
