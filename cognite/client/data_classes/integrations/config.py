from __future__ import annotations

from abc import ABC
from typing import Any

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteResource,
    CogniteResourceList,
    ExternalIDTransformerMixin,
    WriteableCogniteResource,
)


class ConfigRevisionCore(WriteableCogniteResource["ConfigRevisionWrite"], ABC):
    """A versioned configuration document associated with an integration.

    Every write creates a new, immutable revision rather than overwriting the previous one.

    Args:
        external_id (str): External id of the integration this config revision belongs to.
        config (str | None): Contents of this configuration revision.
        description (str | None): Short description of this configuration revision.
    """

    def __init__(self, external_id: str, config: str | None = None, description: str | None = None) -> None:
        self.external_id = external_id
        self.config = config
        self.description = description


class ConfigRevisionWrite(ConfigRevisionCore):
    """A new configuration revision to create for an integration.

    Args:
        external_id (str): External id of the integration to create the config revision for.
        config (str | None): Contents of this configuration revision.
        description (str | None): Short description of this configuration revision.
    """

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource["externalId"],
            config=resource.get("config"),
            description=resource.get("description"),
        )

    def as_write(self) -> ConfigRevisionWrite:
        return self


class ConfigRevision(ConfigRevisionCore):
    """A single configuration revision for an integration, including its contents.

    Args:
        external_id (str): External id of the integration this config revision belongs to.
        revision (int): The revision number of this config revision.
        created_time (int): Time the config revision was created, in milliseconds since epoch.
        last_updated_time (int): Time the config revision was last updated, in milliseconds since epoch.
        config (str | None): Contents of this configuration revision.
        description (str | None): Short description of this configuration revision.
    """

    def __init__(
        self,
        external_id: str,
        revision: int,
        created_time: int,
        last_updated_time: int,
        config: str | None = None,
        description: str | None = None,
    ) -> None:
        super().__init__(external_id=external_id, config=config, description=description)
        self.revision = revision
        self.created_time = created_time
        self.last_updated_time = last_updated_time

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource["externalId"],
            revision=resource["revision"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            config=resource.get("config"),
            description=resource.get("description"),
        )

    def as_write(self) -> ConfigRevisionWrite:
        """Returns this ConfigRevision as a ConfigRevisionWrite"""
        return ConfigRevisionWrite(external_id=self.external_id, config=self.config, description=self.description)


class ConfigRevisionMetadata(CogniteResource):
    """Metadata about a configuration revision, without the config contents itself.

    Args:
        external_id (str): External id of the integration this config revision belongs to.
        revision (int): The revision number of this config revision.
        created_time (int): Time the config revision was created, in milliseconds since epoch.
        last_updated_time (int): Time the config revision was last updated, in milliseconds since epoch.
        description (str | None): Short description of this configuration revision.
    """

    def __init__(
        self,
        external_id: str,
        revision: int,
        created_time: int,
        last_updated_time: int,
        description: str | None = None,
    ) -> None:
        self.external_id = external_id
        self.revision = revision
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.description = description

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource["externalId"],
            revision=resource["revision"],
            created_time=resource["createdTime"],
            last_updated_time=resource["lastUpdatedTime"],
            description=resource.get("description"),
        )


class ConfigRevisionMetadataList(CogniteResourceList[ConfigRevisionMetadata], ExternalIDTransformerMixin):
    _RESOURCE = ConfigRevisionMetadata
