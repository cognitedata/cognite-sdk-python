from __future__ import annotations

import json
import warnings
from abc import ABC
from typing import TYPE_CHECKING, Any, Generic, Literal

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResource,
    T_CogniteResource,
    T_WritableCogniteResource,
    T_WriteClass,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
    basic_instance_dump,
)
from cognite.client.utils._auxiliary import json_dump_default
from cognite.client.utils._importing import local_import

if TYPE_CHECKING:
    import pandas as pd

    from cognite.client import CogniteClient


class DataModelingResource(CogniteResource, ABC):
    def __init__(self, space: str):
        self.space = space

    def __repr__(self) -> str:
        args = []
        if hasattr(self, "space"):
            space = self.space
            args.append(f"{space=}")
        if hasattr(self, "external_id"):
            external_id = self.external_id
            args.append(f"{external_id=}")
        if hasattr(self, "version"):
            version = self.version
            args.append(f"{version=}")

        return f"<{type(self).__qualname__}({', '.join(args)}) at {id(self):#x}>"


class WritableDataModelingResource(WriteableCogniteResource[T_CogniteResource], ABC):
    def __init__(self, space: str) -> None:
        self.space = space

    def __repr__(self) -> str:
        args = []
        if hasattr(self, "space"):
            space = self.space
            args.append(f"{space=}")
        if hasattr(self, "external_id"):
            external_id = self.external_id
            args.append(f"{external_id=}")
        if hasattr(self, "version"):
            version = self.version
            args.append(f"{version=}")

        return f"<{type(self).__qualname__}({', '.join(args)}) at {id(self):#x}>"


class DataModelingSchemaResource(WritableDataModelingResource[T_CogniteResource], ABC):
    def __init__(
        self,
        space: str,
        external_id: str,
        name: str | None,
        description: str | None,
    ) -> None:
        super().__init__(space=space)
        self.external_id = external_id
        self.name = name
        self.description = description


class DataModelingInstancesList(WriteableCogniteResourceList, Generic[T_WriteClass, T_WritableCogniteResource], ABC):
    def to_pandas(  # type: ignore [override]
        self,
        camel_case: bool = False,
        convert_timestamps: bool = True,
        expand_properties: bool = False,
        remove_property_prefix: bool = True,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Convert the instance into a pandas DataFrame. Note that if the properties column is expanded and there are
        keys in the metadata that already exist in the DataFrame, then an error will be raised by pandas during joining.

        Args:
            camel_case (bool): Convert column names to camel case (e.g. `externalId` instead of `external_id`). Does not apply to properties.
            convert_timestamps (bool): Convert known columns storing CDF timestamps (milliseconds since epoch) to datetime. Does not affect properties.
            expand_properties (bool): Expand the properties into separate columns. Note: Will change default to True in the next major version.
            remove_property_prefix (bool): Remove view ID prefix from columns names of expanded properties. Requires data to be from a single view.
            **kwargs (Any): For backwards compatability.

        Returns:
            pd.DataFrame: The Cognite resource as a dataframe.
        """
        kwargs.pop("expand_metadata", None), kwargs.pop("metadata_prefix", None)
        if kwargs:
            raise TypeError(f"Unsupported keyword arguments: {kwargs}")
        if not expand_properties:
            warnings.warn(
                "Keyword argument 'expand_properties' will change default from False to True in the next major version.",
                DeprecationWarning,
            )
        df = super().to_pandas(camel_case=camel_case, expand_metadata=False, convert_timestamps=convert_timestamps)
        if not expand_properties or "properties" not in df.columns:
            return df

        prop_df = local_import("pandas").json_normalize(df.pop("properties"), max_level=2)
        if remove_property_prefix and not prop_df.empty:
            # We only do/allow this if we have a single source:
            view_id, *extra = set(vid for item in self for vid in item.properties)
            if not extra:
                prop_df.columns = prop_df.columns.str.removeprefix("{}.{}/{}.".format(*view_id.as_tuple()))
            else:
                warnings.warn(
                    "Can't remove view ID prefix from expanded property columns as source was not unique",
                    RuntimeWarning,
                )
        return df.join(prop_df)


class DataModelingSort(CogniteObject):
    def __init__(
        self,
        property: str | list[str] | tuple[str, ...],
        direction: Literal["ascending", "descending"] = "ascending",
        nulls_first: bool = False,
    ) -> None:
        super().__init__()
        self.property = property
        self.direction = direction
        self.nulls_first = nulls_first

    def __eq__(self, other: Any) -> bool:
        return type(other) is type(self) and self.dump() == other.dump()

    def __str__(self) -> str:
        return json.dumps(self.dump(), default=json_dump_default, indent=4)

    def __repr__(self) -> str:
        return str(self)

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        if not isinstance(resource, dict):
            raise TypeError(f"Resource must be mapping, not {type(resource)}")

        instance = cls(property=resource["property"])
        if "direction" in resource:
            instance.direction = resource["direction"]
        if "nullsFirst" in resource:
            instance.nulls_first = resource["nullsFirst"]

        return instance

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return basic_instance_dump(self, camel_case=camel_case)
