from __future__ import annotations

import warnings
from abc import ABC, abstractmethod
from collections import UserList
from collections.abc import Iterable, Iterator, Sequence
from contextlib import suppress
from dataclasses import dataclass
from enum import Enum
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    Protocol,
    SupportsIndex,
    TypeAlias,
    TypeVar,
    cast,
    final,
    overload,
    runtime_checkable,
)

from typing_extensions import Self

from cognite.client.exceptions import CogniteMissingClientError
from cognite.client.utils import _json_extended as _json
from cognite.client.utils._auxiliary import load_resource_to_dict, load_yaml_or_json
from cognite.client.utils._identifier import IdentifierSequence, InstanceId
from cognite.client.utils._importing import local_import
from cognite.client.utils._text import convert_all_keys_recursive, convert_all_keys_to_camel_case, to_camel_case
from cognite.client.utils._time import TIME_ATTRIBUTES, convert_and_isoformat_time_attrs
from cognite.client.utils.useful_types import is_sequence_not_str

if TYPE_CHECKING:
    import pandas

    from cognite.client import AsyncCogniteClient


def basic_instance_dump(obj: Any, camel_case: bool) -> dict[str, Any]:
    dumped = {k: v for k, v in vars(obj).items() if v is not None and not k.startswith("_")}
    if camel_case:
        return convert_all_keys_to_camel_case(dumped)
    return dumped


class _WithClientMixin:
    __cognite_client: AsyncCogniteClient

    @property
    def _cognite_client(self) -> AsyncCogniteClient:
        with suppress(AttributeError):
            return self.__cognite_client
        raise CogniteMissingClientError(self)

    @_cognite_client.setter
    def _cognite_client(self, value: AsyncCogniteClient) -> None:
        from cognite.client import AsyncCogniteClient

        if isinstance(value, AsyncCogniteClient):
            # Internally, we pretend value is always a client ref, since getting it will raise if missing/None:
            self.__cognite_client = value
        else:
            raise TypeError("Can't set the client reference to anything else than AsyncCogniteClient")

    def _get_cognite_client(self) -> AsyncCogniteClient | None:
        """Get Cognite client reference without raising (when missing/not set)"""
        with suppress(AttributeError):
            return self.__cognite_client
        return None


class CogniteResource(ABC):
    """The CogniteResource is the main data class in the SDK and is used to add serialization and deserialization, and the to_pandas method,
    which together with _repr_html_ makes it easy to visualize data in a tabular format in e.g. Jupyter notebooks.
    """

    def __eq__(self, other: Any) -> bool:
        return type(self) is type(other) and self.dump() == other.dump()

    def __str__(self) -> str:
        item = convert_and_isoformat_time_attrs(self.dump(camel_case=False))
        return _json.dumps(item, indent=4)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to True.

        Returns:
            dict[str, Any]: A dictionary representation of the instance.
        """
        return basic_instance_dump(self, camel_case=camel_case)

    def dump_yaml(self) -> str:
        """Dump the instance into a YAML formatted string.

        Returns:
            str: A YAML formatted string representing the instance.
        """
        yaml = local_import("yaml")
        return yaml.safe_dump(self.dump(camel_case=True), sort_keys=False)

    @final
    @classmethod
    def load(cls, resource: dict | str) -> Self:
        """Load a resource from a YAML/JSON string or dict."""
        loaded = load_resource_to_dict(resource)
        return cls._load(loaded)

    @classmethod
    @abstractmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        """
        This is the internal load method that is called by the public load method or directly from
        within the SDK when loading resources from the API.

        Subclasses must implement this method to handle their specific resource loading logic.

        Args:
            resource (dict[str, Any]): The resource to load.

        Returns:
            Self: The loaded resource.
        """
        raise NotImplementedError

    @final
    @classmethod
    def _load_if(cls, resource: dict[str, Any] | None) -> Self | None:
        if resource is None:
            return None
        return cls._load(resource)

    def to_pandas(
        self,
        expand_metadata: bool = False,
        metadata_prefix: str = "metadata.",
        ignore: list[str] | None = None,
        camel_case: bool = False,
        convert_timestamps: bool = True,
    ) -> pandas.DataFrame:
        """Convert the instance into a pandas DataFrame.

        Args:
            expand_metadata (bool): Expand the metadata into separate rows (default: False).
            metadata_prefix (str): Prefix to use for the metadata rows, if expanded.
            ignore (list[str] | None): List of row keys to skip when converting to a data frame. Is applied before expansions.
            camel_case (bool): Convert attribute names to camel case (e.g. `externalId` instead of `external_id`). Does not affect custom data like metadata if expanded.
            convert_timestamps (bool): Convert known attributes storing CDF timestamps (milliseconds since epoch) to datetime. Does not affect custom data like metadata.

        Returns:
            pandas.DataFrame: The dataframe.
        """
        pd = local_import("pandas")

        dumped = self.dump(camel_case=camel_case)

        for element in ignore or []:
            dumped.pop(element, None)

        has_time_attribute = False
        if convert_timestamps:
            for k in TIME_ATTRIBUTES.intersection(dumped):
                dumped[k] = pd.Timestamp(dumped[k], unit="ms")
                has_time_attribute = True

        if expand_metadata and "metadata" in dumped and isinstance(dumped["metadata"], dict):
            dumped.update({f"{metadata_prefix}{k}": v for k, v in dumped.pop("metadata").items()})

        df = pd.Series(dumped).to_frame(name="value")
        if len(dumped) == 1 and has_time_attribute:
            df.value = df.value.astype("datetime64[ms]")
        return df

    def _repr_html_(self) -> str:
        from cognite.client.utils._pandas_helpers import notebook_display_with_fallback

        return notebook_display_with_fallback(self)

    def _maybe_set_client_ref(self, client: AsyncCogniteClient) -> Self:
        return self  # Base resource has no client ref set


class UnknownCogniteResource(CogniteResource):
    def __init__(self, data: dict[str, Any]) -> None:
        self.__data = data

    @final
    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(resource)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        # TODO: Recursive key conversion can modify user data if present:
        return convert_all_keys_recursive(self.__data, camel_case=camel_case)


T_WriteClass = TypeVar("T_WriteClass", bound=CogniteResource)


class WriteableCogniteResource(CogniteResource, Generic[T_WriteClass]):
    @abstractmethod
    def as_write(self) -> T_WriteClass:
        raise NotImplementedError


T_CogniteResource = TypeVar("T_CogniteResource", bound=CogniteResource)
T_WritableCogniteResource = TypeVar("T_WritableCogniteResource", bound=WriteableCogniteResource)


class CogniteResourceWithClientRef(CogniteResource, _WithClientMixin):
    """
    This class extends CogniteResource to include a reference to the Cognite client instance.
    This is useful for resources that need to perform operations that require interaction with the Cognite API,
    for example to fetch related data. This should only be used by the SDK internally, so it relies on the client
    reference being set after instantiation. We do this to conserve LSP compliance with the base CogniteResource class
    (essentially only _load/load and __init__).
    """

    def _maybe_set_client_ref(self, client: AsyncCogniteClient) -> Self:
        self._cognite_client = client
        return self

    set_client_ref = _maybe_set_client_ref


class WriteableCogniteResourceWithClientRef(
    WriteableCogniteResource[T_WriteClass], CogniteResourceWithClientRef, Generic[T_WriteClass]
):
    @abstractmethod
    def as_write(self) -> T_WriteClass:
        raise NotImplementedError


class CogniteResourceList(UserList, Generic[T_CogniteResource]):
    _RESOURCE: type[T_CogniteResource]

    def __init__(self, resources: Sequence[T_CogniteResource]) -> None:
        if resources:
            # We do one type check on the first element only, and assume homogeneous if that passes. These classes
            # should only be instantiated by the SDK anyway.
            if not isinstance(resources[0], self._RESOURCE):
                raise TypeError(
                    f"All resources for class '{self.__class__.__name__}' must be of type "
                    f"'{self._RESOURCE.__name__}', not '{type(resources[0])}'."
                )
        super().__init__(resources)

    @cached_property
    def _id_to_item(self) -> dict[int, T_CogniteResource]:
        if self.data and hasattr(self.data[0], "id"):
            return {item.id: item for item in self.data if item.id is not None}
        return {}

    @cached_property
    def _external_id_to_item(self) -> dict[str, T_CogniteResource]:
        if self.data and hasattr(self.data[0], "external_id"):
            return {item.external_id: item for item in self.data if item.external_id is not None}
        return {}

    @cached_property
    def _instance_id_to_item(self) -> dict[InstanceId, T_CogniteResource]:
        if self.data and hasattr(self.data[0], "instance_id"):
            return {item.instance_id: item for item in self.data if item.instance_id is not None}
        return {}

    def pop(self, i: int = -1) -> T_CogniteResource:
        return super().pop(i)

    def __iter__(self) -> Iterator[T_CogniteResource]:
        return super().__iter__()

    @overload
    def __getitem__(self: T_CogniteResourceList, item: SupportsIndex) -> T_CogniteResource: ...

    @overload
    def __getitem__(self: T_CogniteResourceList, item: slice) -> T_CogniteResourceList: ...

    def __getitem__(
        self: T_CogniteResourceList, item: SupportsIndex | slice
    ) -> T_CogniteResource | T_CogniteResourceList:
        value = self.data[item]
        if isinstance(item, slice):
            return type(self)(value)
        return cast(T_CogniteResource, value)

    def __str__(self) -> str:
        item = convert_and_isoformat_time_attrs(self.dump(camel_case=False))
        return _json.dumps(item, indent=4)

    # TODO: We inherit a lot from UserList that we don't actually support...
    def extend(self, other: Iterable[Any]) -> None:
        other_res_list = type(self)(cast(Sequence, other))  # See if we can accept the types
        if self._id_to_item.keys().isdisjoint(other_res_list._id_to_item):
            super().extend(other)
            self._external_id_to_item.update(other_res_list._external_id_to_item)
            self._id_to_item.update(other_res_list._id_to_item)
            self._instance_id_to_item.update(other_res_list._instance_id_to_item)
        else:
            raise ValueError("Unable to extend as this would introduce duplicates")

    def dump(self, camel_case: bool = True) -> list[dict[str, Any]]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to True.

        Returns:
            list[dict[str, Any]]: A list of dicts representing the instance.
        """
        return [resource.dump(camel_case) for resource in self.data]

    def dump_yaml(self) -> str:
        """Dump the instances into a YAML formatted string.

        Returns:
            str: A YAML formatted string representing the instances.
        """
        yaml = local_import("yaml")
        return yaml.safe_dump(self.dump(camel_case=True), sort_keys=False)

    def get(
        self,
        id: int | None = None,
        external_id: str | None = None,
        instance_id: InstanceId | tuple[str, str] | None = None,
    ) -> T_CogniteResource | None:
        """Get an item from this list by id, external_id or instance_id.

        Args:
            id (int | None): The id of the item to get.
            external_id (str | None): The external_id of the item to get.
            instance_id (InstanceId | tuple[str, str] | None): The instance_id of the item to get.

        Returns:
            T_CogniteResource | None: The requested item if present, otherwise None.
        """
        (ident := IdentifierSequence.load(id, external_id, instance_id)).assert_singleton()
        if id:
            return self._id_to_item.get(id)
        elif external_id:
            return self._external_id_to_item.get(external_id)
        # TODO: Instance ID is really not well supported in our identifier helper classes:
        return self._instance_id_to_item.get(ident.as_primitives()[0])  # type: ignore[call-overload]

    def to_pandas(
        self,
        camel_case: bool = False,
        expand_metadata: bool = False,
        metadata_prefix: str = "metadata.",
        convert_timestamps: bool = True,
    ) -> pandas.DataFrame:
        """Convert the instance into a pandas DataFrame. Note that if the metadata column is expanded and there are
        keys in the metadata that already exist in the DataFrame, then an error will be raised by pd.join.

        Args:
            camel_case (bool): Convert column names to camel case (e.g. `externalId` instead of `external_id`)
            expand_metadata (bool): Expand the metadata column into separate columns.
            metadata_prefix (str): Prefix to use for metadata columns.
            convert_timestamps (bool): Convert known columns storing CDF timestamps (milliseconds since epoch) to datetime. Does not affect custom data like metadata.

        Returns:
            pandas.DataFrame: The Cognite resource as a dataframe.
        """
        pd = local_import("pandas")
        from cognite.client.utils._pandas_helpers import (
            convert_nullable_int_cols,
            convert_timestamp_columns_to_datetime,
        )

        df = pd.DataFrame(self.dump(camel_case=camel_case))
        df = convert_nullable_int_cols(df)

        if convert_timestamps:
            df = convert_timestamp_columns_to_datetime(df)

        if expand_metadata and "metadata" in df.columns:
            # Equivalent to pd.json_normalize(df["metadata"]) but is a faster implementation.
            meta_series = df.pop("metadata").dropna()
            meta_df = pd.DataFrame(meta_series.values.tolist(), index=meta_series.index).add_prefix(metadata_prefix)
            df = df.join(meta_df)
        return df

    def _repr_html_(self) -> str:
        from cognite.client.utils._pandas_helpers import notebook_display_with_fallback

        return notebook_display_with_fallback(self)

    @final
    @classmethod
    def load(cls, resource: Sequence[dict[str, Any]] | str) -> Self:
        """Load a resource from a YAML/JSON string or iterable of dict."""
        if isinstance(resource, str):
            resource = load_yaml_or_json(resource)

        if is_sequence_not_str(resource):
            return cls._load(cast(Sequence, resource))

        raise TypeError(f"Resource must be json or yaml str, or sequence of dicts, not {type(resource)}")

    @classmethod
    def _load(cls, resource: Sequence[dict[str, Any]]) -> Self:
        return cls(list(map(cls._RESOURCE._load, resource)))

    @final
    @classmethod
    def _load_if(cls, resource: Sequence[dict[str, Any]] | None) -> Self | None:
        if resource is None:
            return None
        return cls._load(resource)

    @classmethod
    def _load_raw_api_response(cls, responses: list[dict[str, Any]]) -> Self:
        # Certain classes may need more than just 'items' from the raw response. These need to provide
        # an implementation of this method
        raise NotImplementedError

    def dump_raw(self, camel_case: bool = True) -> dict[str, Any]:
        """This method dumps the list with extra information in addition to the items.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to True.

        Returns:
            dict[str, Any]: A dictionary representation of the list.
        """
        return {"items": [resource.dump(camel_case) for resource in self.data]}

    def _maybe_set_client_ref(self, client: AsyncCogniteClient) -> Self:
        # Base resource has no client ref set, but cls._RESOURCE might need it:
        for item in self:
            item._maybe_set_client_ref(client)
        return self


T_CogniteResourceList = TypeVar("T_CogniteResourceList", bound=CogniteResourceList)


class WriteableCogniteResourceList(
    CogniteResourceList[T_WritableCogniteResource], Generic[T_WriteClass, T_WritableCogniteResource]
):
    @abstractmethod
    def as_write(self) -> CogniteResourceList[T_WriteClass]:
        raise NotImplementedError


class CogniteResourceListWithClientRef(CogniteResourceList[T_CogniteResource], _WithClientMixin):
    """
    This class extends CogniteResourceList to include a reference to the Cognite client instance.
    This is useful for resource lists that need to perform operations that require interaction with the Cognite API,
    for example to fetch related data. This should only be used by the SDK internally, so it relies on the client
    reference being set after instantiation. We do this to conserve LSP compliance with the base CogniteResourceList class
    (essentially only _load/load and __init__).
    """

    @overload
    def __getitem__(self: T_CogniteResourceListWithClientRef, item: SupportsIndex) -> T_CogniteResource: ...

    @overload
    def __getitem__(self: T_CogniteResourceListWithClientRef, item: slice) -> T_CogniteResourceListWithClientRef: ...

    def __getitem__(
        self: T_CogniteResourceListWithClientRef, item: SupportsIndex | slice
    ) -> T_CogniteResource | T_CogniteResourceListWithClientRef:
        value = self.data[item]
        if isinstance(item, slice):
            new_list = type(self)(value)
            try:
                return new_list.set_client_ref(self._cognite_client)
            except CogniteMissingClientError:
                # In case the user instantiated the class themselves without a client, lets not raise here:
                return new_list

        return cast(T_CogniteResource, value)

    def _maybe_set_client_ref(self, client: AsyncCogniteClient) -> Self:
        self._cognite_client = client
        for item in self:
            item._maybe_set_client_ref(client)
        return self

    set_client_ref = _maybe_set_client_ref


T_CogniteResourceListWithClientRef = TypeVar(
    "T_CogniteResourceListWithClientRef", bound=CogniteResourceListWithClientRef
)


class WriteableCogniteResourceListWithClientRef(
    CogniteResourceListWithClientRef[T_WritableCogniteResource], Generic[T_WriteClass, T_WritableCogniteResource]
):
    @abstractmethod
    def as_write(self) -> CogniteResourceList[T_WriteClass]:
        raise NotImplementedError


@dataclass
class PropertySpec:
    name: str
    is_list: bool = False
    is_object: bool = False
    is_nullable: bool = True
    # Used to skip replace when the value is None
    is_beta: bool = False
    # Objects that are nullable and support setNull. This is hosted extractor mqtt/kafka sources
    is_explicit_nullable_object: bool = False

    def __post_init__(self) -> None:
        assert not (self.is_list and self.is_object), "PropertySpec cannot be both list and object"


class CogniteUpdate:
    def __init__(self, id: int | None = None, external_id: str | None = None) -> None:
        self._id = id
        self._external_id = external_id
        self._update_object: dict[str, Any] = {}

        if id is not None and external_id is not None:
            warnings.warn(
                "Update object got both of 'id' and 'external_id', 'external_id' will be ignored.",
                UserWarning,
                stacklevel=2,
            )

    def __eq__(self, other: Any) -> bool:
        return type(self) is type(other) and self.dump() == other.dump()

    def __str__(self) -> str:
        return _json.dumps(self.dump(), indent=4)

    def __repr__(self) -> str:
        return str(self)

    def _set(self, name: str, value: Any) -> None:
        update_obj = self._update_object.get(name, {})
        if "remove" in update_obj or "add" in update_obj:
            raise RuntimeError("Can not call set after adding or removing fields from an update object.")
        self._update_object[name] = {"set": value}

    def _set_null(self, name: str) -> None:
        self._update_object[name] = {"setNull": True}

    def _add(self, name: str, value: Any) -> None:
        update_obj = self._update_object.get(name, {})
        if "set" in update_obj:
            raise RuntimeError("Can not call remove or add fields after calling set on an update object.")
        if "add" in update_obj:
            raise RuntimeError(
                "Can not call add twice on the same object, please combine your objects "
                "and pass them to add in one call."
            )
        update_obj["add"] = value
        self._update_object[name] = update_obj

    def _remove(self, name: str, value: Any) -> None:
        update_obj = self._update_object.get(name, {})
        if "set" in update_obj:
            raise RuntimeError("Can not call remove or add fields after calling set on an update object.")
        if "remove" in update_obj:
            raise RuntimeError(
                "Can not call remove twice on the same object, please combine your items "
                "and pass them to remove in one call."
            )
        update_obj["remove"] = value
        self._update_object[name] = update_obj

    def _modify(self, name: str, value: Any) -> None:
        update_obj = self._update_object.get(name, {})
        if "set" in update_obj:
            raise RuntimeError("Can not call remove or add fields after calling set on an update object.")
        if "modify" in update_obj:
            raise RuntimeError(
                "Can not call modify twice on the same object, please combine your items "
                "and pass them to modify in one call."
            )
        update_obj["modify"] = value
        self._update_object[name] = update_obj

    def dump(self, camel_case: Literal[True] = True) -> dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (Literal[True]): No description.

        Returns:
            dict[str, Any]: A dictionary representation of the instance.
        """
        dumped: dict[str, Any] = {"update": self._update_object}
        if self._id is not None:
            dumped["id"] = self._id
        elif self._external_id is not None:
            dumped["externalId"] = self._external_id
        return dumped

    @classmethod
    @abstractmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        raise NotImplementedError


T_CogniteUpdate = TypeVar("T_CogniteUpdate", bound=CogniteUpdate)


class CognitePrimitiveUpdate(Generic[T_CogniteUpdate]):
    def __init__(self, update_object: T_CogniteUpdate, name: str) -> None:
        self._update_object = update_object
        self._name = name

    def _set(self, value: None | str | int | bool | dict | list) -> T_CogniteUpdate:
        if value is None:
            self._update_object._set_null(self._name)
        else:
            self._update_object._set(self._name, value)
        return self._update_object


class CogniteObjectUpdate(Generic[T_CogniteUpdate]):
    def __init__(self, update_object: T_CogniteUpdate, name: str) -> None:
        self._update_object = update_object
        self._name = name

    def _set(self, value: dict) -> T_CogniteUpdate:
        self._update_object._set(self._name, value)
        return self._update_object

    def _add(self, value: dict) -> T_CogniteUpdate:
        self._update_object._add(self._name, value)
        return self._update_object

    def _remove(self, value: list) -> T_CogniteUpdate:
        self._update_object._remove(self._name, value)
        return self._update_object


class CogniteListUpdate(Generic[T_CogniteUpdate]):
    def __init__(self, update_object: T_CogniteUpdate, name: str) -> None:
        self._update_object = update_object
        self._name = name

    def _set(self, value: list) -> T_CogniteUpdate:
        self._update_object._set(self._name, value)
        return self._update_object

    def _add(self, value: list) -> T_CogniteUpdate:
        self._update_object._add(self._name, value)
        return self._update_object

    def _remove(self, value: list) -> T_CogniteUpdate:
        self._update_object._remove(self._name, value)
        return self._update_object

    def _modify(self, value: list) -> T_CogniteUpdate:
        self._update_object._modify(self._name, value)
        return self._update_object


class CogniteLabelUpdate(Generic[T_CogniteUpdate]):
    def __init__(self, update_object: T_CogniteUpdate, name: str) -> None:
        self._update_object = update_object
        self._name = name

    def _set(self, external_ids: str | list[str]) -> T_CogniteUpdate:
        self._update_object._set(self._name, self._wrap_ids(self._wrap_in_list(external_ids)))
        return self._update_object

    def _add(self, external_ids: str | list[str]) -> T_CogniteUpdate:
        self._update_object._add(self._name, self._wrap_ids(self._wrap_in_list(external_ids)))
        return self._update_object

    def _remove(self, external_ids: str | list[str]) -> T_CogniteUpdate:
        self._update_object._remove(self._name, self._wrap_ids(self._wrap_in_list(external_ids)))
        return self._update_object

    def _wrap_ids(self, external_ids: list[str]) -> list[dict[str, str]]:
        return [{"externalId": external_id} for external_id in external_ids]

    def _wrap_in_list(self, external_ids: str | list[str]) -> list[str]:
        return external_ids if isinstance(external_ids, list) else [external_ids]


class CogniteFilter(ABC):
    def __eq__(self, other: Any) -> bool:
        return type(self) is type(other) and self.dump() == other.dump()

    def __str__(self) -> str:
        item = convert_and_isoformat_time_attrs(self.dump(camel_case=False))
        return _json.dumps(item, indent=4)

    def __repr__(self) -> str:
        return str(self)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to True.

        Returns:
            dict[str, Any]: A dictionary representation of the instance.
        """
        return basic_instance_dump(self, camel_case=camel_case)


T_CogniteFilter = TypeVar("T_CogniteFilter", bound=CogniteFilter)


class EnumProperty(Enum):
    @staticmethod
    def _generate_next_value_(name: str, *_: Any) -> str:
        # Allows the use of enum.auto() for member values avoiding camelCase typos
        return to_camel_case(name)

    def as_reference(self) -> list[str]:
        return [self.value]


SortableProperty: TypeAlias = str | list[str] | EnumProperty


class CogniteSort:
    def __init__(
        self,
        property: SortableProperty,
        order: Literal["asc", "desc"] = "asc",
        nulls: Literal["auto", "first", "last"] | None = None,
    ):
        self.property = property
        self.order = order
        self.nulls = nulls

    def __str__(self) -> str:
        return _json.dumps(self.dump(camel_case=False), indent=4)

    def __repr__(self) -> str:
        return str(self)

    @classmethod
    def load(
        cls: type[T_CogniteSort],
        data: dict[str, Any]
        | tuple[SortableProperty, Literal["asc", "desc"]]
        | tuple[SortableProperty, Literal["asc", "desc"], Literal["auto", "first", "last"]]
        | SortableProperty
        | T_CogniteSort,
    ) -> T_CogniteSort:
        if isinstance(data, cls):
            return data
        elif isinstance(data, dict):
            return cls(property=data["property"], order=data.get("order", "asc"), nulls=data.get("nulls"))
        elif isinstance(data, tuple) and len(data) == 2 and data[1] in ["asc", "desc"]:
            return cls(property=data[0], order=data[1])
        elif (
            isinstance(data, tuple)
            and len(data) == 3
            and data[1] in ["asc", "desc"]
            and data[2] in ["auto", "first", "last"]
        ):
            return cls(
                property=data[0],
                order=data[1],
                nulls=data[2],
            )
        elif isinstance(data, str) and (prop_order := data.split(":", 1))[-1] in ("asc", "desc"):
            # Syntax "<fieldname>:asc|desc" is deprecated but handled for compatibility
            return cls(property=prop_order[0], order=cast(Literal["asc", "desc"], prop_order[1]))
        elif isinstance(data, (str, list, EnumProperty)):
            return cls(property=data)
        else:
            raise ValueError(f"Unable to load {cls.__name__} from {data}")

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        prop = self.property
        if isinstance(prop, EnumProperty):
            prop = prop.as_reference()
        elif isinstance(prop, str):
            prop = [to_camel_case(prop)]
        elif isinstance(prop, list):
            prop = [to_camel_case(p) for p in prop]
        else:
            raise ValueError(f"Unable to dump {type(self).__name__} with property {prop}")

        output: dict[str, str | list[str]] = {"property": prop, "order": self.order}
        if self.nulls is not None:
            output["nulls"] = self.nulls
        return output


T_CogniteSort = TypeVar("T_CogniteSort", bound=CogniteSort)
T_WriteClass_co = TypeVar("T_WriteClass_co", bound=CogniteResource, covariant=True)


@runtime_checkable
class SupportsAsWrite(Protocol[T_WriteClass_co]):
    def as_write(self) -> T_WriteClass_co: ...


@runtime_checkable
class HasExternalId(Protocol):
    @property
    def external_id(self) -> str | None: ...


@runtime_checkable
class HasName(Protocol):
    @property
    def name(self) -> str | None: ...


@runtime_checkable
class HasInternalId(Protocol):
    @property
    def id(self) -> int: ...


@runtime_checkable
class HasExternalAndInternalId(Protocol):
    @property
    def external_id(self) -> str | None: ...

    @property
    def id(self) -> int: ...


class ExternalIDTransformerMixin(Sequence[HasExternalId], ABC):
    def as_external_ids(self) -> list[str]:
        """
        Returns the external ids of all resources.

        Raises:
            ValueError: If any resource in the list does not have an external id.

        Returns:
            list[str]: The external ids of all resources in the list.
        """
        external_ids: list[str] = []
        for x in self:
            if x.external_id is None:
                raise ValueError(f"All {type(x).__name__} must have external_id")
            external_ids.append(x.external_id)
        return external_ids


class NameTransformerMixin(Sequence[HasName], ABC):
    def as_names(self) -> list[str]:
        """
        Returns the names of all resources.

        Raises:
            ValueError: If any resource in the list does not have a name.

        Returns:
            list[str]: The names of all resources in the list.
        """
        names: list[str] = []
        for x in self:
            if x.name is None:
                raise ValueError(f"All {type(x).__name__} must have name")
            names.append(x.name)
        return names


class InternalIdTransformerMixin(Sequence[HasInternalId], ABC):
    def as_ids(self) -> list[int]:
        """
        Returns the ids of all resources.

        Raises:
            ValueError: If any resource in the list does not have an id.

        Returns:
            list[int]: The ids of all resources in the list.
        """
        ids: list[int] = []
        for x in self:
            if x.id is None:
                raise ValueError(f"All {type(x).__name__} must have id")
            ids.append(x.id)
        return ids


class IdTransformerMixin(ExternalIDTransformerMixin, InternalIdTransformerMixin): ...
