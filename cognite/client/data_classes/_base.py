from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections import UserList
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Collection,
    Generic,
    Iterator,
    List,
    Literal,
    Protocol,
    Sequence,
    SupportsIndex,
    TypeVar,
    Union,
    cast,
    overload,
)

from typing_extensions import TypeAlias

from cognite.client.exceptions import CogniteMissingClientError
from cognite.client.utils._auxiliary import fast_dict_load, json_dump_default
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._importing import local_import
from cognite.client.utils._pandas_helpers import convert_nullable_int_cols, notebook_display_with_fallback
from cognite.client.utils._text import convert_all_keys_to_camel_case, to_camel_case
from cognite.client.utils._time import convert_time_attributes_to_datetime

if TYPE_CHECKING:
    import pandas

    from cognite.client import CogniteClient


def basic_instance_dump(obj: Any, camel_case: bool) -> dict[str, Any]:
    # TODO: Consider using inheritance?
    dumped = {k: v for k, v in vars(obj).items() if v is not None and not k.startswith("_")}
    if camel_case:
        return convert_all_keys_to_camel_case(dumped)
    return dumped


class CogniteResponse:
    def __str__(self) -> str:
        item = convert_time_attributes_to_datetime(self.dump())
        return json.dumps(item, default=json_dump_default, indent=4)

    def __repr__(self) -> str:
        return str(self)

    def __eq__(self, other: Any) -> bool:
        return type(other) is type(self) and other.dump() == self.dump()

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to False.

        Returns:
            dict[str, Any]: A dictionary representation of the instance.
        """
        return basic_instance_dump(self, camel_case=camel_case)

    @classmethod
    def _load(cls, api_response: dict[str, Any]) -> CogniteResponse:
        raise NotImplementedError

    def to_pandas(self) -> pandas.DataFrame:
        raise NotImplementedError


T_CogniteResponse = TypeVar("T_CogniteResponse", bound=CogniteResponse)


class _WithClientMixin:
    @property
    def _cognite_client(self) -> CogniteClient:
        with suppress(AttributeError):
            if self.__cognite_client is not None:
                return self.__cognite_client
        raise CogniteMissingClientError(self)

    @_cognite_client.setter
    def _cognite_client(self, value: CogniteClient | None) -> None:
        from cognite.client import CogniteClient

        if value is None or isinstance(value, CogniteClient):
            self.__cognite_client = value
        else:
            raise AttributeError(
                "Can't set the CogniteClient reference to anything else than a CogniteClient instance or None"
            )

    def _get_cognite_client(self) -> CogniteClient | None:
        """Get Cognite client reference without raising (when missing)"""
        return self.__cognite_client


class CogniteResource(_WithClientMixin):
    __cognite_client: CogniteClient | None

    def __init__(self, cognite_client: CogniteClient | None = None) -> None:
        raise NotImplementedError

    def __eq__(self, other: Any) -> bool:
        return type(self) is type(other) and self.dump() == other.dump()

    def __str__(self) -> str:
        item = convert_time_attributes_to_datetime(self.dump())
        return json.dumps(item, default=json_dump_default, indent=4)

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to False.

        Returns:
            dict[str, Any]: A dictionary representation of the instance.
        """
        return basic_instance_dump(self, camel_case=camel_case)

    @classmethod
    def _load(
        cls: type[T_CogniteResource], resource: dict | str, cognite_client: CogniteClient | None = None
    ) -> T_CogniteResource:
        if isinstance(resource, dict):
            return fast_dict_load(cls, resource, cognite_client=cognite_client)
        elif isinstance(resource, str):
            return cls._load(json.loads(resource), cognite_client=cognite_client)
        raise TypeError(f"Resource must be json str or dict, not {type(resource)}")

    def to_pandas(
        self, expand: Sequence[str] = ("metadata",), ignore: list[str] | None = None, camel_case: bool = False
    ) -> pandas.DataFrame:
        """Convert the instance into a pandas DataFrame.

        Args:
            expand (Sequence[str]): List of row keys to expand, only works if the value is a Dict. Will expand metadata by default.
            ignore (list[str] | None): List of row keys to not include when converting to a data frame.
            camel_case (bool): Convert column names to camel case (e.g. `externalId` instead of `external_id`)

        Returns:
            pandas.DataFrame: The dataframe.
        """
        ignore = [] if ignore is None else ignore
        pd = cast(Any, local_import("pandas"))
        dumped = self.dump(camel_case=camel_case)

        for element in ignore:
            del dumped[element]
        for key in expand:
            if key in dumped:
                if isinstance(dumped[key], dict):
                    dumped.update(dumped.pop(key))
                else:
                    raise AssertionError(f"Could not expand attribute '{key}'")

        return pd.Series(dumped).to_frame(name="value")

    def _repr_html_(self) -> str:
        return notebook_display_with_fallback(self)


T_CogniteResource = TypeVar("T_CogniteResource", bound=CogniteResource)


class CognitePropertyClassUtil:
    @staticmethod
    def declare_property(schema_name: str) -> property:
        return (
            property(lambda s: s[schema_name] if schema_name in s else None)
            .setter(lambda s, v: CognitePropertyClassUtil._property_setter(s, schema_name, v))
            .deleter(lambda s: s.pop(schema_name, None))
        )

    @staticmethod
    def _property_setter(self: Any, schema_name: str, value: Any) -> None:
        if value is None:
            self.pop(schema_name, None)
        else:
            self[schema_name] = value


class CogniteResourceList(UserList, Generic[T_CogniteResource], _WithClientMixin):
    _RESOURCE: type[T_CogniteResource]
    __cognite_client: CogniteClient | None

    def __init__(self, resources: Collection[Any], cognite_client: CogniteClient | None = None) -> None:
        for resource in resources:
            if not isinstance(resource, self._RESOURCE):
                raise TypeError(
                    f"All resources for class '{self.__class__.__name__}' must be of type "
                    f"'{self._RESOURCE.__name__}', not '{type(resource)}'."
                )
        self._cognite_client = cast("CogniteClient", cognite_client)
        super().__init__(resources)
        self._id_to_item, self._external_id_to_item = {}, {}
        if self.data:
            if hasattr(self.data[0], "external_id"):
                self._external_id_to_item = {
                    item.external_id: item for item in self.data if item.external_id is not None
                }
            if hasattr(self.data[0], "id"):
                self._id_to_item = {item.id: item for item in self.data if item.id is not None}

    def pop(self, i: int = -1) -> T_CogniteResource:
        return super().pop(i)

    def __iter__(self) -> Iterator[T_CogniteResource]:
        return super().__iter__()

    @overload
    def __getitem__(self: T_CogniteResourceList, item: SupportsIndex) -> T_CogniteResource:
        ...

    @overload
    def __getitem__(self: T_CogniteResourceList, item: slice) -> T_CogniteResourceList:
        ...

    def __getitem__(
        self: T_CogniteResourceList, item: SupportsIndex | slice
    ) -> T_CogniteResource | T_CogniteResourceList:
        value = self.data[item]
        if isinstance(item, slice):
            return type(self)(value, cognite_client=self._get_cognite_client())
        return cast(T_CogniteResource, value)

    def __str__(self) -> str:
        item = convert_time_attributes_to_datetime(self.dump())
        return json.dumps(item, default=json_dump_default, indent=4)

    # TODO: We inherit a lot from UserList that we don't actually support...
    def extend(self, other: Collection[Any]) -> None:  # type: ignore [override]
        other_res_list = type(self)(other)  # See if we can accept the types
        if set(self._id_to_item).isdisjoint(other_res_list._id_to_item):
            super().extend(other)
            self._external_id_to_item.update(other_res_list._external_id_to_item)
            self._id_to_item.update(other_res_list._id_to_item)
        else:
            raise ValueError("Unable to extend as this would introduce duplicates")

    def dump(self, camel_case: bool = False) -> list[dict[str, Any]]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to False.

        Returns:
            list[dict[str, Any]]: A list of dicts representing the instance.
        """
        return [resource.dump(camel_case) for resource in self.data]

    def get(self, id: int | None = None, external_id: str | None = None) -> T_CogniteResource | None:
        """Get an item from this list by id or external_id.

        Args:
            id (int | None): The id of the item to get.
            external_id (str | None): The external_id of the item to get.

        Returns:
            T_CogniteResource | None: The requested item
        """
        IdentifierSequence.load(id, external_id).assert_singleton()
        if id:
            return self._id_to_item.get(id)
        return self._external_id_to_item.get(external_id)

    def to_pandas(
        self,
        camel_case: bool = False,
        expand_metadata: bool = False,
        metadata_prefix: str = "metadata.",
    ) -> pandas.DataFrame:
        """Convert the instance into a pandas DataFrame. Note that if the metadata column is expanded and there are
        keys in the metadata that already exist in the DataFrame, then an error will be raised by pd.join.

        Args:
            camel_case (bool): Convert column names to camel case (e.g. `externalId` instead of `external_id`)
            expand_metadata (bool): Expand the metadata column into separate columns.
            metadata_prefix (str): Prefix to use for metadata columns.

        Returns:
            pandas.DataFrame: The Cognite resource as a dataframe.
        """
        pd = cast(Any, local_import("pandas"))
        df = pd.DataFrame(self.dump(camel_case=camel_case))
        df = convert_nullable_int_cols(df, camel_case)

        if expand_metadata and "metadata" in df.columns:
            # Equivalent to pd.json_normalize(df["metadata"]) but is a faster implementation.
            meta_series = df.pop("metadata").dropna()
            meta_df = pd.DataFrame(meta_series.values.tolist(), index=meta_series.index).add_prefix(metadata_prefix)
            df = df.join(meta_df)
        return df

    def _repr_html_(self) -> str:
        return notebook_display_with_fallback(self)

    @classmethod
    def _load(
        cls: type[T_CogniteResourceList],
        resource_list: str | Iterable[dict[str, Any]],
        cognite_client: CogniteClient | None = None,
    ) -> T_CogniteResourceList:
        if isinstance(resource_list, str):
            return cls._load(json.loads(resource_list), cognite_client=cognite_client)
        elif isinstance(resource_list, Iterable):
            resources = [cls._RESOURCE._load(resource, cognite_client=cognite_client) for resource in resource_list]
            return cls(resources, cognite_client=cognite_client)
        else:
            raise NotImplementedError(f"Resource list must be iterable or json str, not {type(resource_list)}")


T_CogniteResourceList = TypeVar("T_CogniteResourceList", bound=CogniteResourceList)


@dataclass
class PropertySpec:
    name: str
    is_container: bool = False
    is_nullable: bool = True
    # Used to skip replace when the value is None
    is_beta: bool = False


class CogniteUpdate:
    def __init__(self, id: int | None = None, external_id: str | None = None) -> None:
        self._id = id
        self._external_id = external_id
        self._update_object: dict[str, Any] = {}

    def __eq__(self, other: Any) -> bool:
        return type(self) is type(other) and self.dump() == other.dump()

    def __str__(self) -> str:
        return json.dumps(self.dump(), default=json_dump_default, indent=4)

    def __repr__(self) -> str:
        return str(self)

    def _set(self, name: str, value: Any) -> None:
        update_obj = self._update_object.get(name, {})
        assert (
            "remove" not in update_obj and "add" not in update_obj
        ), "Can not call set after adding or removing fields from an update object."
        self._update_object[name] = {"set": value}

    def _set_null(self, name: str) -> None:
        self._update_object[name] = {"setNull": True}

    def _add(self, name: str, value: Any) -> None:
        update_obj = self._update_object.get(name, {})
        assert "set" not in update_obj, "Can not call remove or add fields after calling set on an update object."
        assert (
            "add" not in update_obj
        ), "Can not call add twice on the same object, please combine your objects and pass them to add in one call."
        update_obj["add"] = value
        self._update_object[name] = update_obj

    def _remove(self, name: str, value: Any) -> None:
        update_obj = self._update_object.get(name, {})
        assert "set" not in update_obj, "Can not call remove or add fields after calling set on an update object."
        assert (
            "remove" not in update_obj
        ), "Can not call remove twice on the same object, please combine your items and pass them to remove in one call."
        update_obj["remove"] = value
        self._update_object[name] = update_obj

    def _modify(self, name: str, value: Any) -> None:
        update_obj = self._update_object.get(name, {})
        assert "set" not in update_obj, "Can not call remove or add fields after calling set on an update object."
        assert (
            "modify" not in update_obj
        ), "Can not call modify twice on the same object, please combine your items and pass them to modify in one call."
        update_obj["modify"] = value
        self._update_object[name] = update_obj

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): No description.
        Returns:
            dict[str, Any]: A dictionary representation of the instance.
        """
        assert camel_case is True, "snake_case is currently unsupported"  # TODO maybe (when unifying classes)
        dumped: dict[str, Any] = {"update": self._update_object}
        if self._id is not None:
            dumped["id"] = self._id
        elif self._external_id is not None:
            dumped["externalId"] = self._external_id
        return dumped

    @classmethod
    @abstractmethod
    def _get_update_properties(cls) -> list[PropertySpec]:
        raise NotImplementedError


T_CogniteUpdate = TypeVar("T_CogniteUpdate", bound=CogniteUpdate)


class CognitePrimitiveUpdate(Generic[T_CogniteUpdate]):
    def __init__(self, update_object: T_CogniteUpdate, name: str) -> None:
        self._update_object = update_object
        self._name = name

    def _set(self, value: None | str | int | bool) -> T_CogniteUpdate:
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


class CogniteFilter:
    def __eq__(self, other: Any) -> bool:
        return type(self) is type(other) and self.dump() == other.dump()

    def __str__(self) -> str:
        item = convert_time_attributes_to_datetime(self.dump())
        return json.dumps(item, default=json_dump_default, indent=4)

    def __repr__(self) -> str:
        return str(self)

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to False.

        Returns:
            dict[str, Any]: A dictionary representation of the instance.
        """
        return basic_instance_dump(self, camel_case=camel_case)


T_CogniteFilter = TypeVar("T_CogniteFilter", bound=CogniteFilter)


class NoCaseConversionPropertyList(list):
    def as_reference(self) -> list[str]:
        return list(self)


class EnumProperty(Enum):
    @staticmethod
    def _generate_next_value_(name: str, *_: Any) -> str:
        # Allows the use of enum.auto() for member values avoiding camelCase typos
        return to_camel_case(name)

    def as_reference(self) -> list[str]:
        return [self.value]


class Geometry(dict):
    """Represents the points, curves and surfaces in the coordinate space.

    Args:
        type (Literal["Point", "MultiPoint", "LineString", "MultiLineString", "Polygon", "MultiPolygon"]): The geometry type.
        coordinates (list): An array of the coordinates of the geometry. The structure of the elements in this array is determined by the type of geometry.
        geometries (Collection[Geometry] | None): No description.

    Examples:
        Point:
            Coordinates of a point in 2D space, described as an array of 2 numbers.

            Example: `[4.306640625, 60.205710352530346]`

        LineString:
            Coordinates of a line described by a list of two or more points.
            Each point is defined as a pair of two numbers in an array, representing coordinates of a point in 2D space.

            Example: `[[30, 10], [10, 30], [40, 40]]`

        Polygon:
            List of one or more linear rings representing a shape.
            A linear ring is the boundary of a surface or the boundary of a hole in a surface. It is defined as a list consisting of 4 or more Points, where the first and last Point is equivalent.
            Each Point is defined as an array of 2 numbers, representing coordinates of a point in 2D space.

            Example: `[[[35, 10], [45, 45], [15, 40], [10, 20], [35, 10]], [[20, 30], [35, 35], [30, 20], [20, 30]]]`
            type: array

        MultiPoint:
            List of Points. Each Point is defined as an array of 2 numbers, representing coordinates of a point in 2D space.

            Example: `[[35, 10], [45, 45]]`

        MultiLineString:
                List of lines where each line (LineString) is defined as a list of two or more points.
                Each point is defined as a pair of two numbers in an array, representing coordinates of a point in 2D space.

                Example: `[[[30, 10], [10, 30]], [[35, 10], [10, 30], [40, 40]]]`

        MultiPolygon:
            List of multiple polygons.
            Each polygon is defined as a list of one or more linear rings representing a shape.
            A linear ring is the boundary of a surface or the boundary of a hole in a surface. It is defined as a list consisting of 4 or more Points, where the first and last Point is equivalent.
            Each Point is defined as an array of 2 numbers, representing coordinates of a point in 2D space.

            Example: `[[[[30, 20], [45, 40], [10, 40], [30, 20]]], [[[15, 5], [40, 10], [10, 20], [5, 10], [15, 5]]]]`
    """

    _VALID_TYPES = frozenset({"Point", "MultiPoint", "LineString", "MultiLineString", "Polygon", "MultiPolygon"})

    def __init__(
        self,
        type: Literal["Point", "MultiPoint", "LineString", "MultiLineString", "Polygon", "MultiPolygon"],
        coordinates: list,
        geometries: Collection[Geometry] | None = None,
    ) -> None:
        if type not in self._VALID_TYPES:
            raise ValueError(f"type must be one of {self._VALID_TYPES}")
        self.type = type
        self.coordinates = coordinates
        self.geometries = geometries

    type = CognitePropertyClassUtil.declare_property("type")
    coordinates = CognitePropertyClassUtil.declare_property("coordinates")

    @classmethod
    def _load(cls, raw_geometry: dict[str, Any]) -> Geometry:
        return cls(
            type=raw_geometry["type"],
            coordinates=raw_geometry["coordinates"],
            geometries=raw_geometry.get("geometries"),
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output = dict(convert_all_keys_to_camel_case(self) if camel_case else self)
        if self.geometries:
            output["geometries"] = [g.dump(camel_case) for g in self.geometries]
        else:
            output.pop("geometries", None)
        return output


SortableProperty: TypeAlias = Union[str, List[str], EnumProperty]


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
        return json.dumps(self.dump(), default=json_dump_default, indent=4)

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
            and data[2] in ["auto", "first", "last"]  # type: ignore[misc]
        ):
            return cls(
                property=data[0],
                order=data[1],
                nulls=data[2],  # type: ignore[misc]
            )
        elif isinstance(data, (str, list, EnumProperty)):
            return cls(property=data)
        else:
            raise ValueError(f"Unable to load {cls.__name__} from {data}")

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
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


class HasExternalAndInternalId(Protocol):
    @property
    def external_id(self) -> str | None:
        ...

    @property
    def id(self) -> int | None:
        ...


class IdTransformerMixin(Sequence[HasExternalAndInternalId], ABC):
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
