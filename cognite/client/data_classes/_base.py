from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections import UserList
from contextlib import contextmanager, suppress
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    NoReturn,
    Optional,
    Sequence,
    SupportsIndex,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

from cognite.client.exceptions import CogniteMissingClientError
from cognite.client.utils._auxiliary import fast_dict_load, json_dump_default, local_import
from cognite.client.utils._identifier import Identifier
from cognite.client.utils._pandas_helpers import convert_nullable_int_cols, notebook_display_with_fallback
from cognite.client.utils._text import convert_all_keys_to_camel_case
from cognite.client.utils._time import convert_time_attributes_to_datetime

if TYPE_CHECKING:
    import pandas

    from cognite.client import CogniteClient


class CogniteBase(ABC):
    @abstractmethod
    def __init__(self) -> None:
        raise NotImplementedError

    def __str__(self) -> str:
        item = convert_time_attributes_to_datetime(self.dump())
        return json.dumps(item, default=json_dump_default, indent=4)

    def __repr__(self) -> str:
        return str(self)

    def __eq__(self, other: Any) -> bool:
        return type(self) is type(other) and self.dump() == other.dump()

    def dump(self, camel_case: bool = False) -> Dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to False.

        Returns:
            Dict[str, Any]: A dictionary representation of the instance.
        """
        dumped = {k: v for k, v in vars(self).items() if v is not None and not k.startswith("_")}
        if camel_case:
            return convert_all_keys_to_camel_case(dumped)
        return dumped


T_CogniteBase = TypeVar("T_CogniteBase", bound=CogniteBase)


class CogniteBaseList(UserList):
    _RESOURCE: Type[T_CogniteBase]  # type: ignore [valid-type]

    def __str__(self) -> str:
        item = convert_time_attributes_to_datetime(self.dump())
        return json.dumps(item, default=json_dump_default, indent=4)

    def __repr__(self) -> str:
        return str(self)

    @classmethod
    def _load(cls: Type[T_CogniteBaseList], items: List[Dict[str, Any]]) -> T_CogniteBaseList:
        raise NotImplementedError

    def __init__(self, items: List[T_CogniteBase]) -> None:
        self._verify_items(items)
        super().__init__(items)
        self._init_lookup()

    def _verify_items(self, items: List[T_CogniteBase]) -> None:
        for item in items:
            if not isinstance(item, self._RESOURCE):
                raise TypeError(
                    f"All resources for class '{type(self).__name__}' must be of type "
                    f"'{self._RESOURCE}', not '{type(item)}'."
                )

    def _init_lookup(self) -> None:
        self._id_to_item, self._external_id_to_item = {}, {}
        if not self.data:
            return
        if hasattr(self.data[0], "external_id"):
            self._external_id_to_item = {item.external_id: item for item in self.data if item.external_id is not None}  # type: ignore [attr-defined]
        if hasattr(self.data[0], "id"):
            self._id_to_item = {item.id: item for item in self.data if item.id is not None}  # type: ignore [attr-defined]

    def _repr_html_(self) -> str:
        return notebook_display_with_fallback(self)

    def to_pandas(self, camel_case: bool = False) -> pandas.DataFrame:
        """Convert the instance into a pandas DataFrame.

        Args:
            camel_case (bool): Decide if the columns names should be camelCased. Defaults to False.

        Returns:
            pandas.DataFrame: The dataframe.
        """
        pd = cast(Any, local_import("pandas"))
        df = pd.DataFrame(self.dump(camel_case=camel_case))
        return convert_nullable_int_cols(df, camel_case)

    def dump(self, camel_case: bool = False) -> List[Dict[str, Any]]:
        """Dump the instance into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to False.

        Returns:
            List[Dict[str, Any]]: A list of dicts representing the instance.
        """
        return [resource.dump(camel_case) for resource in self.data]

    def get(self, id: int = None, external_id: str = None) -> Optional[T_CogniteBase]:
        """Get an item from this list by id or exernal_id. Specify either, but not both.

        Args:
            id (int): The id of the item to get.
            external_id (str): The external_id of the item to get.

        Returns:
            Optional[CogniteResource]: The requested item
        """
        if Identifier.of_either(id, external_id).is_id:
            return self._id_to_item.get(id)
        return self._external_id_to_item.get(external_id)

    @staticmethod
    def _get_item_id(item: Any) -> Optional[int]:
        try:
            return item.id
        except AttributeError:
            return None

    @staticmethod
    def _get_item_external_id(item: Any) -> Optional[str]:
        try:
            return item.external_id
        except AttributeError:
            return None

    def _get_identifiers(self, item: Any) -> Tuple[Optional[int], Optional[str]]:
        return self._get_item_id(item), self._get_item_external_id(item)

    def _remove_from_mappings(self, item: T_CogniteBase) -> None:
        id_, xid = self._get_identifiers(item)
        if id_ is not None:
            self._id_to_item.pop(id_, None)
        if xid is not None:
            self._external_id_to_item.pop(xid, None)

    def __contains__(self, item: Any) -> bool:
        if not isinstance(item, self._RESOURCE):
            return False
        id_, xid = self._get_identifiers(item)
        if id_ is not None and (self_item := self.get(id=id_) is not None):
            return item == self_item
        elif xid is not None and (self_item := self.get(external_id=xid) is not None):
            return item == self_item
        else:
            # O(1) lookups failed, search:
            return item in self.data

    def __setitem__(self, i: int, item: T_CogniteBase) -> None:  # type: ignore [override]
        if isinstance(i, slice):
            raise NotImplementedError
        elif not isinstance(item, self._RESOURCE):
            raise TypeError(f"item must be of type {self._RESOURCE}, not {type(item)}")
        self.data[i] = item

    def __delitem__(self, i: SupportsIndex | slice) -> None:
        to_del = self.data[i]
        if not isinstance(i, slice):
            to_del = [to_del]
        for item in to_del:
            self._remove_from_mappings(item)
        del self.data[i]

    def __iadd__(self: T_CogniteBaseList, other: Iterable[Any]) -> T_CogniteBaseList:
        self.extend(other)  # type: ignore [arg-type]
        return self

    def __mul__(self, n: int) -> NoReturn:
        """Cognite resource lists do not support ops that introduce duplicates, so __[,r,i]mul__ is not supported"""
        raise NotImplementedError

    __rmul__ = __mul__

    def __imul__(self, n: int) -> NoReturn:
        """Cognite resource lists do not support ops that introduce duplicates, so __[,r,i]mul__ is not supported"""
        raise NotImplementedError

    @contextmanager
    def _verify_and_update_item(self, item: Any) -> Iterator[None]:
        if not isinstance(item, self._RESOURCE):
            raise TypeError(f"item must be of type {self._RESOURCE}, not {type(item)}")

        id_, xid = self._get_identifiers(item)
        if id_ is not None and id_ in self._id_to_item or xid is not None and xid in self._external_id_to_item:
            raise ValueError("item id or external id already exists")
        # Add operation might fail, so we don't update internal mapping before it completes:
        yield
        self._id_to_item[id_] = item
        self._external_id_to_item[xid] = item

    def append(self, item: T_CogniteBase) -> None:
        with self._verify_and_update_item(item):
            self.data.append(item)

    def extend(self, other: List[T_CogniteBase]) -> None:  # type: ignore [override]
        other_res_list = type(self)(other)  # See if we can accept the types
        if set(self._id_to_item).isdisjoint(other_res_list._id_to_item):
            super().extend(other)
            self._external_id_to_item.update(other_res_list._external_id_to_item)
            self._id_to_item.update(other_res_list._id_to_item)
        else:
            raise ValueError("Unable to extend as this would introduce duplicates")

    def insert(self, i: int, item: T_CogniteBase) -> None:
        with self._verify_and_update_item(item):
            self.data.insert(i, item)

    def pop(self, i: int = -1) -> T_CogniteBase:  # type: ignore [type-var]
        item: T_CogniteBase = self.data.pop(i)  # raises if index out of range
        self._remove_from_mappings(item)
        return item

    def remove(self, item: Any) -> None:
        self.data.remove(item)  # raises if not in data
        self._remove_from_mappings(item)

    def clear(self) -> None:
        self.data.clear()
        self._id_to_item.clear()
        self._external_id_to_item.clear()


T_CogniteBaseList = TypeVar("T_CogniteBaseList", bound=CogniteBaseList)


class _WithClientMixin:
    @property
    def _cognite_client(self) -> CogniteClient:
        with suppress(AttributeError):
            if self.__cognite_client is not None:
                return self.__cognite_client
        raise CogniteMissingClientError(self)

    @_cognite_client.setter
    def _cognite_client(self, value: Optional[CogniteClient]) -> None:
        from cognite.client import CogniteClient

        if value is None or isinstance(value, CogniteClient):
            self.__cognite_client = value
        else:
            raise AttributeError(
                "Can't set the CogniteClient reference to anything else than a CogniteClient instance or None"
            )

    def _get_cognite_client(self) -> Optional[CogniteClient]:
        """Get Cognite client reference without raising (when missing)"""
        return self.__cognite_client


class CogniteResponse(CogniteBase):
    def to_pandas(self) -> pandas.DataFrame:
        raise NotImplementedError

    @classmethod
    def _load(cls: Type[T_CogniteResponse], item: Dict[str, Any]) -> T_CogniteResponse:
        raise NotImplementedError


T_CogniteResponse = TypeVar("T_CogniteResponse", bound=CogniteResponse)


class CogniteResponseList(Generic[T_CogniteResponse], CogniteBaseList):
    _RESOURCE: Type[T_CogniteResponse]

    @classmethod
    def _load(cls: Type[T_CogniteResponseList], items: List[Dict[str, Any]]) -> T_CogniteResponseList:
        if isinstance(items, list):
            return cls(list(map(cls._RESOURCE._load, items)))
        raise TypeError(f"The items to load must be a list (of dicts), not {type(items)}")


T_CogniteResponseList = TypeVar("T_CogniteResponseList", bound=CogniteResponseList)


class CogniteFilter(CogniteBase):
    ...


T_CogniteFilter = TypeVar("T_CogniteFilter", bound=CogniteFilter)


class CogniteResource(CogniteBase, _WithClientMixin):
    """A ``CogniteResource``, as opposed to ``CogniteResponse``, is any resource that needs access to an
    instantiated ``CogniteClient`` in order to implement helper methods, e.g. ``Asset.parent()``.

    Note:
        For historic reasons, this is not enforced, but should be a guideline for future additions.
    """

    __cognite_client: Optional[CogniteClient]

    def __init__(self, cognite_client: Optional[CogniteClient] = None) -> None:
        raise NotImplementedError

    @classmethod
    def _load(
        cls: Type[T_CogniteResource], item: Dict[str, Any], cognite_client: Optional[CogniteClient]
    ) -> T_CogniteResource:
        if isinstance(item, dict):
            return fast_dict_load(cls, item, cognite_client=cognite_client)
        raise TypeError(f"Item to load must be a mapping, not {type(item)}")

    def to_pandas(
        self, expand: Sequence[str] = ("metadata",), ignore: List[str] = None, camel_case: bool = False
    ) -> pandas.DataFrame:
        """Convert the instance into a pandas DataFrame.

        Args:
            expand (List[str]): List of row keys to expand, only works if the value is a Dict.
                Will expand metadata by default.
            ignore (List[str]): List of row keys to not include when converting to a data frame.
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

        df = pd.DataFrame(columns=["value"])
        for name, value in dumped.items():
            df.loc[name] = [value]
        return df

    def _repr_html_(self) -> str:
        return notebook_display_with_fallback(self)


T_CogniteResource = TypeVar("T_CogniteResource", bound=CogniteResource)


class CogniteResourceList(Generic[T_CogniteResource], CogniteBaseList, _WithClientMixin):
    _RESOURCE: Type[T_CogniteResource]
    __cognite_client: Optional[CogniteClient]

    def __init__(self, items: List[T_CogniteResource], cognite_client: Optional[CogniteClient] = None):
        super().__init__(items)
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(  # type: ignore [override]
        cls: Type[T_CogniteResourceList], items: List[Dict[str, Any]], cognite_client: Optional[CogniteClient]
    ) -> T_CogniteResourceList:
        if isinstance(items, list):
            resources = [cls._RESOURCE._load(res, cognite_client=cognite_client) for res in items]
            return cls(resources, cognite_client=cognite_client)
        raise TypeError(f"The items to load must be a list (of dicts), not {type(items)}")

    @overload  # type: ignore [override]
    # Generic[T] + UserList does not like this overload:
    def __getitem__(self: T_CogniteResourceList, item: int) -> T_CogniteResource:
        ...

    @overload
    def __getitem__(self: T_CogniteResourceList, item: slice) -> T_CogniteResourceList:
        ...

    def __getitem__(
        self: T_CogniteResourceList, item: Union[int, slice]
    ) -> Union[T_CogniteResource, T_CogniteResourceList]:
        value = self.data[item]
        if isinstance(item, slice):
            return type(self)(value, cognite_client=self._get_cognite_client())
        return cast(T_CogniteResource, value)


T_CogniteResourceList = TypeVar("T_CogniteResourceList", bound=CogniteResourceList)


class CogniteUpdate:
    def __init__(self, id: int = None, external_id: str = None) -> None:
        self._id = id
        self._external_id = external_id
        self._update_object: Dict[str, Any] = {}

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

    def dump(self, camel_case: bool = True) -> Dict[str, Any]:
        """Dump the instance into a json serializable Python data type.

        Returns:
            Dict[str, Any]: A dictionary representation of the instance.
        """
        assert camel_case is True, "snake_case is currently unsupported"  # TODO maybe (when unifying classes)
        dumped: Dict[str, Any] = {"update": self._update_object}
        if self._id is not None:
            dumped["id"] = self._id
        elif self._external_id is not None:
            dumped["externalId"] = self._external_id
        return dumped

    @classmethod
    def _get_update_properties(cls) -> List[str]:
        return [key for key in cls.__dict__ if not (key.startswith("_") or key == "columns")]


T_CogniteUpdate = TypeVar("T_CogniteUpdate", bound=CogniteUpdate)


class CognitePrimitiveUpdate(Generic[T_CogniteUpdate]):
    def __init__(self, update_object: T_CogniteUpdate, name: str):
        self._update_object = update_object
        self._name = name

    def _set(self, value: Union[None, str, int, bool]) -> T_CogniteUpdate:
        if value is None:
            self._update_object._set_null(self._name)
        else:
            self._update_object._set(self._name, value)
        return self._update_object


class CogniteObjectUpdate(Generic[T_CogniteUpdate]):
    def __init__(self, update_object: T_CogniteUpdate, name: str) -> None:
        self._update_object = update_object
        self._name = name

    def _set(self, value: Dict) -> T_CogniteUpdate:
        self._update_object._set(self._name, value)
        return self._update_object

    def _add(self, value: Dict) -> T_CogniteUpdate:
        self._update_object._add(self._name, value)
        return self._update_object

    def _remove(self, value: List) -> T_CogniteUpdate:
        self._update_object._remove(self._name, value)
        return self._update_object


class CogniteListUpdate(Generic[T_CogniteUpdate]):
    def __init__(self, update_object: T_CogniteUpdate, name: str):
        self._update_object = update_object
        self._name = name

    def _set(self, value: List) -> T_CogniteUpdate:
        self._update_object._set(self._name, value)
        return self._update_object

    def _add(self, value: List) -> T_CogniteUpdate:
        self._update_object._add(self._name, value)
        return self._update_object

    def _remove(self, value: List) -> T_CogniteUpdate:
        self._update_object._remove(self._name, value)
        return self._update_object

    def _modify(self, value: List) -> T_CogniteUpdate:
        self._update_object._modify(self._name, value)
        return self._update_object


class CogniteLabelUpdate(Generic[T_CogniteUpdate]):
    def __init__(self, update_object: T_CogniteUpdate, name: str) -> None:
        self._update_object = update_object
        self._name = name

    def _set(self, external_ids: Union[str, List[str]]) -> T_CogniteUpdate:
        self._update_object._set(self._name, self._wrap_ids(self._wrap_in_list(external_ids)))
        return self._update_object

    def _add(self, external_ids: Union[str, List[str]]) -> T_CogniteUpdate:
        self._update_object._add(self._name, self._wrap_ids(self._wrap_in_list(external_ids)))
        return self._update_object

    def _remove(self, external_ids: Union[str, List[str]]) -> T_CogniteUpdate:
        self._update_object._remove(self._name, self._wrap_ids(self._wrap_in_list(external_ids)))
        return self._update_object

    def _wrap_ids(self, external_ids: List[str]) -> List[Dict[str, str]]:
        return [{"externalId": external_id} for external_id in external_ids]

    def _wrap_in_list(self, external_ids: Union[str, List[str]]) -> List[str]:
        return external_ids if isinstance(external_ids, list) else [external_ids]


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
