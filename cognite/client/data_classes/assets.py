from __future__ import annotations

import functools
import itertools
import operator as op
import textwrap
import threading
import warnings
from collections import Counter, defaultdict
from functools import lru_cache
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Collection,
    Dict,
    List,
    Literal,
    Optional,
    Sequence,
    TextIO,
    Union,
    cast,
)

from graphlib import TopologicalSorter
from typing_extensions import TypeAlias

from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteLabelUpdate,
    CogniteListUpdate,
    CogniteObjectUpdate,
    CognitePrimitiveUpdate,
    CognitePropertyClassUtil,
    CogniteResource,
    CogniteResourceList,
    CogniteSort,
    CogniteUpdate,
    EnumProperty,
    IdTransformerMixin,
    PropertySpec,
)
from cognite.client.data_classes.labels import Label, LabelDefinition, LabelFilter
from cognite.client.data_classes.shared import GeoLocation, GeoLocationFilter, TimestampRange
from cognite.client.exceptions import CogniteAssetHierarchyError
from cognite.client.utils._auxiliary import split_into_chunks
from cognite.client.utils._concurrency import execute_tasks
from cognite.client.utils._graph import find_all_cycles_with_elements
from cognite.client.utils._text import DrawTables, shorten

if TYPE_CHECKING:
    import pandas

    from cognite.client import CogniteClient
    from cognite.client.data_classes import EventList, FileMetadataList, SequenceList, TimeSeriesList
    from cognite.client.data_classes._base import T_CogniteResource, T_CogniteResourceList


class AssetAggregate(dict):
    """Aggregation group of assets

    Args:
        count (int | None): Size of the aggregation group
        **kwargs (Any): No description.
    """

    def __init__(self, count: int | None = None, **kwargs: Any) -> None:
        self.count = count
        self.update(kwargs)

    count = CognitePropertyClassUtil.declare_property("count")


class AggregateResultItem(dict):
    """Aggregated metrics of the asset

    Args:
        child_count (int | None): Number of direct descendants for the asset
        depth (int | None): Asset path depth (number of levels below root node).
        path (list[dict[str, Any]] | None): IDs of assets on the path to the asset.
        **kwargs (Any): No description.
    """

    def __init__(
        self,
        child_count: int | None = None,
        depth: int | None = None,
        path: list[dict[str, Any]] | None = None,
        **kwargs: Any,
    ) -> None:
        self.child_count = child_count
        self.depth = depth
        self.path = path
        self.update(kwargs)

    child_count = CognitePropertyClassUtil.declare_property("childCount")
    depth = CognitePropertyClassUtil.declare_property("depth")
    path = CognitePropertyClassUtil.declare_property("path")


class Asset(CogniteResource):
    """A representation of a physical asset, for example a factory or a piece of equipment.

    Args:
        external_id (str | None): The external ID provided by the client. Must be unique for the resource type.
        name (str | None): The name of the asset.
        parent_id (int | None): The parent of the node, null if it is the root node.
        parent_external_id (str | None): The external ID of the parent. The property is omitted if the asset doesn't have a parent or if the parent doesn't have externalId.
        description (str | None): The description of the asset.
        data_set_id (int | None): The id of the dataset this asset belongs to.
        metadata (dict[str, str] | None): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 10240 bytes, up to 256 key-value pairs, of total size at most 10240.
        source (str | None): The source of the asset.
        labels (list[Label | str | LabelDefinition | dict] | None): A list of the labels associated with this resource item.
        geo_location (GeoLocation | None): The geographic metadata of the asset.
        id (int | None): A server-generated ID for the object.
        created_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int | None): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        root_id (int | None): ID of the root asset.
        aggregates (dict[str, Any] | AggregateResultItem | None): Aggregated metrics of the asset
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: str | None = None,
        name: str | None = None,
        parent_id: int | None = None,
        parent_external_id: str | None = None,
        description: str | None = None,
        data_set_id: int | None = None,
        metadata: dict[str, str] | None = None,
        source: str | None = None,
        labels: list[Label | str | LabelDefinition | dict] | None = None,
        geo_location: GeoLocation | None = None,
        id: int | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
        root_id: int | None = None,
        aggregates: dict[str, Any] | AggregateResultItem | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        if geo_location is not None and not isinstance(geo_location, GeoLocation):
            raise TypeError("Asset.geo_location should be of type GeoLocation")
        self.external_id = external_id
        self.name = name
        self.parent_id = parent_id
        self.parent_external_id = parent_external_id
        self.description = description
        self.data_set_id = data_set_id
        self.metadata = metadata
        self.source = source
        self.labels = Label._load_list(labels)
        self.geo_location = geo_location
        self.id = id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.root_id = root_id
        self.aggregates = aggregates
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> Asset:
        instance = super()._load(resource, cognite_client)
        if isinstance(resource, dict) and instance.aggregates is not None:
            instance.aggregates = AggregateResultItem(**instance.aggregates)
        instance.labels = Label._load_list(instance.labels)
        if instance.geo_location is not None:
            instance.geo_location = GeoLocation._load(instance.geo_location)
        return instance

    def __hash__(self) -> int:
        return hash(self.external_id)

    def parent(self) -> Asset:
        """Returns this asset's parent.

        Returns:
            Asset: The parent asset.
        """
        if self.parent_id is None:
            raise ValueError("parent_id is None, is this a root asset?")
        return cast(Asset, self._cognite_client.assets.retrieve(id=self.parent_id))

    def children(self) -> AssetList:
        """Returns the children of this asset.

        Returns:
            AssetList: The requested assets
        """
        assert self.id is not None
        return self._cognite_client.assets.list(parent_ids=[self.id], limit=None)

    def subtree(self, depth: int | None = None) -> AssetList:
        """Returns the subtree of this asset up to a specified depth.

        Args:
            depth (int | None): Retrieve assets up to this depth below the asset.

        Returns:
            AssetList: The requested assets sorted topologically.
        """
        assert self.id is not None
        return self._cognite_client.assets.retrieve_subtree(id=self.id, depth=depth)

    def time_series(self, **kwargs: Any) -> TimeSeriesList:
        """Retrieve all time series related to this asset.

        Args:
            **kwargs (Any): All extra keyword arguments are passed to time_series/list. NB: 'asset_ids' can't be used.
        Returns:
            TimeSeriesList: All time series related to this asset.
        """
        assert self.id is not None
        return self._cognite_client.time_series.list(asset_ids=[self.id], **kwargs)

    def sequences(self, **kwargs: Any) -> SequenceList:
        """Retrieve all sequences related to this asset.

        Args:
            **kwargs (Any): All extra keyword arguments are passed to sequences/list. NB: 'asset_ids' can't be used.
        Returns:
            SequenceList: All sequences related to this asset.
        """
        assert self.id is not None
        return self._cognite_client.sequences.list(asset_ids=[self.id], **kwargs)

    def events(self, **kwargs: Any) -> EventList:
        """Retrieve all events related to this asset.

        Args:
            **kwargs (Any): All extra keyword arguments are passed to events/list. NB: 'asset_ids' can't be used.
        Returns:
            EventList: All events related to this asset.
        """
        assert self.id is not None
        return self._cognite_client.events.list(asset_ids=[self.id], **kwargs)

    def files(self, **kwargs: Any) -> FileMetadataList:
        """Retrieve all files metadata related to this asset.

        Args:
            **kwargs (Any): All extra keyword arguments are passed to files/list. NB: 'asset_ids' can't be used.
        Returns:
            FileMetadataList: Metadata about all files related to this asset.
        """
        assert self.id is not None
        return self._cognite_client.files.list(asset_ids=[self.id], **kwargs)

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        result = super().dump(camel_case)
        if self.labels is not None:
            result["labels"] = [label.dump(camel_case) for label in self.labels]
        return result

    def to_pandas(
        self,
        expand: Sequence[str] = ("metadata", "aggregates"),
        ignore: list[str] | None = None,
        camel_case: bool = False,
    ) -> pandas.DataFrame:
        """Convert the instance into a pandas DataFrame.

        Args:
            expand (Sequence[str]): List of row keys to expand, only works if the value is a Dict.
            ignore (list[str] | None): List of row keys to not include when converting to a data frame.
            camel_case (bool): Convert column names to camel case (e.g. `externalId` instead of `external_id`)

        Returns:
            pandas.DataFrame: The dataframe.
        """
        return super().to_pandas(expand=expand, ignore=ignore, camel_case=camel_case)


class AssetUpdate(CogniteUpdate):
    """Changes applied to asset

    Args:
        id (int): A server-generated ID for the object.
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
    """

    class _PrimitiveAssetUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> AssetUpdate:
            return self._set(value)

    class _ObjectAssetUpdate(CogniteObjectUpdate):
        def set(self, value: dict) -> AssetUpdate:
            return self._set(value)

        def add(self, value: dict) -> AssetUpdate:
            return self._add(value)

        def remove(self, value: list) -> AssetUpdate:
            return self._remove(value)

    class _ListAssetUpdate(CogniteListUpdate):
        def set(self, value: list) -> AssetUpdate:
            return self._set(value)

        def add(self, value: list) -> AssetUpdate:
            return self._add(value)

        def remove(self, value: list) -> AssetUpdate:
            return self._remove(value)

    class _LabelAssetUpdate(CogniteLabelUpdate):
        def set(self, value: str | list[str]) -> AssetUpdate:
            return self._set(value)

        def add(self, value: str | list[str]) -> AssetUpdate:
            return self._add(value)

        def remove(self, value: str | list[str]) -> AssetUpdate:
            return self._remove(value)

    @property
    def external_id(self) -> AssetUpdate._PrimitiveAssetUpdate:
        return AssetUpdate._PrimitiveAssetUpdate(self, "externalId")

    @property
    def name(self) -> _PrimitiveAssetUpdate:
        return AssetUpdate._PrimitiveAssetUpdate(self, "name")

    @property
    def description(self) -> _PrimitiveAssetUpdate:
        return AssetUpdate._PrimitiveAssetUpdate(self, "description")

    @property
    def data_set_id(self) -> _PrimitiveAssetUpdate:
        return AssetUpdate._PrimitiveAssetUpdate(self, "dataSetId")

    @property
    def metadata(self) -> _ObjectAssetUpdate:
        return AssetUpdate._ObjectAssetUpdate(self, "metadata")

    @property
    def source(self) -> _PrimitiveAssetUpdate:
        return AssetUpdate._PrimitiveAssetUpdate(self, "source")

    @property
    def parent_id(self) -> _PrimitiveAssetUpdate:
        return AssetUpdate._PrimitiveAssetUpdate(self, "parentId")

    @property
    def parent_external_id(self) -> _PrimitiveAssetUpdate:
        return AssetUpdate._PrimitiveAssetUpdate(self, "parentExternalId")

    @property
    def labels(self) -> _LabelAssetUpdate:
        return AssetUpdate._LabelAssetUpdate(self, "labels")

    @property
    def geo_location(self) -> _PrimitiveAssetUpdate:
        return AssetUpdate._PrimitiveAssetUpdate(self, "geoLocation")

    @classmethod
    def _get_update_properties(cls) -> list[PropertySpec]:
        return [
            # External ID is nullable, but is used in the upsert logic and thus cannot be nulled out.
            PropertySpec("external_id", is_nullable=False),
            PropertySpec("name", is_nullable=False),
            PropertySpec("description"),
            PropertySpec("data_set_id"),
            PropertySpec("metadata", is_container=True),
            PropertySpec("source"),
            PropertySpec("parent_id", is_nullable=False),
            PropertySpec("parent_external_id", is_nullable=False),
            PropertySpec("labels", is_container=True),
            PropertySpec("geo_location"),
        ]


class AssetList(CogniteResourceList[Asset], IdTransformerMixin):
    _RESOURCE = Asset

    def __init__(self, resources: Collection[Any], cognite_client: CogniteClient | None = None) -> None:
        super().__init__(resources, cognite_client)
        self._retrieve_chunk_size = 100

    def time_series(self) -> TimeSeriesList:
        """Retrieve all time series related to these assets.

        Returns:
            TimeSeriesList: All time series related to the assets in this AssetList.
        """
        from cognite.client.data_classes import TimeSeriesList

        return self._retrieve_related_resources(TimeSeriesList, self._cognite_client.time_series)

    def sequences(self) -> SequenceList:
        """Retrieve all sequences related to these assets.

        Returns:
            SequenceList: All sequences related to the assets in this AssetList.
        """
        from cognite.client.data_classes import SequenceList

        return self._retrieve_related_resources(SequenceList, self._cognite_client.sequences)

    def events(self) -> EventList:
        """Retrieve all events related to these assets.

        Returns:
            EventList: All events related to the assets in this AssetList.
        """
        from cognite.client.data_classes import EventList

        return self._retrieve_related_resources(EventList, self._cognite_client.events)

    def files(self) -> FileMetadataList:
        """Retrieve all files metadata related to these assets.

        Returns:
            FileMetadataList: Metadata about all files related to the assets in this AssetList.
        """
        from cognite.client.data_classes import FileMetadataList

        return self._retrieve_related_resources(FileMetadataList, self._cognite_client.files)

    def _retrieve_related_resources(
        self, resource_list_class: type[T_CogniteResourceList], resource_api: Any
    ) -> T_CogniteResourceList:
        seen: set[int] = set()
        add_to_seen = seen.add
        lock = threading.Lock()

        def retrieve_and_deduplicate(asset_ids: list[int]) -> list[T_CogniteResource]:
            res = resource_api.list(asset_ids=asset_ids, limit=-1)
            with lock:
                return [r for r in res if not (r.id in seen or add_to_seen(r.id))]

        ids = [a.id for a in self.data]
        tasks = [{"asset_ids": chunk} for chunk in split_into_chunks(ids, self._retrieve_chunk_size)]
        res_list = execute_tasks(retrieve_and_deduplicate, tasks, resource_api._config.max_workers).results
        return resource_list_class(list(itertools.chain.from_iterable(res_list)), cognite_client=self._cognite_client)


class AssetFilter(CogniteFilter):
    """Filter on assets with strict matching.

    Args:
        name (str | None): The name of the asset.
        parent_ids (Sequence[int] | None): Return only the direct descendants of the specified assets.
        parent_external_ids (Sequence[str] | None): Return only the direct descendants of the specified assets.
        asset_subtree_ids (Sequence[dict[str, Any]] | None): Only include assets in subtrees rooted at the specified assets (including the roots given). If the total size of the given subtrees exceeds 100,000 assets, an error will be returned.
        data_set_ids (Sequence[dict[str, Any]] | None): No description.
        metadata (dict[str, str] | None): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 128 bytes, value 10240 bytes, up to 256 key-value pairs, of total size at most 10240.
        source (str | None): The source of the asset.
        created_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
        last_updated_time (dict[str, Any] | TimestampRange | None): Range between two timestamps.
        root (bool | None): Whether the filtered assets are root assets, or not. Set to True to only list root assets.
        external_id_prefix (str | None): Filter by this (case-sensitive) prefix for the external ID.
        labels (LabelFilter | None): Return only the resource matching the specified label constraints.
        geo_location (GeoLocationFilter | None): Only include files matching the specified geographic relation.
    """

    def __init__(
        self,
        name: str | None = None,
        parent_ids: Sequence[int] | None = None,
        parent_external_ids: Sequence[str] | None = None,
        asset_subtree_ids: Sequence[dict[str, Any]] | None = None,
        data_set_ids: Sequence[dict[str, Any]] | None = None,
        metadata: dict[str, str] | None = None,
        source: str | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        root: bool | None = None,
        external_id_prefix: str | None = None,
        labels: LabelFilter | None = None,
        geo_location: GeoLocationFilter | None = None,
    ) -> None:
        self.name = name
        self.parent_ids = parent_ids
        self.parent_external_ids = parent_external_ids
        self.asset_subtree_ids = asset_subtree_ids
        self.data_set_ids = data_set_ids
        self.metadata = metadata
        self.source = source
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.root = root
        self.external_id_prefix = external_id_prefix
        self.labels = labels
        self.geo_location = geo_location

        if labels is not None and not isinstance(labels, LabelFilter):
            raise TypeError("AssetFilter.labels must be of type LabelFilter")

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        result = super().dump(camel_case)
        if isinstance(self.labels, LabelFilter):
            result["labels"] = self.labels.dump(camel_case)
        return result


class AssetHierarchy:
    """Class that verifies if a collection of Assets is valid, by validating its internal consistency.
    This is done "offline", meaning CDF is -not- queried for the already existing assets. As a result,
    any asset providing a parent link by ID instead of external ID, are assumed valid.

    Args:
        assets (Sequence[Asset]): Sequence of assets to be inspected for validity.
        ignore_orphans (bool): If true, orphan assets are assumed valid and won't raise.

    Examples:

        >>> from cognite.client import CogniteClient
        >>> from cognite.client.data_classes import AssetHierarchy
        >>> client = CogniteClient()
        >>> hierarchy = AssetHierarchy(assets)
        >>> # Get a report written to the terminal listing any issues:
        >>> hierarchy.validate_and_report()
        >>> if hierarchy.is_valid():
        ...     res = client.assets.create_hierarchy(hierarchy)
        ... # If there are issues, you may inspect them directly:
        ... else:
        ...     hierarchy.orphans
        ...     hierarchy.invalid
        ...     hierarchy.unsure_parents
        ...     hierarchy.duplicates
        ...     hierarchy.cycles  # Requires no other basic issues

        There are other ways to generate the report than to write directly to screen. You may pass an
        ``output_file`` which can be either a ``Path`` object (writes are done in append-mode) or a
        file-like object supporting ``write`` (default is ``None`` which is just regular ``print``):

        >>> # Get a report written to file:
        >>> from pathlib import Path
        >>> report = Path("path/to/my_report.txt")
        >>> hierarchy = AssetHierarchy(assets)
        >>> hierarchy.validate_and_report(output_file=report)

        >>> # Get a report as text "in memory":
        >>> import io
        >>> with io.StringIO() as file_like:
        ...     hierarchy.validate_and_report(output_file=file_like)
        ...     report = file_like.getvalue()
    """

    def __init__(self, assets: Sequence[Asset], ignore_orphans: bool = False) -> None:
        self._assets = assets
        self._roots: list[Asset] | None = None
        self._orphans: list[Asset] | None = None
        self._ignore_orphans = ignore_orphans
        self._invalid: list[Asset] | None = None
        self._unsure_parents: list[Asset] | None = None
        self._duplicates: dict[str, list[Asset]] | None = None
        self._cycles: list[list[str]] | None = None

        self.__validation_has_run = False

    def __len__(self) -> int:
        return len(self._assets)

    def is_valid(self, on_error: Literal["ignore", "warn", "raise"] = "ignore") -> bool:
        if not self.__validation_has_run:
            self.validate(verbose=False, on_error="ignore")
            return self.is_valid(on_error=on_error)

        elif self._no_basic_issues() and not self._cycles:
            return True

        self._on_error(on_error, "Asset hierarchy is not valid.")
        return False

    @property
    def roots(self) -> AssetList:
        if self._roots is None:
            raise RuntimeError("Unable to list root assets before validation has run")
        return AssetList(self._roots)

    @property
    def orphans(self) -> AssetList:
        if self._orphans is None:  # Note: The option 'ignore_orphans' has no impact on this
            raise RuntimeError("Unable to list orphan assets before validation has run")
        return AssetList(self._orphans)

    @property
    def invalid(self) -> AssetList:
        if self._invalid is None:
            raise RuntimeError("Unable to list assets invalid attributes before validation has run")
        return AssetList(self._invalid)

    @property
    def unsure_parents(self) -> AssetList:
        if self._unsure_parents is None:
            raise RuntimeError("Unable to list assets with unsure parent link before validation has run")
        return AssetList(self._unsure_parents)

    @property
    def duplicates(self) -> dict[str, list[Asset]]:
        if self._duplicates is None:
            raise RuntimeError("Unable to list duplicate assets before validation has run")
        # NB: Do not return AssetList (as it does not handle duplicates well):
        return {xid: assets for xid, assets in self._duplicates.items()}

    @property
    def cycles(self) -> list[list[str]]:
        if self._cycles is None:
            if self.__validation_has_run:
                self._preconditions_for_cycle_check_are_met(on_error="raise")
            raise RuntimeError("Unable to list cycles before validation has run")
        return self._cycles

    def validate(
        self,
        verbose: bool = False,
        output_file: Path | None = None,
        on_error: Literal["ignore", "warn", "raise"] = "warn",
    ) -> AssetHierarchy:
        self._roots, self._orphans, self._invalid, self._unsure_parents, self._duplicates = self._inspect_attributes()
        if verbose:
            self._report_on_identifiers(output_file)

        if self._preconditions_for_cycle_check_are_met("ignore"):
            cycle_subtree_size, self._cycles = self._locate_cycles()
            if verbose:
                self._report_on_cycles(output_file, cycle_subtree_size)
        elif verbose:
            self.print_to(
                "\nUnable to check for cyclical references before above issues are fixed", output_file=output_file
            )

        self.__validation_has_run = True
        self.is_valid(on_error=on_error)
        return self

    def validate_and_report(self, output_file: Path | None = None) -> AssetHierarchy:
        return self.validate(verbose=True, output_file=output_file, on_error="ignore")

    def groupby_parent_xid(self) -> dict[str | None, list[Asset]]:
        """Returns a mapping from parent external ID to a list of its direct children.

        Note:
            If the AssetHierarchy was initialized with `ignore_orphans=True`, all orphans assets,
            if any, are returned as part of the root assets in the mapping and can be accessed by
            `mapping[None]`.
            The same is true for all assets linking its parent by ID.

        Returns:
            dict[str | None, list[Asset]]: No description."""
        self.is_valid(on_error="raise")

        # Sort (on parent) as required by groupby. This is tricky as we need to avoid comparing string with None,
        # and can't simply hash it because of the possibility of collisions. Further, the empty string is a valid
        # external ID leaving no other choice than to prepend all strings with ' ' before comparison:

        def parent_sort_fn(asset: Asset) -> str:
            # All assets using 'parent_id' will be grouped together with the root assets:
            if (pxid := asset.parent_external_id) is None:
                return ""
            return f" {pxid}"

        parent_sorted = sorted(self._assets, key=parent_sort_fn)
        mapping = {
            parent: list(child_assets)
            for parent, child_assets in itertools.groupby(parent_sorted, key=op.attrgetter("parent_external_id"))
        }
        if not (self._ignore_orphans and self._orphans):
            return mapping

        mapping.setdefault(None, [])
        mapping[None].extend(
            itertools.chain.from_iterable(
                mapping.pop(parent, [])
                for parent in {a.parent_external_id for a in self._orphans}
                if parent is not None
            )
        )
        return mapping

    def count_subtree(self, mapping: dict[str | None, list[Asset]]) -> dict[str, int]:
        """Returns a mapping from asset external ID to the size of its subtree (children and children of children etc.).

        Args:
            mapping (dict[str | None, list[Asset]]): The mapping returned by `groupby_parent_xid()`. If None is passed, will be recreated (slightly expensive).

        Returns:
            dict[str, int]: Lookup from external ID to descendant count.
        """
        if mapping is None:
            mapping = self.groupby_parent_xid()

        @lru_cache(None)
        def _count_subtree(xid: str, count: int = 0) -> int:
            for child in mapping.get(xid, []):
                count += 1 + _count_subtree(child.external_id)
            return count

        # Avoid recursion error by counting in reverse topological order ("bottom up"):
        sorter = TopologicalSorter(
            {xid: [asset.external_id for asset in children] for xid, children in mapping.items()}
        )
        counts = [(parent, _count_subtree(parent)) for parent in sorter.static_order()]
        counts.sort(key=lambda args: -args[-1])
        # The count for the fictitious "root of roots" is just len(assets), so we remove it:
        (count_dct := dict(counts)).pop(None, None)
        return count_dct

    def _on_error(self, on_error: Literal["ignore", "warn", "raise"], message: str) -> None:
        if on_error == "warn":
            warnings.warn(message + " See report for details: `validate_and_report()`")
        elif on_error == "raise":
            raise CogniteAssetHierarchyError(message, hierarchy=self)

    def _no_basic_issues(self) -> bool:
        return not (
            self._invalid or self._unsure_parents or self._duplicates or (self._orphans and not self._ignore_orphans)
        )

    def _inspect_attributes(self) -> tuple[list[Asset], list[Asset], list[Asset], list[Asset], dict[str, list[Asset]]]:
        invalid, orphans, roots, unsure_parents, duplicates = [], [], [], [], defaultdict(list)
        xid_count = Counter(a.external_id for a in self._assets)

        for asset in self._assets:
            id_, xid, name = asset.id, asset.external_id, asset.name
            if xid is None or name is None or len(name) < 1 or id_ is not None:
                invalid.append(asset)
                continue  # Don't report invalid as part of any other group

            if xid_count[xid] > 1:
                duplicates[xid].append(asset)

            has_parent_id = asset.parent_id is not None
            has_parent_xid = (parent_xid := asset.parent_external_id) is not None

            if not has_parent_xid and not has_parent_id:
                roots.append(asset)

            elif has_parent_xid:
                if has_parent_id:  # Both parent links are given
                    unsure_parents.append(asset)

                elif parent_xid not in xid_count:  # Only parent XID given, but not part of assets given
                    orphans.append(asset)

            # Only case left is when parent is only given by ID, which we assume is valid

        return roots, orphans, invalid, unsure_parents, dict(duplicates)

    def _preconditions_for_cycle_check_are_met(self, on_error: Literal["ignore", "warn", "raise"]) -> bool:
        if self._no_basic_issues():
            return True
        self._on_error(on_error, "Unable to run cycle-check before basic issues are fixed.")
        return False

    def _locate_cycles(self) -> tuple[int, list[list[str]]]:
        has_cycles = set()
        no_cycles = {None, *(a.external_id for a in self._roots or [])}
        edges = cast(Dict[str, Optional[str]], {a.external_id: a.parent_external_id for a in self._assets})

        if self._ignore_orphans:
            no_cycles |= {a.parent_external_id for a in cast(List[Asset], self._orphans)}

        for xid, parent in edges.items():
            if parent in no_cycles:
                no_cycles.add(xid)

            elif parent in has_cycles or xid == parent:
                has_cycles.add(xid)
            else:
                self._cycle_search(xid, parent, edges, no_cycles, has_cycles)

        return len(has_cycles), find_all_cycles_with_elements(has_cycles, edges)

    @staticmethod
    def _cycle_search(
        xid: str,
        parent: str | None,
        edges: dict[str, str | None],
        no_cycles: set[str | None],
        has_cycles: set[str],
    ) -> None:
        seen = {xid}
        while True:
            xid, parent = parent, edges[parent]  # type: ignore [assignment, index]
            seen.add(xid)
            if parent in no_cycles:
                no_cycles |= seen
                return
            elif parent in seen:
                has_cycles |= seen
                return

    @staticmethod
    def print_to(*args: Any, output_file: Path | TextIO | None) -> None:
        out = "\n".join(s.rstrip() for s in map(str, args))
        if output_file is None:
            print(out)  # noqa: T201
        elif isinstance(output_file, Path):
            with output_file.open("a", encoding="utf-8") as file:
                print(out, file=file)
        else:
            try:
                # There are no isinstance checks for TextIO
                output_file.write(out + "\n")
            except Exception as e:
                raise TypeError("Unable to write to `output_file`, a file-like object is required") from e

    def _report_on_identifiers(self, output_file: Path | None) -> None:
        print_fn = functools.partial(self.print_to, output_file=output_file)

        def print_header(title: str, columns: list[str]) -> None:
            print_fn(
                title,
                DrawTables.TOPLINE.join(DrawTables.HLINE * 20 for _ in columns),
                DrawTables.VLINE.join(f"{col:^20}" for col in columns),
                DrawTables.XLINE.join(DrawTables.HLINE * 20 for _ in columns),
            )

        def print_table(lst: list[Asset], columns: list[str]) -> None:
            for entry in lst:
                cols = (f"{shorten(getattr(entry, col)):<20}" for col in columns)
                print_fn(DrawTables.VLINE.join(cols))

        add_vspace = False

        def maybe_add_vspace() -> None:
            nonlocal add_vspace
            if add_vspace:
                print_fn()
            else:
                add_vspace = True

        cols_with_parent_id = ["External ID", "Parent ID", "Parent external ID", "Name"]
        attrs_with_parent_id = ["external_id", "parent_id", "parent_external_id", "name"]

        if self._invalid:
            maybe_add_vspace()
            print_header("Invalid assets (no name, external ID, or with ID set):", cols_with_parent_id)
            print_table(self._invalid, attrs_with_parent_id)

        if self._unsure_parents:
            maybe_add_vspace()
            print_header("Assets with parent link given by both ID and external ID:", cols_with_parent_id)
            print_table(self._unsure_parents, attrs_with_parent_id)

        cols_with_description = ["External ID", "Parent external ID", "Name", "Description"]
        attrs_with_description = ["external_id", "parent_external_id", "name", "description"]

        if self._orphans and not self._ignore_orphans:
            maybe_add_vspace()
            print_header("Orphan assets (parent ext. ID not part of given assets)", cols_with_description)
            print_table(self._orphans, attrs_with_description)

        if self._duplicates:
            maybe_add_vspace()
            print_header("Assets with duplicated external ID:", cols_with_description)
            for dupe_assets in self._duplicates.values():
                print_table(dupe_assets, attrs_with_description)

    def _report_on_cycles(self, output_file: Path | None, cycle_subtree_size: int) -> None:
        if not (cycles := self._cycles):
            return

        n_cycles, n_cycle_assets = len(cycles), sum(map(len, cycles))
        n_subtree_cycle_assets = cycle_subtree_size - n_cycle_assets

        print_fn = functools.partial(self.print_to, output_file=output_file)
        print_fn(
            "Asset hierarchy had cyclical references:",
            f"- {n_cycles} cycles",
            f"- {n_cycle_assets} assets part of a cycle",
            f"- {n_subtree_cycle_assets} non-cycle assets connected to a cycle asset",
            DrawTables.HLINE * 83,
        )
        for i, cycle in enumerate(cycles, 1):
            print_fn(
                f"Cycle {i}/{n_cycles}:",
                textwrap.fill(" -> ".join(map(repr, cycle)), width=80, break_on_hyphens=False),
            )


class AssetProperty(EnumProperty):
    labels = "labels"
    created_time = "createdTime"
    data_set_id = "dataSetId"
    id = "id"
    last_updated_time = "lastUpdatedTime"
    parent_id = "parentId"
    root_id = "rootId"
    description = "description"
    external_id = "externalId"
    metadata = "metadata"
    name = "name"
    source = "source"

    @staticmethod
    def metadata_key(key: str) -> list[str]:
        return ["metadata", key]


AssetPropertyLike: TypeAlias = Union[AssetProperty, str, List[str]]


class SortableAssetProperty(EnumProperty):
    created_time = "createdTime"
    data_set_id = "dataSetId"
    description = "description"
    external_id = "externalId"
    last_updated_time = "lastUpdatedTime"
    name = "name"
    source = "source"
    score = "_score_"

    @staticmethod
    def metadata_key(key: str) -> list[str]:
        return ["metadata", key]


SortableAssetPropertyLike: TypeAlias = Union[SortableAssetProperty, str, List[str]]


class AssetSort(CogniteSort):
    def __init__(
        self,
        property: SortableAssetProperty,
        order: Literal["asc", "desc"] = "asc",
        nulls: Literal["auto", "first", "last"] = "auto",
    ):
        super().__init__(property, order, nulls)
