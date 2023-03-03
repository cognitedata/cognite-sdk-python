import queue
import threading
from collections import OrderedDict
from typing import Dict, Iterator, Optional, Sequence, Set, Union, cast, overload

from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import Asset, AssetAggregate, AssetFilter, AssetList, AssetUpdate
from cognite.client.data_classes.shared import AggregateBucketResult
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._identifier import IdentifierSequence


class AssetsAPI(APIClient):
    _RESOURCE_PATH = "/assets"

    def __call__(
        self,
        chunk_size=None,
        name=None,
        parent_ids=None,
        parent_external_ids=None,
        asset_subtree_ids=None,
        asset_subtree_external_ids=None,
        metadata=None,
        data_set_ids=None,
        data_set_external_ids=None,
        labels=None,
        geo_location=None,
        source=None,
        created_time=None,
        last_updated_time=None,
        root=None,
        external_id_prefix=None,
        aggregated_properties=None,
        limit=None,
        partitions=None,
    ):
        if aggregated_properties:
            aggregated_properties = [utils._auxiliary.to_camel_case(s) for s in aggregated_properties]
        asset_subtree_ids_processed = None
        if asset_subtree_ids or asset_subtree_external_ids:
            asset_subtree_ids_processed = IdentifierSequence.load(
                asset_subtree_ids, asset_subtree_external_ids
            ).as_dicts()
        data_set_ids_processed = None
        if data_set_ids or data_set_external_ids:
            data_set_ids_processed = IdentifierSequence.load(data_set_ids, data_set_external_ids).as_dicts()
        filter = AssetFilter(
            name=name,
            parent_ids=parent_ids,
            parent_external_ids=parent_external_ids,
            asset_subtree_ids=asset_subtree_ids_processed,
            data_set_ids=data_set_ids_processed,
            labels=labels,
            geo_location=geo_location,
            metadata=metadata,
            source=source,
            created_time=created_time,
            last_updated_time=last_updated_time,
            root=root,
            external_id_prefix=external_id_prefix,
        ).dump(camel_case=True)
        return self._list_generator(
            list_cls=AssetList,
            resource_cls=Asset,
            method="POST",
            chunk_size=chunk_size,
            filter=filter,
            limit=limit,
            partitions=partitions,
            other_params=({"aggregatedProperties": aggregated_properties} if aggregated_properties else {}),
        )

    def __iter__(self):
        return cast(Iterator[Asset], self())

    def retrieve(self, id=None, external_id=None):
        identifier = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(list_cls=AssetList, resource_cls=Asset, identifiers=identifier)

    def retrieve_multiple(self, ids=None, external_ids=None, ignore_unknown_ids=False):
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=AssetList, resource_cls=Asset, identifiers=identifiers, ignore_unknown_ids=ignore_unknown_ids
        )

    def list(
        self,
        name=None,
        parent_ids=None,
        parent_external_ids=None,
        asset_subtree_ids=None,
        asset_subtree_external_ids=None,
        data_set_ids=None,
        data_set_external_ids=None,
        labels=None,
        geo_location=None,
        metadata=None,
        source=None,
        created_time=None,
        last_updated_time=None,
        root=None,
        external_id_prefix=None,
        aggregated_properties=None,
        partitions=None,
        limit=25,
    ):
        if aggregated_properties:
            aggregated_properties = [utils._auxiliary.to_camel_case(s) for s in aggregated_properties]
        asset_subtree_ids_processed = None
        if asset_subtree_ids or asset_subtree_external_ids:
            asset_subtree_ids_processed = IdentifierSequence.load(
                asset_subtree_ids, asset_subtree_external_ids
            ).as_dicts()
        data_set_ids_processed = None
        if data_set_ids or data_set_external_ids:
            data_set_ids_processed = IdentifierSequence.load(data_set_ids, data_set_external_ids).as_dicts()
        filter = AssetFilter(
            name=name,
            parent_ids=parent_ids,
            parent_external_ids=parent_external_ids,
            asset_subtree_ids=asset_subtree_ids_processed,
            data_set_ids=data_set_ids_processed,
            labels=labels,
            geo_location=geo_location,
            metadata=metadata,
            source=source,
            created_time=created_time,
            last_updated_time=last_updated_time,
            root=root,
            external_id_prefix=external_id_prefix,
        ).dump(camel_case=True)
        return self._list(
            list_cls=AssetList,
            resource_cls=Asset,
            method="POST",
            limit=limit,
            filter=filter,
            other_params=({"aggregatedProperties": aggregated_properties} if aggregated_properties else {}),
            partitions=partitions,
        )

    def aggregate(self, filter=None):
        return self._aggregate(filter=filter, cls=AssetAggregate)

    def aggregate_metadata_keys(self, filter=None):
        return self._aggregate(filter=filter, aggregate="metadataKeys", cls=AggregateBucketResult)

    def aggregate_metadata_values(self, keys, filter=None):
        return self._aggregate(filter=filter, aggregate="metadataValues", keys=keys, cls=AggregateBucketResult)

    @overload
    def create(self, asset):
        ...

    @overload
    def create(self, asset):
        ...

    def create(self, asset):
        utils._auxiliary.assert_type(asset, "asset", [Asset, Sequence])
        return self._create_multiple(list_cls=AssetList, resource_cls=Asset, items=asset)

    def create_hierarchy(self, assets):
        utils._auxiliary.assert_type(assets, "assets", [Sequence])
        return _AssetPoster(assets, client=self).post()

    def delete(self, id=None, external_id=None, recursive=False, ignore_unknown_ids=False):
        self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=id, external_ids=external_id),
            wrap_ids=True,
            extra_body_fields={"recursive": recursive, "ignoreUnknownIds": ignore_unknown_ids},
        )

    @overload
    def update(self, item):
        ...

    @overload
    def update(self, item):
        ...

    def update(self, item):
        return self._update_multiple(list_cls=AssetList, resource_cls=Asset, update_cls=AssetUpdate, items=item)

    def search(self, name=None, description=None, query=None, filter=None, limit=100):
        return self._search(
            list_cls=AssetList,
            search={"name": name, "description": description, "query": query},
            filter=(filter or {}),
            limit=limit,
        )

    def retrieve_subtree(self, id=None, external_id=None, depth=None):
        asset = self.retrieve(id=id, external_id=external_id)
        if asset is None:
            return AssetList([], self._cognite_client)
        subtree = self._get_asset_subtree([asset], current_depth=0, depth=depth)
        return AssetList(subtree, self._cognite_client)

    def _get_asset_subtree(self, assets, current_depth, depth):
        subtree = assets
        if (depth is None) or (current_depth < depth):
            children = self._get_children(assets)
            if children:
                subtree.extend(self._get_asset_subtree(children, (current_depth + 1), depth))
        return subtree

    def _get_children(self, assets):
        ids = [a.id for a in assets]
        tasks = []
        chunk_size = 100
        for i in range(0, len(ids), chunk_size):
            tasks.append({"parent_ids": ids[i : (i + chunk_size)], "limit": (-1)})
        tasks_summary = utils._concurrency.execute_tasks(self.list, tasks=tasks, max_workers=self._config.max_workers)
        tasks_summary.raise_compound_exception_if_failed_tasks()
        res_list = tasks_summary.results
        children = []
        for res in res_list:
            children.extend(res)
        return children


class _AssetsFailedToPost:
    def __init__(self, exc, assets):
        self.exc = exc
        self.assets = assets


class _AssetPosterWorker(threading.Thread):
    def __init__(self, client, request_queue, response_queue):
        self.client = client
        self.request_queue = request_queue
        self.response_queue = response_queue
        self.stop = False
        super().__init__(daemon=True)

    def run(self):
        request: Sequence[Asset] = []
        while not self.stop:
            try:
                try:
                    request = cast(Sequence[Asset], self.request_queue.get(timeout=0.1))
                except queue.Empty:
                    continue
                assets = cast(AssetList, self.client.create(request))
                self.response_queue.put(assets)
            except Exception as e:
                self.response_queue.put(_AssetsFailedToPost(e, request))


class _AssetPoster:
    def __init__(self, assets, client):
        self._validate_asset_hierarchy(assets)
        self.remaining_external_ids: OrderedDict[(str, Optional[str])] = OrderedDict()
        self.remaining_external_ids_set = set()
        self.external_id_to_asset = {}
        for asset in assets:
            self.remaining_external_ids[cast(str, asset.external_id)] = None
            self.remaining_external_ids_set.add(asset.external_id)
            self.external_id_to_asset[asset.external_id] = asset
        self.client = client
        self.num_of_assets = len(self.remaining_external_ids)
        self.external_ids_without_circular_deps: Set[str] = set()
        self.external_id_to_children: Dict[(str, Set[Asset])] = {
            external_id: set() for external_id in self.remaining_external_ids
        }
        self.external_id_to_descendent_count = {external_id: 0 for external_id in self.remaining_external_ids}
        self.successfully_posted_external_ids: Set[str] = set()
        self.posted_assets: Set[Asset] = set()
        self.may_have_been_posted_assets: Set[Asset] = set()
        self.not_posted_assets: Set[Asset] = set()
        self.exception: Optional[Exception] = None
        self.request_queue: queue.Queue[Sequence[Asset]] = queue.Queue()
        self.response_queue: queue.Queue[Union[(AssetList, _AssetsFailedToPost)]] = queue.Queue()
        self._initialize()

    def assets_remaining(self):
        return (
            (len(self.posted_assets) + len(self.may_have_been_posted_assets)) + len(self.not_posted_assets)
        ) < self.num_of_assets

    @staticmethod
    def _validate_asset_hierarchy(assets):
        external_ids_seen = set()
        for asset in assets:
            if asset.external_id is None:
                raise AssertionError("An asset does not have external_id.")
            if asset.external_id in external_ids_seen:
                raise AssertionError(f"Duplicate external_id '{asset.external_id}' found")
            external_ids_seen.add(asset.external_id)
            parent_ref = asset.parent_external_id
            if parent_ref:
                if asset.parent_id is not None:
                    raise AssertionError(
                        f"An asset has both parent_id '{asset.parent_id}' and parent_external_id '{asset.parent_external_id}' set."
                    )

    def _initialize(self):
        root_assets = set()
        for external_id in self.remaining_external_ids:
            asset = self.external_id_to_asset[external_id]
            if (asset.parent_external_id not in self.external_id_to_asset) or (asset.parent_id is not None):
                root_assets.add(asset)
            elif asset.parent_external_id in self.external_id_to_children:
                self.external_id_to_children[asset.parent_external_id].add(asset)
            self._verify_asset_is_not_part_of_tree_with_circular_deps(asset)
        for root_asset in root_assets:
            self._initialize_asset_to_descendant_count(root_asset)
        self.remaining_external_ids = self._sort_external_ids_by_descendant_count(self.remaining_external_ids)

    def _initialize_asset_to_descendant_count(self, asset):
        for child in self.external_id_to_children[cast(str, asset.external_id)]:
            self.external_id_to_descendent_count[
                cast(str, asset.external_id)
            ] += 1 + self._initialize_asset_to_descendant_count(child)
        return self.external_id_to_descendent_count[cast(str, asset.external_id)]

    def _get_descendants(self, asset):
        descendants = []
        for child in self.external_id_to_children[cast(str, asset.external_id)]:
            descendants.append(child)
            descendants.extend(self._get_descendants(child))
        return descendants

    def _verify_asset_is_not_part_of_tree_with_circular_deps(self, asset):
        next_asset: Optional[Asset] = asset
        seen = {cast(str, asset.external_id)}
        while (next_asset is not None) and (next_asset.parent_external_id is not None):
            next_asset = self.external_id_to_asset.get(next_asset.parent_external_id)
            if next_asset is None:
                break
            if next_asset.external_id in self.external_ids_without_circular_deps:
                break
            if next_asset.external_id not in seen:
                seen.add(cast(str, next_asset.external_id))
            else:
                raise AssertionError("The asset hierarchy has circular dependencies")
        self.external_ids_without_circular_deps.update(seen)

    def _sort_external_ids_by_descendant_count(self, external_ids):
        sorted_external_ids = sorted(
            external_ids, key=(lambda x: self.external_id_to_descendent_count[x]), reverse=True
        )
        return OrderedDict({external_id: None for external_id in sorted_external_ids})

    def _get_assets_unblocked_locally(self, asset, limit):
        pq = utils._auxiliary.PriorityQueue()
        pq.add(asset, self.external_id_to_descendent_count[cast(str, asset.external_id)])
        unblocked_descendents: Set[Asset] = set()
        while pq:
            if len(unblocked_descendents) == limit:
                break
            asset = pq.get()
            unblocked_descendents.add(asset)
            self.remaining_external_ids_set.remove(asset.external_id)
            for child in self.external_id_to_children[cast(str, asset.external_id)]:
                pq.add(child, self.external_id_to_descendent_count[cast(str, child.external_id)])
        return unblocked_descendents

    def _get_unblocked_assets(self):
        limit = self.client._CREATE_LIMIT
        unblocked_assets_lists = []
        unblocked_assets_chunk: Set[Asset] = set()
        for external_id in self.remaining_external_ids:
            asset = self.external_id_to_asset[external_id]
            parent_external_id = asset.parent_external_id
            if external_id in self.remaining_external_ids_set:
                has_parent_id = asset.parent_id is not None
                is_root = (not has_parent_id) and (parent_external_id not in self.external_id_to_asset)
                is_unblocked = parent_external_id in self.successfully_posted_external_ids
                if is_root or has_parent_id or is_unblocked:
                    unblocked_assets_chunk.update(
                        self._get_assets_unblocked_locally(asset, (limit - len(unblocked_assets_chunk)))
                    )
                    if len(unblocked_assets_chunk) == limit:
                        unblocked_assets_lists.append(unblocked_assets_chunk)
                        unblocked_assets_chunk = set()
        if len(unblocked_assets_chunk) > 0:
            unblocked_assets_lists.append(unblocked_assets_chunk)
        for unblocked_assets_chunk in unblocked_assets_lists:
            for unblocked_asset in unblocked_assets_chunk:
                del self.remaining_external_ids[cast(str, unblocked_asset.external_id)]
        return unblocked_assets_lists

    def run(self):
        unblocked_assets_lists = self._get_unblocked_assets()
        for unblocked_assets in unblocked_assets_lists:
            self.request_queue.put(list(unblocked_assets))
        while self.assets_remaining():
            res = self.response_queue.get()
            if isinstance(res, _AssetsFailedToPost):
                if isinstance(res.exc, CogniteAPIError):
                    self.exception = res.exc
                    for asset in res.assets:
                        if res.exc.code >= 500:
                            self.may_have_been_posted_assets.add(asset)
                        elif res.exc.code >= 400:
                            self.not_posted_assets.add(asset)
                        for descendant in self._get_descendants(asset):
                            self.not_posted_assets.add(descendant)
                else:
                    raise res.exc
            else:
                for asset in res:
                    self.posted_assets.add(asset)
                    self.successfully_posted_external_ids.add(cast(str, asset.external_id))
                unblocked_assets_lists = self._get_unblocked_assets()
                for unblocked_assets in unblocked_assets_lists:
                    self.request_queue.put(list(unblocked_assets))
        if (len(self.may_have_been_posted_assets) > 0) or (len(self.not_posted_assets) > 0):
            if isinstance(self.exception, CogniteAPIError):
                raise CogniteAPIError(
                    message=self.exception.message,
                    code=self.exception.code,
                    x_request_id=self.exception.x_request_id,
                    missing=self.exception.missing,
                    duplicated=self.exception.duplicated,
                    successful=AssetList(list(self.posted_assets)),
                    unknown=AssetList(list(self.may_have_been_posted_assets)),
                    failed=AssetList(list(self.not_posted_assets)),
                    unwrap_fn=(lambda a: a.external_id),
                )
            raise cast(Exception, self.exception)

    def post(self):
        workers = []
        for _ in range(self.client._config.max_workers):
            worker = _AssetPosterWorker(self.client, self.request_queue, self.response_queue)
            workers.append(worker)
            worker.start()
        self.run()
        for worker in workers:
            worker.stop = True
        return AssetList(self.posted_assets)
