from typing import Any, Dict, Iterator, Sequence, Union, cast

from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import (
    ThreeDAssetMapping,
    ThreeDAssetMappingList,
    ThreeDModel,
    ThreeDModelList,
    ThreeDModelRevision,
    ThreeDModelRevisionList,
    ThreeDModelRevisionUpdate,
    ThreeDModelUpdate,
    ThreeDNode,
    ThreeDNodeList,
)
from cognite.client.utils._identifier import IdentifierSequence, InternalId


class ThreeDAPI(APIClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.models = ThreeDModelsAPI(*args, **kwargs)
        self.revisions = ThreeDRevisionsAPI(*args, **kwargs)
        self.files = ThreeDFilesAPI(*args, **kwargs)
        self.asset_mappings = ThreeDAssetMappingAPI(*args, **kwargs)


class ThreeDModelsAPI(APIClient):
    _RESOURCE_PATH = "/3d/models"

    def __call__(self, chunk_size=None, published=None, limit=None):
        return self._list_generator(
            list_cls=ThreeDModelList,
            resource_cls=ThreeDModel,
            method="GET",
            chunk_size=chunk_size,
            filter={"published": published},
            limit=limit,
        )

    def __iter__(self):
        return cast(Iterator[ThreeDModel], self())

    def retrieve(self, id):
        return self._retrieve(cls=ThreeDModel, identifier=InternalId(id))

    def list(self, published=None, limit=25):
        return self._list(
            list_cls=ThreeDModelList,
            resource_cls=ThreeDModel,
            method="GET",
            filter={"published": published},
            limit=limit,
        )

    def create(self, name):
        utils._auxiliary.assert_type(name, "name", [str, Sequence])
        if isinstance(name, str):
            name_processed: Union[(Dict[(str, Any)], Sequence[Dict[(str, Any)]])] = {"name": name}
        else:
            name_processed = [{"name": n} for n in name]
        return self._create_multiple(list_cls=ThreeDModelList, resource_cls=ThreeDModel, items=name_processed)

    def update(self, item):
        return self._update_multiple(
            list_cls=ThreeDModelList, resource_cls=ThreeDModel, update_cls=ThreeDModelUpdate, items=item
        )

    def delete(self, id):
        self._delete_multiple(identifiers=IdentifierSequence.load(ids=id), wrap_ids=True)


class ThreeDRevisionsAPI(APIClient):
    _RESOURCE_PATH = "/3d/models/{}/revisions"

    def __call__(self, model_id, chunk_size=None, published=False, limit=None):
        return self._list_generator(
            list_cls=ThreeDModelRevisionList,
            resource_cls=ThreeDModelRevision,
            resource_path=utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, model_id),
            method="GET",
            chunk_size=chunk_size,
            filter={"published": published},
            limit=limit,
        )

    def retrieve(self, model_id, id):
        return self._retrieve(
            cls=ThreeDModelRevision,
            resource_path=utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, model_id),
            identifier=InternalId(id),
        )

    def create(self, model_id, revision):
        return self._create_multiple(
            list_cls=ThreeDModelRevisionList,
            resource_cls=ThreeDModelRevision,
            resource_path=utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, model_id),
            items=revision,
        )

    def list(self, model_id, published=False, limit=25):
        return self._list(
            list_cls=ThreeDModelRevisionList,
            resource_cls=ThreeDModelRevision,
            resource_path=utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, model_id),
            method="GET",
            filter={"published": published},
            limit=limit,
        )

    def update(self, model_id, item):
        return self._update_multiple(
            list_cls=ThreeDModelRevisionList,
            resource_cls=ThreeDModelRevision,
            update_cls=ThreeDModelRevisionUpdate,
            resource_path=utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, model_id),
            items=item,
        )

    def delete(self, model_id, id):
        self._delete_multiple(
            resource_path=utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, model_id),
            identifiers=IdentifierSequence.load(ids=id),
            wrap_ids=True,
        )

    def update_thumbnail(self, model_id, revision_id, file_id):
        resource_path = utils._auxiliary.interpolate_and_url_encode(
            (self._RESOURCE_PATH + "/{}/thumbnail"), model_id, revision_id
        )
        body = {"fileId": file_id}
        self._post(resource_path, json=body)

    def list_nodes(
        self, model_id, revision_id, node_id=None, depth=None, sort_by_node_id=False, partitions=None, limit=25
    ):
        resource_path = utils._auxiliary.interpolate_and_url_encode(
            (self._RESOURCE_PATH + "/{}/nodes"), model_id, revision_id
        )
        return self._list(
            list_cls=ThreeDNodeList,
            resource_cls=ThreeDNode,
            resource_path=resource_path,
            method="GET",
            limit=limit,
            filter={"depth": depth, "nodeId": node_id},
            partitions=partitions,
            other_params={"sortByNodeId": sort_by_node_id},
        )

    def filter_nodes(self, model_id, revision_id, properties=None, limit=25, partitions=None):
        resource_path = utils._auxiliary.interpolate_and_url_encode(
            (self._RESOURCE_PATH + "/{}/nodes"), model_id, revision_id
        )
        return self._list(
            list_cls=ThreeDNodeList,
            resource_cls=ThreeDNode,
            resource_path=resource_path,
            method="POST",
            limit=limit,
            filter={"properties": properties},
            partitions=partitions,
        )

    def list_ancestor_nodes(self, model_id, revision_id, node_id=None, limit=25):
        resource_path = utils._auxiliary.interpolate_and_url_encode(
            (self._RESOURCE_PATH + "/{}/nodes"), model_id, revision_id
        )
        return self._list(
            list_cls=ThreeDNodeList,
            resource_cls=ThreeDNode,
            resource_path=resource_path,
            method="GET",
            limit=limit,
            filter={"nodeId": node_id},
        )


class ThreeDFilesAPI(APIClient):
    _RESOURCE_PATH = "/3d/files"

    def retrieve(self, id):
        path = utils._auxiliary.interpolate_and_url_encode((self._RESOURCE_PATH + "/{}"), id)
        return self._get(path).content


class ThreeDAssetMappingAPI(APIClient):
    _RESOURCE_PATH = "/3d/models/{}/revisions/{}/mappings"
    _LIST_CLASS = ThreeDAssetMappingList

    def list(self, model_id, revision_id, node_id=None, asset_id=None, limit=25):
        path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, model_id, revision_id)
        return self._list(
            list_cls=ThreeDAssetMappingList,
            resource_cls=ThreeDAssetMapping,
            resource_path=path,
            method="GET",
            filter={"nodeId": node_id, "assetId": asset_id},
            limit=limit,
        )

    def create(self, model_id, revision_id, asset_mapping):
        path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, model_id, revision_id)
        return self._create_multiple(
            list_cls=ThreeDAssetMappingList, resource_cls=ThreeDAssetMapping, resource_path=path, items=asset_mapping
        )

    def delete(self, model_id, revision_id, asset_mapping):
        path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, model_id, revision_id)
        utils._auxiliary.assert_type(asset_mapping, "asset_mapping", [Sequence, ThreeDAssetMapping])
        if isinstance(asset_mapping, ThreeDAssetMapping):
            asset_mapping = [asset_mapping]
        chunks = utils._auxiliary.split_into_chunks(
            [ThreeDAssetMapping(a.node_id, a.asset_id).dump(camel_case=True) for a in asset_mapping], self._DELETE_LIMIT
        )
        tasks = [{"url_path": (path + "/delete"), "json": {"items": chunk}} for chunk in chunks]
        summary = utils._concurrency.execute_tasks(self._post, tasks, self._config.max_workers)
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=(lambda task: task["json"]["items"]),
            task_list_element_unwrap_fn=(lambda el: ThreeDAssetMapping._load(el)),
            str_format_element_fn=(lambda el: (el.asset_id, el.node_id)),
        )
