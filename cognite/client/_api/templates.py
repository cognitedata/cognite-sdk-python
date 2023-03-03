from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes.templates import (
    GraphQlResponse,
    TemplateGroup,
    TemplateGroupList,
    TemplateGroupVersion,
    TemplateGroupVersionList,
    TemplateInstance,
    TemplateInstanceList,
    TemplateInstanceUpdate,
    View,
    ViewList,
    ViewResolveItem,
    ViewResolveList,
)
from cognite.client.utils._identifier import IdentifierSequence


class TemplatesAPI(APIClient):
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.groups = TemplateGroupsAPI(*args, **kwargs)
        self.versions = TemplateGroupVersionsAPI(*args, **kwargs)
        self.instances = TemplateInstancesAPI(*args, **kwargs)
        self.views = TemplateViewsAPI(*args, **kwargs)

    def graphql_query(self, external_id, version, query):
        path = "/templategroups/{}/versions/{}/graphql"
        path = utils._auxiliary.interpolate_and_url_encode(path, external_id, version)
        response = self._post(path, {"query": query})
        return GraphQlResponse._load(response.json())


class TemplateGroupsAPI(APIClient):
    _RESOURCE_PATH = "/templategroups"

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

    def create(self, template_groups):
        return self._create_multiple(list_cls=TemplateGroupList, resource_cls=TemplateGroup, items=template_groups)

    def upsert(self, template_groups):
        path = self._RESOURCE_PATH + "/upsert"
        is_single = not isinstance(template_groups, list)
        if is_single:
            template_groups_processed: List[TemplateGroup] = cast(List[TemplateGroup], [template_groups])
        else:
            template_groups_processed = cast(List[TemplateGroup], template_groups)
        updated = self._post(
            path, {"items": [item.dump(camel_case=True) for item in template_groups_processed]}
        ).json()["items"]
        res = TemplateGroupList._load(updated, cognite_client=self._cognite_client)
        if is_single:
            return res[0]
        return res

    def retrieve_multiple(self, external_ids, ignore_unknown_ids=False):
        identifiers = IdentifierSequence.load(ids=None, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=TemplateGroupList,
            resource_cls=TemplateGroup,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    def list(self, limit=25, owners=None):
        filter = {}
        if owners is not None:
            filter["owners"] = owners
        return self._list(
            list_cls=TemplateGroupList,
            resource_cls=TemplateGroup,
            method="POST",
            limit=limit,
            filter=filter,
            partitions=None,
            sort=None,
        )

    def delete(self, external_ids, ignore_unknown_ids=False):
        return self._delete_multiple(
            wrap_ids=True,
            identifiers=IdentifierSequence.load(external_ids=external_ids),
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )


class TemplateGroupVersionsAPI(APIClient):
    _RESOURCE_PATH = "/templategroups/{}/versions"

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

    def upsert(self, external_id, version):
        resource_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id) + "/upsert"
        version_res = self._post(resource_path, version.dump(camel_case=True)).json()
        return TemplateGroupVersion._load(version_res)

    def list(self, external_id, limit=25, min_version=None, max_version=None):
        resource_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id)
        filter = {}
        if min_version is not None:
            filter["minVersion"] = min_version
        if max_version is not None:
            filter["maxVersion"] = max_version
        return self._list(
            list_cls=TemplateGroupVersionList,
            resource_cls=TemplateGroupVersion,
            resource_path=resource_path,
            method="POST",
            limit=limit,
            filter=filter,
        )

    def delete(self, external_id, version):
        resource_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id)
        self._post((resource_path + "/delete"), {"version": version})


class TemplateInstancesAPI(APIClient):
    _RESOURCE_PATH = "/templategroups/{}/versions/{}/instances"
    _LIST_CLASS = TemplateInstanceList

    def create(self, external_id, version, instances):
        resource_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version)
        return self._create_multiple(
            list_cls=TemplateInstanceList, resource_cls=TemplateInstance, resource_path=resource_path, items=instances
        )

    def upsert(self, external_id, version, instances):
        if isinstance(instances, TemplateInstance):
            instances = [instances]
        resource_path = (
            utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version) + "/upsert"
        )
        updated = self._post(
            resource_path, {"items": [instance.dump(camel_case=True) for instance in instances]}
        ).json()["items"]
        res = TemplateInstanceList._load(updated, cognite_client=self._cognite_client)
        if len(res) == 1:
            return res[0]
        return res

    def update(self, external_id, version, item):
        resource_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version)
        return self._update_multiple(
            list_cls=TemplateInstanceList,
            resource_cls=TemplateInstance,
            update_cls=TemplateInstanceUpdate,
            items=item,
            resource_path=resource_path,
        )

    def retrieve_multiple(self, external_id, version, external_ids, ignore_unknown_ids=False):
        resource_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version)
        identifiers = IdentifierSequence.load(ids=None, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=TemplateInstanceList,
            resource_cls=TemplateInstance,
            resource_path=resource_path,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    def list(self, external_id, version, limit=25, data_set_ids=None, template_names=None):
        resource_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version)
        filter: Dict[(str, Any)] = {}
        if data_set_ids is not None:
            filter["dataSetIds"] = data_set_ids
        if template_names is not None:
            filter["templateNames"] = template_names
        return self._list(
            list_cls=TemplateInstanceList,
            resource_cls=TemplateInstance,
            resource_path=resource_path,
            method="POST",
            limit=limit,
            filter=filter,
        )

    def delete(self, external_id, version, external_ids, ignore_unknown_ids=False):
        resource_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version)
        return self._delete_multiple(
            resource_path=resource_path,
            identifiers=IdentifierSequence.load(external_ids=external_ids),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )


class TemplateViewsAPI(APIClient):
    _RESOURCE_PATH = "/templategroups/{}/versions/{}/views"

    def create(self, external_id, version, views):
        resource_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version)
        return self._create_multiple(list_cls=ViewList, resource_cls=View, resource_path=resource_path, items=views)

    def upsert(self, external_id, version, views):
        if isinstance(views, View):
            views = [views]
        resource_path = (
            utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version) + "/upsert"
        )
        updated = self._post(resource_path, {"items": [view.dump(camel_case=True) for view in views]}).json()["items"]
        res = ViewList._load(updated, cognite_client=self._cognite_client)
        if len(res) == 1:
            return res[0]
        return res

    def resolve(self, external_id, version, view_external_id, input, limit=25):
        url_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version) + "/resolve"
        return self._list(
            list_cls=ViewResolveList,
            resource_cls=ViewResolveItem,
            url_path=url_path,
            method="POST",
            limit=limit,
            other_params={"externalId": view_external_id, "input": input},
        )

    def list(self, external_id, version, limit=25):
        resource_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version)
        return self._list(list_cls=ViewList, resource_cls=View, resource_path=resource_path, method="POST", limit=limit)

    def delete(self, external_id, version, view_external_id, ignore_unknown_ids=False):
        resource_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version)
        return self._delete_multiple(
            resource_path=resource_path,
            identifiers=IdentifierSequence.load(external_ids=view_external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )
