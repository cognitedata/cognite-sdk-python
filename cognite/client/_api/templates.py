from cognite.client._api_client import APIClient
from cognite.client.data_classes.templates import *


class TemplatesAPI(APIClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.groups = TemplateGroupsApi(*args, **kwargs)
        self.versions = TemplateGroupVersionsApi(*args, **kwargs)
        self.instances = TemplateInstancesApi(*args, **kwargs)
        self.query = TemplatesQuery(*args, **kwargs)


class TemplateGroupsApi(APIClient):
    _RESOURCE_PATH = "/templategroups"
    _LIST_CLASS = TemplateGroupList

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def create(
        self, template_groups: Union[TemplateGroup, List[TemplateGroup]]
    ) -> Union[TemplateGroup, TemplateGroupList]:
        """`Create one or more template groups.`

        Args:
            template_groups (Union[TemplateGroup, List[TemplateGroup]])

        Returns:
            Union[TemplateGroup, TemplateGroupList]: Created template group(s)

        Examples:
            create a new template group:

                >>> from cognite.client.alpha import CogniteClient
                >>> from cognite.client.data_classes import
                >>> c = CogniteClient()
                >>> template_group_1 = TemplateGroup("sdk-test-group", "This is a test group")
                >>> template_group_2 = TemplateGroup("sdk-test-group-2", "This is another test group")
                >>> c.templates.groups.create([template_group_1, template_group_2])
        """
        return self._create_multiple(items=template_groups)

    def upsert(self, items: Union[TemplateGroup, List[TemplateGroup]]) -> Union[TemplateGroup, TemplateGroupList]:
        single_item = not isinstance(items, list)
        if single_item:
            items = [items]
        updated = self._put(self._RESOURCE_PATH, {"items": [item.dump(camel_case=True) for item in items]},).json()[
            "items"
        ]
        res = self._LIST_CLASS._load(updated, cognite_client=self._cognite_client)
        if single_item:
            return res[0]
        return res

    def retrive_multiple(self, external_ids: List[str], ignore_unknown_ids: bool = False):
        self._retrieve_multiple(external_ids=external_ids, ignore_unknown_ids=ignore_unknown_ids, wrap_ids=True)

    def list(self, limit: int = 25, owners: List[str] = None) -> TemplateGroupList:
        filter = {"owners": owners} if owners is not None else None
        return self._list(method="POST", limit=limit, filter=filter, partitions=None, sort=None)

    def delete(self, items: Union[str, List[str]]) -> Union[TemplateGroup, TemplateGroupList]:
        return self._delete_multiple(True, external_ids=items)


class TemplateGroupVersionsApi(APIClient):
    _RESOURCE_PATH = "/templategroups/{}/versions"
    _LIST_CLASS = TemplateGroupVersionList

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def upsert(self, external_id: str, version: TemplateGroupVersion):
        resource_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id)
        return self._create_multiple(resource_path=resource_path, items=version)

    def list(
        self, external_id: str, limit: int = 25, min_version: Optional[int] = None, max_version: Optional[int] = None
    ) -> TemplateGroupVersionList:
        resource_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id)
        filter = {}
        if min_version is not None:
            filter["minVersion"] = min_version
        if max_version is not None:
            filter["maxVersion"] = max_version
        return self._list(resource_path=resource_path, method="POST", limit=limit, filter=filter)


class TemplatesQuery(APIClient):
    _PATH = "/templategroups/{}/versions/{}/graphql"

    def graphql_query(self, external_id: str, version: int, query: str) -> GraphQlResponse:
        path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version)
        response = self._post(path, {"query": query})
        return GraphQlResponse._load(response.json())


class TemplateInstancesApi(APIClient):
    _RESOURCE_PATH = "/templategroups/{}/versions/{}/instances"
    _LIST_CLASS = TemplateInstanceList

    def create(
        self, external_id: str, version: int, instances: Union[TemplateInstance, List[TemplateInstance]]
    ) -> Union[TemplateInstance, TemplateInstanceList]:
        resource_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version)
        return self._create_multiple(resource_path=resource_path, items=instances,)

    def upsert(
        self, external_id: str, version: int, instances: Union[TemplateGroup, List[TemplateGroup]]
    ) -> Union[TemplateInstance, TemplateInstanceList]:
        resource_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version)
        updated = self._post(
            resource_path, {"items": [instance.dump(camel_case=True) for instance in instances]},
        ).json()["items"]
        res = TemplateInstanceList._load(updated, cognite_client=self._cognite_client)
        if len(res) == 1:
            return res[0]
        return res

    def retrieve_multiple(
        self, external_id: str, version: int, external_ids: List[str], ignore_unkown_ids: bool = False
    ) -> TemplateInstanceList:
        resource_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version)
        return self._retrieve_multiple(
            resource_path=resource_path, external_ids=external_ids, ignore_unknown_ids=ignore_unkown_ids, wrap_ids=True
        )

    def list(
        self,
        external_id: str,
        version: int,
        limit: int = 25,
        data_set_ids: Optional[List[int]] = None,
        template_names: Optional[List[str]] = None,
    ):
        resource_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version)
        filter = {}
        if data_set_ids is not None:
            filter["dataSetIds"] = data_set_ids
        if template_names is not None:
            filter["template_names"] = template_names
        return self._list(resource_path=resource_path, method="POST", limit=limit, filter=filter)

    def delete(self, external_id: str, version: int, items: List[str]) -> Union[TemplateGroup, TemplateGroupList]:
        resource_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version)
        return self._delete_multiple(resource_path=resource_path, external_ids=items, wrap_ids=True)
