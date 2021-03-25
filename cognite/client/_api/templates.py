from typing import List, Union

from cognite.client._api_client import APIClient
from cognite.client.data_classes.templates import *


class TemplatesAPI(APIClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.groups = TemplateGroupsAPI(*args, **kwargs)
        self.versions = TemplateGroupVersionsAPI(*args, **kwargs)
        self.instances = TemplateInstancesAPI(*args, **kwargs)

    def graphql_query(self, external_id: str, version: int, query: str) -> GraphQlResponse:
        """
        `Run a GraphQL Query.`
        To learn more, see https://graphql.org/learn/

        Args:
            external_id (str): The external id of the template group.
            version (int): The version of the template group to run the query on.
            query (str): The GraphQL query to run.

        Returns:
            GraphQlResponse: the result of the query.

        Examples:
            Run a GraphQL query:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> query = '''
                >>>    {
                >>>        countryList {
                >>>           name,
                >>>           demographics {
                >>>               populationSize,
                >>>               growthRate
                >>>           },
                >>>           deaths {
                >>>               datapoints(limit: 100) {
                >>>                   timestamp,
                >>>                   value
                >>>               }
                >>>           }
                >>>        }
                >>>    }
                >>>    '''
                >>> result = c.templates.query.graphql_query("template-group-ext-id", 1, query)
        """
        path = "/templategroups/{}/versions/{}/graphql"
        path = utils._auxiliary.interpolate_and_url_encode(path, external_id, version)
        response = self._post(path, {"query": query})
        return GraphQlResponse._load(response.json())


class TemplateGroupsAPI(APIClient):
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
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TemplateGroup
                >>> c = CogniteClient()
                >>> template_group_1 = TemplateGroup("sdk-test-group", "This is a test group")
                >>> template_group_2 = TemplateGroup("sdk-test-group-2", "This is another test group")
                >>> c.templates.groups.create([template_group_1, template_group_2])
        """
        return self._create_multiple(items=template_groups)

    def upsert(
        self, template_groups: Union[TemplateGroup, List[TemplateGroup]]
    ) -> Union[TemplateGroup, TemplateGroupList]:
        """`Upsert one or more template groups.`
        Will overwrite existing template group(s) with the same external id(s).

        Args:
            template_groups (Union[TemplateGroup, List[TemplateGroup]])

        Returns:
            Union[TemplateGroup, TemplateGroupList]: Created template group(s)

        Examples:
            create a new template group:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TemplateGroup
                >>> c = CogniteClient()
                >>> template_group_1 = TemplateGroup("sdk-test-group", "This is a test group")
                >>> template_group_2 = TemplateGroup("sdk-test-group-2", "This is another test group")
                >>> c.templates.groups.upsert([template_group_1, template_group_2])
        """
        path = self._RESOURCE_PATH + "/upsert"
        is_single = not isinstance(template_groups, list)
        if is_single:
            template_groups = [template_groups]
        updated = self._post(path, {"items": [item.dump(camel_case=True) for item in template_groups]}).json()["items"]
        res = self._LIST_CLASS._load(updated, cognite_client=self._cognite_client)
        if is_single:
            return res[0]
        return res

    def retrieve_multiple(self, external_ids: List[str], ignore_unknown_ids: bool = False) -> TemplateGroupList:
        """`Retrieve multiple template groups by external id.`

        Args:
            external_ids (List[str]): External IDs
            ignore_unknown_ids (bool): Ignore external IDs that are not found rather than throw an exception.

        Returns:
            TemplateGroupList: The requested template groups.

        Examples:
            Get template groups by external id:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.templates.groups.retrieve_multiple(external_ids=["abc", "def"])
        """
        return self._retrieve_multiple(external_ids=external_ids, ignore_unknown_ids=ignore_unknown_ids, wrap_ids=True)

    def list(self, limit: int = 25, owners: List[str] = None) -> TemplateGroupList:
        """`Lists template groups stored in the project based on a query filter given in the payload of this request.`
        Up to 1000 template groups can be retrieved in one operation.

        Args:
            owners (List[str]): Include template groups that have any of these values in their `owner` field.
            limit (int): Maximum number of template groups to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            TemplateGroupList: List of requested template groups

        Examples:
            List template groups:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> template_group_list = c.templates.groups.list(limit=5)
        """
        filter = {}
        if owners is not None:
            filter["owners"] = owners
        return self._list(method="POST", limit=limit, filter=filter, partitions=None, sort=None)

    def delete(
        self, external_ids: Union[str, List[str]], ignore_unknown_ids: bool = False
    ) -> Union[TemplateGroup, TemplateGroupList]:
        """`Delete one or more template groups.`

        Args:
            external_ids (Union[str, List[str]]): External ID or list of external ids
            ignore_unknown_ids (bool): Ignore external IDs that are not found rather than throw an exception.

        Returns:
            None

        Examples:
            Delete template groups by external id:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.templates.groups.delete(external_id=["a", "b"])
        """
        return self._delete_multiple(
            True, external_ids=external_ids, extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids}
        )


class TemplateGroupVersionsAPI(APIClient):
    _RESOURCE_PATH = "/templategroups/{}/versions"
    _LIST_CLASS = TemplateGroupVersionList

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def upsert(self, external_id: str, version: TemplateGroupVersion):
        """`Upsert a template group version.`
        A Template Group update supports specifying different conflict modes, which is used when an existing schema already exists.

        Patch -> It diffs the new schema with the old schema and fails if there are breaking changes.
        Update -> It sets the new schema as schema of a new version.
        Force -> It ignores breaking changes and replaces the old schema with the new schema.
        The default mode is "patch".

        Args:
            external_id (str): The external id of the template group.
            version (TemplateGroupVersion): The GraphQL schema of this version.

        Returns:
            TemplateGroupVersion: Created template group version

        Examples:
            create a new template group version modeling Covid-19:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TemplateGroup
                >>> c = CogniteClient()
                >>> template_group = TemplateGroup("sdk-test-group", "This template group models Covid-19 spread")
                >>> c.templates.groups.create(template_group)
                >>> schema = '''
                >>>     type Demographics @template {
                >>>         "The amount of people"
                >>>         populationSize: Int,
                >>>         "The population growth rate"
                >>>         growthRate: Float,
                >>>     }
                >>>     type Country @template {
                >>>         name: String,
                >>>         demographics: Demographics,
                >>>         deaths: TimeSeries,
                >>>         confirmed: TimeSeries,
                >>>     }'''
                >>> template_group_version = TemplateGroupVersion(schema)
                >>> c.templates.versions.upsert(template_group.external_id, template_group_version)
        """
        resource_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id) + "/upsert"
        version = self._post(resource_path, version.dump(camel_case=True)).json()
        return TemplateGroupVersion._load(version)

    def list(
        self, external_id: str, limit: int = 25, min_version: Optional[int] = None, max_version: Optional[int] = None
    ) -> TemplateGroupVersionList:
        """`Lists versions of a specified template group.`
        Up to 1000 template group version can be retrieved in one operation.

        Args:
            external_id (str): The external id of the template group.
            limit (int): Maximum number of template group versions to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            min_version: (Optional[int]): Exclude versions with a version number smaller than this.
            max_version: (Optional[int]): Exclude versions with a version number larger than this.

        Returns:
            TemplateGroupVersionList: List of requested template group versions

        Examples:
            List template group versions:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> template_group_list = c.templates.versions.list("template-group-ext-id", limit=5)
        """
        resource_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id)
        filter = {}
        if min_version is not None:
            filter["minVersion"] = min_version
        if max_version is not None:
            filter["maxVersion"] = max_version
        return self._list(resource_path=resource_path, method="POST", limit=limit, filter=filter)

    def delete(self, external_id: str, version: int) -> None:
        """`Delete a template group version.`

        Args:
            external_id (Union[str, List[str]]): External ID of the template group.
            version (int): The version of the template group to delete.

        Returns:
            None

        Examples:
            Delete template groups by external id:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.templates.versions.delete("sdk-test-group", 1)
        """
        resource_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id)
        self._post(resource_path + "/delete", {"version": version})


class TemplateInstancesAPI(APIClient):
    _RESOURCE_PATH = "/templategroups/{}/versions/{}/instances"
    _LIST_CLASS = TemplateInstanceList

    def create(
        self, external_id: str, version: int, instances: Union[TemplateInstance, List[TemplateInstance]]
    ) -> Union[TemplateInstance, TemplateInstanceList]:
        """`Create one or more template instances.`

        Args:
            external_id (str): The external id of the template group.
            version (int): The version of the template group to create instances for.
            instances (Union[TemplateInstance, List[TemplateInstance]]): The instances to create.

        Returns:
            Union[TemplateInstance, TemplateInstanceList]: Created template instance(s).

        Examples:
            create new template instances for Covid-19 spread:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TemplateInstance
                >>> c = CogniteClient()
                >>> template_instance_1 = TemplateInstance(
                >>>                               external_id="norway",
                >>>                               template_name="Country",
                >>>                               field_resolvers={
                >>>                                   "name": ConstantResolver("Norway"),
                >>>                                   "demographics": ConstantResolver("norway_demographics"),
                >>>                                   "deaths": ConstantResolver("Norway_deaths"),
                >>>                                   "confirmed": ConstantResolver("Norway_confirmed"),
                >>>                                   }
                >>>                               )
                >>> template_instance_2 = TemplateInstance(
                >>>                               external_id="norway_demographics",
                >>>                               template_name="Demographics",
                >>>                               field_resolvers={
                >>>                                   "populationSize": ConstantResolver(5328000),
                >>>                                   "growthRate": ConstantResolver(value=0.02)
                >>>                                   }
                >>>                               )
                >>> c.templates.instances.create("sdk-test-group", 1, [template_instance_1, template_instance_2])
        """
        resource_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version)
        return self._create_multiple(resource_path=resource_path, items=instances)

    def upsert(
        self, external_id: str, version: int, instances: Union[TemplateGroup, List[TemplateGroup]]
    ) -> Union[TemplateInstance, TemplateInstanceList]:
        """`Upsert one or more template instances.`
        Will overwrite existing instances.

        Args:
         external_id (str): The external id of the template group.
         version (int): The version of the template group to create instances for.
         instances (Union[TemplateInstance, List[TemplateInstance]]): The instances to create.

        Returns:
         Union[TemplateInstance, TemplateInstanceList]: Created template instance(s).

        Examples:
         create new template instances for Covid-19 spread:

             >>> from cognite.client import CogniteClient
             >>> from cognite.client.data_classes import TemplateInstance
             >>> c = CogniteClient()
             >>> template_instance_1 = TemplateInstance(
             >>>        external_id="norway",
             >>>        template_name="Country",
             >>>        field_resolvers={
             >>>            "name": ConstantResolver("Norway"),
             >>>            "demographics": ConstantResolver("norway_demographics"),
             >>>            "deaths": ConstantResolver("Norway_deaths"),
             >>>            "confirmed": ConstantResolver("Norway_confirmed"),
             >>>        }
             >>>    )
             >>> template_instance_2 = TemplateInstance(external_id="norway_demographics",
             >>>       template_name="Demographics",
             >>>       field_resolvers={
             >>>           "populationSize": ConstantResolver(5328000),
             >>>           "growthRate": ConstantResolver(0.02)
             >>>           }
             >>>   )
             >>> c.templates.instances.upsert("sdk-test-group", 1, [template_instance_1, template_instance_2])
        """
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

    def retrieve_multiple(
        self, external_id: str, version: int, external_ids: List[str], ignore_unknown_ids: bool = False
    ) -> TemplateInstanceList:
        """`Retrieve multiple template instances by external id.`

        Args:
            external_id (str): The template group to retrieve instances from.
            version (int): The version of the template group.
            external_ids (List[str]): External IDs of the instances.
            ignore_unknown_ids (bool): Ignore external IDs that are not found rather than throw an exception.

        Returns:
            TemplateInstanceList: The requested template groups.

        Examples:
            Get template instances by external id:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.templates.instances.retrieve_multiple(external_id="sdk-test-group", version=1, external_ids=["abc", "def"])
        """
        resource_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version)
        return self._retrieve_multiple(
            resource_path=resource_path, external_ids=external_ids, ignore_unknown_ids=ignore_unknown_ids, wrap_ids=True
        )

    def list(
        self,
        external_id: str,
        version: int,
        limit: int = 25,
        data_set_ids: Optional[List[int]] = None,
        template_names: Optional[List[str]] = None,
    ) -> TemplateInstanceList:
        """`Lists instances in a template group.`
        Up to 1000 template instances can be retrieved in one operation.

        Args:
            external_id (str): The external id of the template group.
            version (int): The version of the template group.
            limit (int): Maximum number of template group versions to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            data_set_ids: (Optional[List[int]]): Only include instances which has one of these values in their `data_set_id` field.
            template_names: (Optional[List[str]]): Only include instances which has one of these values in their `template_name` field.

        Returns:
            TemplateInstanceList: List of requested template instances

        Examples:
            List template instances:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> template_instances_list = c.templates.instances.list("template-group-ext-id", 1, limit=5)
        """
        resource_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version)
        filter = {}
        if data_set_ids is not None:
            filter["dataSetIds"] = data_set_ids
        if template_names is not None:
            filter["template_names"] = template_names
        return self._list(resource_path=resource_path, method="POST", limit=limit, filter=filter)

    def delete(self, external_id: str, version: int, external_ids: List[str], ignore_unknown_ids: bool = False) -> None:
        """`Delete one or more template instances.`

        Args:
            external_id (Union[str, List[str]]): External ID of the template group.
            version (int): The version of the template group.
            external_ids (List[str]): The external ids of the template instances to delete
            ignore_unknown_ids (bool): Ignore external IDs that are not found rather than throw an exception.

        Returns:
            None

        Examples:
            Delete template groups by external id:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.templates.instances.delete("sdk-test-group", 1, external_id=["a", "b"])
        """
        resource_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version)
        return self._delete_multiple(
            resource_path=resource_path,
            external_ids=external_ids,
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )
