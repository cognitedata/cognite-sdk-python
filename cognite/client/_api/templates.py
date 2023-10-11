from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Sequence, cast

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
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
from cognite.client.utils._auxiliary import interpolate_and_url_encode
from cognite.client.utils._identifier import IdentifierSequence

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class TemplatesAPI(APIClient):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.groups = TemplateGroupsAPI(config, api_version, cognite_client)
        self.versions = TemplateGroupVersionsAPI(config, api_version, cognite_client)
        self.instances = TemplateInstancesAPI(config, api_version, cognite_client)
        self.views = TemplateViewsAPI(config, api_version, cognite_client)

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
                >>>        countryQuery {
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
                >>> result = c.templates.graphql_query("template-group-ext-id", 1, query)
        """
        path = "/templategroups/{}/versions/{}/graphql"
        path = interpolate_and_url_encode(path, external_id, version)
        response = self._post(path, {"query": query})
        return GraphQlResponse._load(response.json())


class TemplateGroupsAPI(APIClient):
    _RESOURCE_PATH = "/templategroups"

    def create(self, template_groups: TemplateGroup | Sequence[TemplateGroup]) -> TemplateGroup | TemplateGroupList:
        """`Create one or more template groups.`

        Args:
            template_groups (TemplateGroup | Sequence[TemplateGroup]): No description.

        Returns:
            TemplateGroup | TemplateGroupList: Created template group(s)

        Examples:
            Create a new template group:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TemplateGroup
                >>> c = CogniteClient()
                >>> template_group_1 = TemplateGroup("sdk-test-group", "This is a test group")
                >>> template_group_2 = TemplateGroup("sdk-test-group-2", "This is another test group")
                >>> c.templates.groups.create([template_group_1, template_group_2])
        """
        return self._create_multiple(list_cls=TemplateGroupList, resource_cls=TemplateGroup, items=template_groups)

    def upsert(self, template_groups: TemplateGroup | Sequence[TemplateGroup]) -> TemplateGroup | TemplateGroupList:
        """`Upsert one or more template groups.`
        Will overwrite existing template group(s) with the same external id(s).

        Args:
            template_groups (TemplateGroup | Sequence[TemplateGroup]): No description.

        Returns:
            TemplateGroup | TemplateGroupList: Created template group(s)

        Examples:
            Upsert a template group:

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
            template_groups_processed: list[TemplateGroup] = cast(List[TemplateGroup], [template_groups])
        else:
            template_groups_processed = cast(List[TemplateGroup], template_groups)
        updated = self._post(
            path, {"items": [item.dump(camel_case=True) for item in template_groups_processed]}
        ).json()["items"]
        res = TemplateGroupList._load(updated, cognite_client=self._cognite_client)
        if is_single:
            return res[0]
        return res

    def retrieve_multiple(self, external_ids: Sequence[str], ignore_unknown_ids: bool = False) -> TemplateGroupList:
        """`Retrieve multiple template groups by external id.`

        Args:
            external_ids (Sequence[str]): External IDs
            ignore_unknown_ids (bool): Ignore external IDs that are not found rather than throw an exception.

        Returns:
            TemplateGroupList: The requested template groups.

        Examples:
            Get template groups by external id:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.templates.groups.retrieve_multiple(external_ids=["abc", "def"])
        """
        identifiers = IdentifierSequence.load(ids=None, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=TemplateGroupList,
            resource_cls=TemplateGroup,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    def list(self, limit: int | None = DEFAULT_LIMIT_READ, owners: Sequence[str] | None = None) -> TemplateGroupList:
        """`Lists template groups stored in the project based on a query filter given in the payload of this request.`
        Up to 1000 template groups can be retrieved in one operation.

        Args:
            limit (int | None): Maximum number of template groups to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            owners (Sequence[str] | None): Include template groups that have any of these values in their `owner` field.

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
        return self._list(
            list_cls=TemplateGroupList,
            resource_cls=TemplateGroup,
            method="POST",
            limit=limit,
            filter=filter,
            partitions=None,
            sort=None,
        )

    def delete(self, external_ids: str | Sequence[str], ignore_unknown_ids: bool = False) -> None:
        """`Delete one or more template groups.`

        Args:
            external_ids (str | Sequence[str]): External ID or list of external ids
            ignore_unknown_ids (bool): Ignore external IDs that are not found rather than throw an exception.

        Examples:
            Delete template groups by external id:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.templates.groups.delete(external_ids=["a", "b"])
        """
        self._delete_multiple(
            wrap_ids=True,
            identifiers=IdentifierSequence.load(external_ids=external_ids),
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )


class TemplateGroupVersionsAPI(APIClient):
    _RESOURCE_PATH = "/templategroups/{}/versions"

    def upsert(self, external_id: str, version: TemplateGroupVersion) -> TemplateGroupVersion:
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
        resource_path = interpolate_and_url_encode(self._RESOURCE_PATH, external_id) + "/upsert"
        version_res = self._post(resource_path, version.dump(camel_case=True)).json()
        return TemplateGroupVersion._load(version_res)

    def list(
        self,
        external_id: str,
        limit: int | None = DEFAULT_LIMIT_READ,
        min_version: int | None = None,
        max_version: int | None = None,
    ) -> TemplateGroupVersionList:
        """`Lists versions of a specified template group.`
        Up to 1000 template group version can be retrieved in one operation.

        Args:
            external_id (str): The external id of the template group.
            limit (int | None): Maximum number of template group versions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            min_version (int | None): (Optional[int]): Exclude versions with a version number smaller than this.
            max_version (int | None): (Optional[int]): Exclude versions with a version number larger than this.

        Returns:
            TemplateGroupVersionList: List of requested template group versions

        Examples:
            List template group versions:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> template_group_list = c.templates.versions.list("template-group-ext-id", limit=5)
        """
        resource_path = interpolate_and_url_encode(self._RESOURCE_PATH, external_id)
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

    def delete(self, external_id: str, version: int) -> None:
        """`Delete a template group version.`

        Args:
            external_id (str): External ID of the template group.
            version (int): The version of the template group to delete.

        Examples:
            Delete template groups by external id:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.templates.versions.delete("sdk-test-group", 1)
        """
        resource_path = interpolate_and_url_encode(self._RESOURCE_PATH, external_id)
        self._post(resource_path + "/delete", {"version": version})


class TemplateInstancesAPI(APIClient):
    _RESOURCE_PATH = "/templategroups/{}/versions/{}/instances"

    def create(
        self, external_id: str, version: int, instances: TemplateInstance | Sequence[TemplateInstance]
    ) -> TemplateInstance | TemplateInstanceList:
        """`Create one or more template instances.`

        Args:
            external_id (str): The external id of the template group.
            version (int): The version of the template group to create instances for.
            instances (TemplateInstance | Sequence[TemplateInstance]): The instances to create.

        Returns:
            TemplateInstance | TemplateInstanceList: Created template instance(s).

        Examples:
            Create new template instances for Covid-19 spread:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TemplateInstance
                >>> c = CogniteClient()
                >>> template_instance_1 = TemplateInstance(external_id="norway",
                >>>                               template_name="Country",
                >>>                               field_resolvers={
                >>>                                   "name": ConstantResolver("Norway"),
                >>>                                   "demographics": ConstantResolver("norway_demographics"),
                >>>                                   "deaths": ConstantResolver("Norway_deaths"),
                >>>                                   "confirmed": ConstantResolver("Norway_confirmed"),
                >>>                                   }
                >>>                               )
                >>> template_instance_2 = TemplateInstance(external_id="norway_demographics",
                >>>                               template_name="Demographics",
                >>>                               field_resolvers={
                >>>                                   "populationSize": ConstantResolver(5328000),
                >>>                                   "growthRate": ConstantResolver(value=0.02)
                >>>                                   }
                >>>                               )
                >>> c.templates.instances.create("sdk-test-group", 1, [template_instance_1, template_instance_2])
        """
        resource_path = interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version)
        return self._create_multiple(
            list_cls=TemplateInstanceList, resource_cls=TemplateInstance, resource_path=resource_path, items=instances
        )

    def upsert(
        self, external_id: str, version: int, instances: TemplateInstance | Sequence[TemplateInstance]
    ) -> TemplateInstance | TemplateInstanceList:
        """`Upsert one or more template instances.`
        Will overwrite existing instances.

        Args:
            external_id (str): The external id of the template group.
            version (int): The version of the template group to create instances for.
            instances (TemplateInstance | Sequence[TemplateInstance]): The instances to create.

        Returns:
            TemplateInstance | TemplateInstanceList: Created template instance(s).

        Examples:
            Create new template instances for Covid-19 spread:

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes import TemplateInstance
            >>> c = CogniteClient()
            >>> template_instance_1 = TemplateInstance(external_id="norway",
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
        resource_path = interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version) + "/upsert"
        updated = self._post(
            resource_path, {"items": [instance.dump(camel_case=True) for instance in instances]}
        ).json()["items"]
        res = TemplateInstanceList._load(updated, cognite_client=self._cognite_client)
        if len(res) == 1:
            return res[0]
        return res

    def update(
        self, external_id: str, version: int, item: TemplateInstanceUpdate | Sequence[TemplateInstanceUpdate]
    ) -> TemplateInstance | TemplateInstanceList:
        """`Update one or more template instances`
        Args:
            external_id (str): No description.
            version (int): No description.
            item (TemplateInstanceUpdate | Sequence[TemplateInstanceUpdate]): Templates instance(s) to update

        Returns:
            TemplateInstance | TemplateInstanceList: Updated template instance(s)

        Examples:
            Perform a partial update on a template instance::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import TemplateInstanceUpdate
                >>> c = CogniteClient()
                >>> my_update = TemplateInstanceUpdate(external_id="test").field_resolvers.add({ "name": ConstantResolver("Norway") })
                >>> res = c.templates.instances.update("sdk-test-group", 1, my_update)
        """
        resource_path = interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version)
        return self._update_multiple(
            list_cls=TemplateInstanceList,
            resource_cls=TemplateInstance,
            update_cls=TemplateInstanceUpdate,
            items=item,
            resource_path=resource_path,
        )

    def retrieve_multiple(
        self, external_id: str, version: int, external_ids: Sequence[str], ignore_unknown_ids: bool = False
    ) -> TemplateInstanceList:
        """`Retrieve multiple template instances by external id.`

        Args:
            external_id (str): The template group to retrieve instances from.
            version (int): The version of the template group.
            external_ids (Sequence[str]): External IDs of the instances.
            ignore_unknown_ids (bool): Ignore external IDs that are not found rather than throw an exception.

        Returns:
            TemplateInstanceList: The requested template groups.

        Examples:
            Get template instances by external id:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.templates.instances.retrieve_multiple(external_id="sdk-test-group", version=1, external_ids=["abc", "def"])
        """
        resource_path = interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version)
        identifiers = IdentifierSequence.load(ids=None, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=TemplateInstanceList,
            resource_cls=TemplateInstance,
            resource_path=resource_path,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    def list(
        self,
        external_id: str,
        version: int,
        limit: int | None = DEFAULT_LIMIT_READ,
        data_set_ids: Sequence[int] | None = None,
        template_names: Sequence[str] | None = None,
    ) -> TemplateInstanceList:
        """`Lists instances in a template group.`
        Up to 1000 template instances can be retrieved in one operation.

        Args:
            external_id (str): The external id of the template group.
            version (int): The version of the template group.
            limit (int | None): Maximum number of template group versions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            data_set_ids (Sequence[int] | None): (Optional[Sequence[int]]): Only include instances which has one of these values in their `data_set_id` field.
            template_names (Sequence[str] | None): (Optional[Sequence[str]]): Only include instances which has one of these values in their `template_name` field.

        Returns:
            TemplateInstanceList: List of requested template instances

        Examples:
            List template instances:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> template_instances_list = c.templates.instances.list("template-group-ext-id", 1, limit=5)
        """
        resource_path = interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version)
        filter: dict[str, Any] = {}
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

    def delete(
        self, external_id: str, version: int, external_ids: Sequence[str], ignore_unknown_ids: bool = False
    ) -> None:
        """`Delete one or more template instances.`

        Args:
            external_id (str): External ID of the template group.
            version (int): The version of the template group.
            external_ids (Sequence[str]): The external ids of the template instances to delete
            ignore_unknown_ids (bool): Ignore external IDs that are not found rather than throw an exception.

        Examples:
            Delete template groups by external id:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.templates.instances.delete("sdk-test-group", 1, external_ids=["a", "b"])
        """
        resource_path = interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version)
        self._delete_multiple(
            resource_path=resource_path,
            identifiers=IdentifierSequence.load(external_ids=external_ids),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )


class TemplateViewsAPI(APIClient):
    _RESOURCE_PATH = "/templategroups/{}/versions/{}/views"

    def create(self, external_id: str, version: int, views: View | Sequence[View]) -> View | ViewList:
        """`Create one or more template views.`

        Args:
            external_id (str): The external id of the template group.
            version (int): The version of the template group to create views for.
            views (View | Sequence[View]): The views to create.

        Returns:
            View | ViewList: Created view(s).

        Examples:
            Create new views:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.templates import View, Source
                >>> c = CogniteClient()
                >>> view = View(external_id="view",
                >>>             source=Source(
                >>>                 type='events',
                >>>                 filter={
                >>>                     "startTime": {
                >>>                         "min": "$startTime"
                >>>                     },
                >>>                     "type": "Test",
                >>>                 }
                >>>                 mappings={
                >>>                     "author": "metadata/author"
                >>>                 }
                >>>             )
                >>>        )
                >>> c.templates.views.create("sdk-test-group", 1, [view])
        """
        resource_path = interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version)
        return self._create_multiple(list_cls=ViewList, resource_cls=View, resource_path=resource_path, items=views)

    def upsert(self, external_id: str, version: int, views: View | Sequence[View]) -> View | ViewList:
        """`Upsert one or more template views.`

        Args:
            external_id (str): The external id of the template group.
            version (int): The version of the template group to create views for.
            views (View | Sequence[View]): The views to create.

        Returns:
            View | ViewList: Created view(s).

        Examples:
            Upsert new views:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.templates import View
                >>> c = CogniteClient()
                >>> view = View(external_id="view",
                >>>             source=Source(
                >>>                 type: 'events',
                >>>                 filter: {
                >>>                     startTime: {
                >>>                         min: "$startTime"
                >>>                     },
                >>>                     type: "Test",
                >>>                 }
                >>>                 mappings: {
                >>>                     author: "metadata/author"
                >>>                 }
                >>>             )
                >>>        )
                >>> c.templates.views.upsert("sdk-test-group", 1, [view])
        """
        if isinstance(views, View):
            views = [views]
        resource_path = interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version) + "/upsert"
        updated = self._post(resource_path, {"items": [view.dump(camel_case=True) for view in views]}).json()["items"]
        res = ViewList._load(updated, cognite_client=self._cognite_client)
        if len(res) == 1:
            return res[0]
        return res

    def resolve(
        self,
        external_id: str,
        version: int,
        view_external_id: str,
        input: dict[str, Any] | None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> ViewResolveList:
        """`Resolves a View.`
        It resolves the source specified in a View with the provided input and applies the mapping rules to the response.

        Args:
            external_id (str): The external id of the template group.
            version (int): The version of the template group.
            view_external_id (str): No description.
            input (dict[str, Any] | None): The input for the View.
            limit (int | None): Maximum number of views to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            ViewResolveList: The resolved items.

        Examples:
            Resolve view:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.templates.views.resolve("template-group-ext-id", 1, "view", { "startTime": 10 }, limit=5)
        """
        url_path = interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version) + "/resolve"
        return self._list(
            list_cls=ViewResolveList,
            resource_cls=ViewResolveItem,
            url_path=url_path,
            method="POST",
            limit=limit,
            other_params={"externalId": view_external_id, "input": input},
        )

    def list(self, external_id: str, version: int, limit: int | None = DEFAULT_LIMIT_READ) -> ViewList:
        """`Lists view in a template group.`
        Up to 1000 views can be retrieved in one operation.

        Args:
            external_id (str): The external id of the template group.
            version (int): The version of the template group.
            limit (int | None): Maximum number of views to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            ViewList: List of requested views

        Examples:
            List views:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.templates.views.list("template-group-ext-id", 1, limit=5)
        """
        resource_path = interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version)
        return self._list(list_cls=ViewList, resource_cls=View, resource_path=resource_path, method="POST", limit=limit)

    def delete(
        self,
        external_id: str,
        version: int,
        view_external_id: Sequence[str] | str,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete one or more views.`

        Args:
            external_id (str): External ID of the template group.
            version (int): The version of the template group.
            view_external_id (Sequence[str] | str): The external ids of the views to delete
            ignore_unknown_ids (bool): Ignore external IDs that are not found rather than throw an exception.

        Examples:
            Delete views by external id:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.templates.views.delete("sdk-test-group", 1, external_id=["a", "b"])
        """
        resource_path = interpolate_and_url_encode(self._RESOURCE_PATH, external_id, version)
        self._delete_multiple(
            resource_path=resource_path,
            identifiers=IdentifierSequence.load(external_ids=view_external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
        )
