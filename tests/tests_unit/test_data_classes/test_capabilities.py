from __future__ import annotations

import re
from typing import Any

import pytest

import cognite.client.data_classes.capabilities as capabilities_module  # F401
from cognite.client.data_classes import Group, GroupList
from cognite.client.data_classes.capabilities import (
    AllProjectsScope,
    AllScope,
    Capability,
    CurrentUserScope,
    DataModelInstancesAcl,
    DataSetsAcl,
    EventsAcl,
    ExperimentsAcl,
    GroupsAcl,
    ProjectCapability,
    ProjectCapabilityList,
    ProjectsAcl,
    RawAcl,
    SAPWritebackAcl,
    SpaceIDScope,
    TableScope,
    UnknownAcl,
    UnknownScope,
)
from tests.utils import get_url


def all_acls():
    yield from [
        {"annotationsAcl": {"actions": ["WRITE", "READ", "SUGGEST", "REVIEW"], "scope": {"all": {}}}},
        {"assetsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"assetsAcl": {"actions": ["READ", "WRITE"], "scope": {"datasetScope": {"ids": ["372"]}}}},
        {"auditlogAcl": {"actions": ["READ"], "scope": {"all": {}}}},
        {"dataModelInstancesAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"dataModelInstancesAcl": {"actions": ["READ"], "scope": {"spaceScope": {"externalIds": ["maintain"]}}}},
        {
            "dataModelInstancesAcl": {
                "actions": ["WRITE_PROPERTIES"],
                "scope": {"spaceIdScope": {"spaceIds": ["tech-space"]}},
            }
        },
        {"dataModelsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {
            "dataModelsAcl": {
                "actions": ["READ"],
                "scope": {"dataModelScope": {"externalIds": ["maintain", "main-data"]}},
            }
        },
        {"datasetsAcl": {"actions": ["READ", "WRITE", "OWNER"], "scope": {"all": {}}}},
        {"datasetsAcl": {"actions": ["READ", "WRITE", "OWNER"], "scope": {"idScope": {"ids": ["2918026428"]}}}},
        {"diagramParsingAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"digitalTwinAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"documentFeedbackAcl": {"actions": ["CREATE", "READ", "DELETE"], "scope": {"all": {}}}},
        {"documentPipelinesAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"entitymatchingAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"eventsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"eventsAcl": {"actions": ["READ", "WRITE"], "scope": {"datasetScope": {"ids": ["372"]}}}},
        {"eventsAcl": {"actions": ["READ"], "scope": {"datasetScope": {"ids": ["233257", "372989"]}}}},
        {"extractionConfigsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"extractionPipelinesAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"extractionRunsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"filePipelinesAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"filesAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"filesAcl": {"actions": ["READ", "WRITE"], "scope": {"datasetScope": {"ids": ["2332579", "372"]}}}},
        {"functionsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"genericsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"groupsAcl": {"actions": ["LIST", "READ", "DELETE", "UPDATE", "CREATE"], "scope": {"all": {}}}},
        {"groupsAcl": {"actions": ["READ", "CREATE", "UPDATE", "DELETE"], "scope": {"currentuserscope": {}}}},
        {"hostedExtractorsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"labelsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"locationFiltersAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"modelHostingAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"monitoringTasksAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"notificationsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"pipelinesAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"postgresGatewayAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"projectsAcl": {"actions": ["UPDATE", "LIST", "READ", "CREATE", "DELETE"], "scope": {"all": {}}}},
        {"rawAcl": {"actions": ["READ", "WRITE", "LIST"], "scope": {"all": {}}}},
        {
            "rawAcl": {
                "actions": ["READ", "WRITE", "LIST"],
                "scope": {"tableScope": {"dbsToTables": {"no table in this": {}}}},
            }
        },
        {
            "rawAcl": {
                "actions": ["READ", "WRITE", "LIST"],
                "scope": {"tableScope": {"dbsToTables": {"test db 1": {"tables": ["empty tbl", "test tbl 1"]}}}},
            }
        },
        {"relationshipsAcl": {"actions": ["READ"], "scope": {"all": {}}}},
        {"relationshipsAcl": {"actions": ["READ"], "scope": {"datasetScope": {"ids": ["372", "2332579"]}}}},
        {"roboticsAcl": {"actions": ["READ", "CREATE", "UPDATE", "DELETE"], "scope": {"all": {}}}},
        {"roboticsAcl": {"actions": ["READ"], "scope": {"datasetScope": {"ids": ["583194012260066"]}}}},
        {"sapWritebackAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"sapWritebackAcl": {"actions": ["READ", "WRITE"], "scope": {"instancesScope": {"instances": ["123", "456"]}}}},
        {"sapWritebackRequestsAcl": {"actions": ["WRITE", "LIST"], "scope": {"all": {}}}},
        {
            "sapWritebackRequestsAcl": {
                "actions": ["WRITE", "LIST"],
                "scope": {"instancesScope": {"instances": ["123", "456"]}},
            }
        },
        {"scheduledCalculationsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {
            "securityCategoriesAcl": {
                "actions": ["DELETE", "MEMBEROF", "LIST", "CREATE", "UPDATE"],
                "scope": {"all": {}},
            }
        },
        {
            "securityCategoriesAcl": {
                "actions": ["MEMBEROF", "LIST", "CREATE", "UPDATE", "DELETE"],
                "scope": {"idscope": {"ids": ["7398", "3018"]}},
            }
        },
        {"seismicAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"seismicAcl": {"actions": ["WRITE"], "scope": {"partition": {"partitionIds": ["123", 456]}}}},
        {"sequencesAcl": {"actions": ["READ"], "scope": {"all": {}}}},
        {"sequencesAcl": {"actions": ["WRITE"], "scope": {"datasetScope": {"ids": ["2332579", "372"]}}}},
        {"sessionsAcl": {"actions": ["LIST", "CREATE", "DELETE"], "scope": {"all": {}}}},
        {"templateGroupsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"templateGroupsAcl": {"actions": ["READ", "WRITE"], "scope": {"datasetScope": {"ids": ["1", "42"]}}}},
        {"templateInstancesAcl": {"actions": ["READ", "WRITE"], "scope": {"datasetScope": {"ids": ["4", "365"]}}}},
        {"templateInstancesAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"threedAcl": {"actions": ["READ", "CREATE", "UPDATE", "DELETE"], "scope": {"all": {}}}},
        {"timeSeriesAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"timeSeriesAcl": {"actions": ["READ", "WRITE"], "scope": {"datasetScope": {"ids": ["233579", "372"]}}}},
        {"timeSeriesAcl": {"actions": ["READ"], "scope": {"idscope": {"ids": ["2495"]}}}},
        {"timeSeriesAcl": {"actions": ["WRITE", "READ"], "scope": {"assetRootIdScope": {"rootIds": ["58"]}}}},
        {"transformationsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"transformationsAcl": {"actions": ["READ", "WRITE"], "scope": {"datasetScope": {"ids": ["94"]}}}},
        {"visionModelAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"wellsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"workflowOrchestrationAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {
            "workflowOrchestrationAcl": {
                "actions": ["READ", "WRITE"],
                "scope": {"datasetScope": {"ids": ["2332579", "372"]}},
            }
        },
    ]


@pytest.fixture
def cap_with_extra_key():
    return {
        "extra-key": {"urlNames": ["my-sandbox"]},
        "projectsAcl": {"actions": ["UPDATE", "LIST", "READ"], "scope": {"all": {}}},
    }


@pytest.fixture
def unknown_cap_with_extra_key():
    return {
        "extra-key": {"urlNames": ["my-sandbox"]},
        "veryUnknownAcl": {"actions": ["DELETE"], "scope": {"all": {}}},
    }


class TestCapabilities:
    @pytest.mark.parametrize("access_control", list(all_acls()))
    def test_loads_to_known(self, access_control):
        cap = Capability.load(access_control, allow_unknown=False)

        assert not isinstance(cap, UnknownAcl)
        assert not isinstance(cap.scope, UnknownScope)
        assert all(isinstance(action, Capability.Action) for action in cap.actions)

    @pytest.mark.parametrize(
        "raw",
        [
            {
                "assetsAcl": {
                    "actions": ["READ"],
                    "scope": {"datasetScope": {"ids": [1, 2, 3]}},
                }
            },
            {
                "securityCategoriesAcl": {"actions": ["MEMBEROF", "LIST"], "scope": {"idscope": {"ids": [1, 2, 3]}}},
            },
            {
                "rawAcl": {
                    "actions": ["READ", "WRITE", "LIST"],
                    "scope": {"tableScope": {"dbsToTables": {"databaseName": {"tables": ["my_db1", "my_db2"]}}}},
                },
            },
            {
                "experimentAcl": {
                    "actions": ["USE"],
                    "scope": {"experimentscope": {"experiments": ["workflowOrchestrator"]}},
                }
            },
            {
                "dataModelsAcl": {
                    "actions": ["READ"],
                    "scope": {
                        "spaceIdScope": {
                            "spaceIds": [
                                "APM_Config",
                                "cdf_apm",
                                "cdf_infield",
                                "cdf_core",
                                "cdf_apps_shared",
                                "APM_SourceData",
                            ]
                        }
                    },
                },
            },
        ],
    )
    def test_load_dump(self, raw: dict[str, Any]) -> None:
        capability = Capability.load(raw)
        assert capability.dump(camel_case=True) == raw

    def test_load_dump__extra_top_level_keys(self, cap_with_extra_key) -> None:
        capability = Capability.load(cap_with_extra_key)
        assert isinstance(capability, ProjectsAcl)
        assert isinstance(capability.scope, AllScope)
        assert all(isinstance(action, ProjectsAcl.Action) for action in capability.actions)

        assert capability.dump(camel_case=True) != cap_with_extra_key
        cap_with_extra_key.pop("extra-key")
        assert capability.dump(camel_case=True) == cap_with_extra_key

    def test_load_dump_unknown__extra_top_level_keys(self, unknown_cap_with_extra_key) -> None:
        match = (
            "^Unable to parse Capability, none of the top-level keys in the input, "
            r"\['extra-key', 'veryUnknownAcl'\], matched known ACLs"
        )
        with pytest.raises(ValueError, match=match):
            Capability.load(unknown_cap_with_extra_key)

        # API responses should never fail to load, even when crazy:
        loaded = Capability.load(unknown_cap_with_extra_key, allow_unknown=True)
        assert loaded.dump() == unknown_cap_with_extra_key

    @pytest.mark.parametrize(
        "raw", [{"dataproductAcl": {"actions": ["UTILIZE"], "scope": {"components": {"ids": [1, 2, 3]}}}}]
    )
    def test_load_dump_unknown(self, raw: dict[str, Any]) -> None:
        capability = Capability.load(raw, allow_unknown=True)
        assert isinstance(capability, UnknownAcl)
        assert isinstance(capability.scope, UnknownScope)
        assert all(isinstance(action, Capability.Action) for action in capability.actions)
        assert capability.raw_data == raw
        assert capability.dump(camel_case=True) == raw

    def test_load_capability_misspelled_acl(self, unknown_acls_items):
        unknown_acl, *_ = unknown_acls_items
        exp_err_msg = re.escape(
            "Unable to parse Capability, none of the top-level keys in the input, ['funkyAssetsAcl'], "
            "matched known ACLs, - or - multiple was found. Pass `allow_unknown=True` to force loading "
            "it as an unknown capability. Did you mean one of: ['assetsAcl', 'functionsAcl']? List of all "
            "ACLs: from cognite.client.data_classes.capabilities import ALL_CAPABILITIES"
        )
        # difflib should give some nice suggestions for misspelling:
        # - funkyAssetsAcl -> [assetsAcl, functionsAcl]
        with pytest.raises(ValueError, match=f"^{exp_err_msg}$"):
            Capability.load(unknown_acl, allow_unknown=False)

        # when difflib doesnt find any matches, it should be omitted from the err. msg:
        with pytest.raises(ValueError, match="force loading it as an unknown capability. List of all ACLs"):
            Capability.load(
                {"does not match anything really": {"actions": ["READ"], "scope": {"all": {}}}},
                allow_unknown=False,
            )

    @pytest.mark.parametrize(
        "acl_cls_name, bad_action, dumped",
        [
            ("AssetsAcl", "SONG-WRITER", {"assetsAcl": {"actions": ["READ", "SONG-WRITER"], "scope": {"all": {}}}}),
            (
                "DocumentFeedbackAcl",
                "CTRL-ALT-DELETE",
                {"documentFeedbackAcl": {"actions": ["CREATE", "CTRL-ALT-DELETE"], "scope": {"all": {}}}},
            ),
            (
                "FilesAcl",
                "COMPRESS",
                {"filesAcl": {"actions": ["COMPRESS"], "scope": {"datasetScope": {"ids": ["2332579", "372"]}}}},
            ),
            (
                "NotificationsAcl",
                "SEND-PIGEON",
                {"notificationsAcl": {"actions": ["READ", "SEND-PIGEON"], "scope": {"all": {}}}},
            ),
        ],
    )
    def test_load__action_does_not_exist(self, acl_cls_name: str, bad_action: str, dumped: dict[str, Any]) -> None:
        with pytest.raises(ValueError, match=rf"^'{bad_action}' is not a valid {acl_cls_name}.Action$"):
            Capability.load(dumped)

        assert Capability.load(dumped, allow_unknown=True)

    @pytest.mark.parametrize("has_write_allscope", (True, False))
    @pytest.mark.parametrize("has_write_props_allscope", (True, False))
    @pytest.mark.parametrize("has_write_in_same_scope", (True, False))
    def test_load_data_model_instances__with_write_properties(
        self, cognite_client, has_write_allscope, has_write_props_allscope, has_write_in_same_scope
    ):
        # WRITE_PROPERTIES grants a subset of capabilities of WRITE, so we ensure that having just WRITE
        # won't cause WRITE_PROPERTIES to be reported as missing.
        existing = [
            DataModelInstancesAcl([DataModelInstancesAcl.Action.Write], SpaceIDScope(["foo", "this"])),
            DataModelInstancesAcl([DataModelInstancesAcl.Action.Write_Properties], SpaceIDScope(["bar"])),
        ]
        if has_write_allscope:
            existing.append(DataModelInstancesAcl([DataModelInstancesAcl.Action.Write], AllScope()))
        if has_write_in_same_scope:
            existing.append(DataModelInstancesAcl([DataModelInstancesAcl.Action.Write], SpaceIDScope(["too_much"])))
        if has_write_props_allscope:
            existing.append(DataModelInstancesAcl([DataModelInstancesAcl.Action.Write_Properties], AllScope()))

        desired = [
            DataModelInstancesAcl([DataModelInstancesAcl.Action.Write_Properties], SpaceIDScope(["foo", "too_much"])),
            DataModelInstancesAcl([DataModelInstancesAcl.Action.Write_Properties], SpaceIDScope(["bar"])),
        ]
        if has_write_allscope or has_write_props_allscope or has_write_in_same_scope:
            expected_missing = []
        else:
            expected_missing = [
                DataModelInstancesAcl([DataModelInstancesAcl.Action.Write_Properties], SpaceIDScope(["too_much"]))
            ]

        missing = cognite_client.iam.compare_capabilities(existing_capabilities=existing, desired_capabilities=desired)
        assert missing == expected_missing

    def test_create_capability_forget_initializing_scope(self):
        # Ensure these do not raise. All other scopes require arguments and so will
        # raise appropriate errors if not initialized.
        ds1 = DataSetsAcl([DataSetsAcl.Action.Read], scope=DataSetsAcl.Scope.All)
        ds2 = DataSetsAcl([DataSetsAcl.Action.Read], scope=DataSetsAcl.Scope.All())
        assert ds1.dump() == ds2.dump()

        ds1 = DataSetsAcl([DataSetsAcl.Action.Read], scope=AllScope)
        ds2 = DataSetsAcl([DataSetsAcl.Action.Read], scope=AllScope())
        assert ds1.dump() == ds2.dump()

        grp1 = GroupsAcl([GroupsAcl.Action.Delete], scope=GroupsAcl.Scope.CurrentUser)
        grp2 = GroupsAcl([GroupsAcl.Action.Delete], scope=GroupsAcl.Scope.CurrentUser())
        assert grp1.dump() == grp2.dump()

        grp1 = GroupsAcl([GroupsAcl.Action.Delete], scope=CurrentUserScope)
        grp2 = GroupsAcl([GroupsAcl.Action.Delete], scope=CurrentUserScope())
        assert grp1.dump() == grp2.dump()

    def test_create_capability_forget_initializing_scope__not_supported(self):
        with pytest.raises(ValueError, match="DataSetsAcl got an unknown scope"):
            DataSetsAcl(actions=[DataSetsAcl.Action.Read], scope=CurrentUserScope)

        with pytest.raises(ValueError, match="DataSetsAcl got an unknown scope"):
            DataSetsAcl(actions=[DataSetsAcl.Action.Read], scope=GroupsAcl.Scope.CurrentUser)

        with pytest.raises(ValueError, match="ExperimentsAcl got an unknown scope"):
            ExperimentsAcl(actions=[ExperimentsAcl.Action.Use], scope=AllScope)


@pytest.fixture
def proj_cap_allprojects_dct():
    return {"labelsAcl": {"actions": ["READ"], "scope": {"all": {}}}, "projectScope": {"allProjects": {}}}


@pytest.fixture
def project_name():
    return "my-project"


@pytest.fixture
def proj_capabs_list(project_name):
    return ProjectCapabilityList(
        [
            ProjectCapability(
                capability=EventsAcl([EventsAcl.Action.Read], scope=EventsAcl.Scope.All()),
                project_scope=ProjectCapability.Scope.All(),
            ),
            ProjectCapability(
                capability=EventsAcl([EventsAcl.Action.Write], scope=EventsAcl.Scope.DataSet(["1", 2])),
                project_scope=ProjectCapability.Scope.Projects([project_name]),
            ),
            ProjectCapability(
                capability=RawAcl(
                    [RawAcl.Action.Read], scope=RawAcl.Scope.Table({"my_db": ["my_table", "my_other_table"]})
                ),
                project_scope=ProjectCapability.Scope.Projects([project_name]),
            ),
            ProjectCapability(
                capability=SAPWritebackAcl(
                    [SAPWritebackAcl.Action.Read, SAPWritebackAcl.Action.Write],
                    scope=SAPWritebackAcl.Scope.All(),
                ),
                project_scope=ProjectCapability.Scope.Projects([project_name]),
            ),
        ]
    )


@pytest.fixture
def unknown_acls_items():
    return [
        # Unknown capability:
        {"funkyAssetsAcl": {"actions": ["READ"], "scope": {"all": {}}}},
        # Unknown action:
        {"assetsAcl": {"actions": ["READ", "UN-KN-OWN"], "scope": {"all": {}}}},
        # Unknown scope:
        {"assetsAcl": {"actions": ["READ"], "scope": {"astronautSpace": {"name": ["buzz"]}}}},
        # Unknown -everything-:
        {"funkyAssetsAcl": {"actions": ["UN-KN-OWN"], "scope": {"astronautSpace": {"name": ["buzz"]}}}},
    ]


@pytest.fixture
def mock_groups_resp(httpx_mock, cognite_client, unknown_acls_items):
    response_body = {
        "items": [
            {
                "name": "my name",
                "id": 123,
                "source_id": "something-uuid-like",
                "capabilities": [unknown],
            }
            for unknown in unknown_acls_items
        ]
    }
    url_pattern = get_url(cognite_client.iam.groups) + "/groups?all=false"
    httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json=response_body)
    yield httpx_mock


@pytest.fixture
def mock_token_inspect_resp(httpx_mock, cognite_client, unknown_acls_items):
    response_body = {
        "subject": "a49ba849-c0d7-abcd-dcba-8a1f0366aaaf",
        "projects": [{"projectUrlName": "my-sandbox", "groups": [229705, 863871]}],
        "capabilities": [{"projectScope": {"projects": ["my-sandbox"]}, **unknown} for unknown in unknown_acls_items],
    }
    url_pattern = get_url(cognite_client.iam.token) + "/api/v1/token/inspect"
    httpx_mock.add_response(method="GET", url=url_pattern, status_code=200, json=response_body)
    yield httpx_mock


class TestCogniteClientDoesntRaiseOnUnknownAcls:
    def test_groups_list(self, cognite_client, mock_groups_resp, unknown_acls_items):
        groups = cognite_client.iam.groups.list()

        expected = [[unknown_acl] for unknown_acl in unknown_acls_items]
        assert expected == [g["capabilities"] for g in groups.dump()]

        # Ensure that the capabilities that did -not- raise from groups/list, would raise for a normal user:
        acl_err_match = r"top-level keys in the input, \['funkyAssetsAcl'\], matched known ACLs"
        action_err_match = "^'UN-KN-OWN' is not a valid AssetsAcl.Action$"
        scope_err_match = "^Could not instantiate AssetsAcl due to: Unable to parse Scope, 'astronautSpace' is not"
        with pytest.raises(ValueError, match=acl_err_match):
            GroupList.load(groups.dump(camel_case=True))

        # ...and ensure each individual (acl/action/scope) raises:
        u1, u2, u3, u4 = unknown_acls_items
        group = {"name": "me", "id": 123, "source_id": "huh"}
        with pytest.raises(ValueError, match=acl_err_match):
            Group.load({**group, "capabilities": [u1]})  # Unknown capability
        with pytest.raises(ValueError, match=action_err_match):
            Group.load({**group, "capabilities": [u2]})  # Unknown action
        with pytest.raises(ValueError, match=scope_err_match):
            Group.load({**group, "capabilities": [u3]})  # Unknown scope
        with pytest.raises(ValueError, match=acl_err_match):
            Group.load({**group, "capabilities": [u4]})  # Unknown -everything-

    def test_token_inspect(self, cognite_client, mock_token_inspect_resp):
        # Mostly a repeat of test_groups_list, ensuring token/inspect won't ever raise
        assert cognite_client.iam.token.inspect()


class TestProjectCapabilityList:
    def test_project_scope_is_all_projects(self, proj_cap_allprojects_dct):
        loaded = ProjectCapability.load(proj_cap_allprojects_dct.copy())
        assert type(loaded.project_scope) is AllProjectsScope

        loaded = ProjectCapabilityList.load([proj_cap_allprojects_dct])
        assert type(loaded[0].project_scope) is AllProjectsScope


class TestIAMCompareCapabilities:
    @pytest.mark.parametrize(
        "capability",
        [
            EventsAcl([EventsAcl.Action.Read], scope=EventsAcl.Scope.All()),
            EventsAcl([EventsAcl.Action.Write], scope=EventsAcl.Scope.DataSet([1, "2"])),
            RawAcl([RawAcl.Action.Read], scope=RawAcl.Scope.Table({"my_db": ["my_table"]})),
            SAPWritebackAcl([SAPWritebackAcl.Action.Write], scope=SAPWritebackAcl.Scope.Instances(["1", "2"])),
        ],
    )
    def test_has_capability(
        self, cognite_client, proj_capabs_list: ProjectCapabilityList, project_name: str, capability: Capability
    ) -> None:
        assert not cognite_client.iam.compare_capabilities(proj_capabs_list, [capability], project=project_name)

    @pytest.mark.parametrize(
        "capability",
        [
            EventsAcl([EventsAcl.Action.Write], scope=EventsAcl.Scope.All()),
            EventsAcl([EventsAcl.Action.Write], scope=EventsAcl.Scope.DataSet(["3"])),
            EventsAcl([EventsAcl.Action.Write], scope=EventsAcl.Scope.DataSet([3])),
            RawAcl([RawAcl.Action.Read], scope=RawAcl.Scope.Table({"my_db": ["unknown_table"]})),
            RawAcl([RawAcl.Action.Read], scope=RawAcl.Scope.Table({"unknown_db": ["my_table"]})),
        ],
    )
    def test_is_missing_capability(
        self, cognite_client, proj_capabs_list: ProjectCapabilityList, project_name: str, capability: Capability
    ) -> None:
        missing_acls = cognite_client.iam.compare_capabilities(proj_capabs_list, [capability], project=project_name)
        assert missing_acls == [capability]

        missing_acls = cognite_client.iam.compare_capabilities(
            proj_capabs_list, [capability], project="do-es-nt-ex-is-ts"
        )
        assert missing_acls == [capability]

    def test_partly_missing_capabilities(
        self, cognite_client, proj_capabs_list: ProjectCapabilityList, project_name: str
    ) -> None:
        has = RawAcl([RawAcl.Action.Read], scope=RawAcl.Scope.Table({"my_db": ["my_table"]}))
        has_also = EventsAcl([EventsAcl.Action.Write], scope=EventsAcl.Scope.DataSet([1, "2"]))
        has_not = RawAcl([RawAcl.Action.Read], scope=RawAcl.Scope.Table({"my_db": ["unknown_table"]}))

        missing_acls = cognite_client.iam.compare_capabilities(
            proj_capabs_list, [has, has_not, has_also], project=project_name
        )
        assert missing_acls == [has_not]

    def test_raw_acl_database_scope_only(self, cognite_client):
        # Would fail with: 'ValueError: No capabilities given' prior to 7.2.1 due to a bug in 'as_tuples'.
        has_all_scope = RawAcl([RawAcl.Action.Read], AllScope)
        has_db_scope = RawAcl([RawAcl.Action.Read], TableScope(dbs_to_tables={"db1": []}))
        assert not cognite_client.iam.compare_capabilities(has_all_scope, has_db_scope)

    @pytest.mark.parametrize(
        "extra_existing",
        [
            [],
            [RawAcl(actions=[RawAcl.Action.Read], scope=AllScope)],
            [RawAcl(actions=[RawAcl.Action.Read], scope=TableScope({"db1": []}))],
            [RawAcl(actions=[RawAcl.Action.Read], scope=TableScope({"db1": {"tables": []}}))],
        ],
    )
    def test_raw_acl_database_scope(self, cognite_client, extra_existing):
        existing = [
            RawAcl([RawAcl.Action.Read], RawAcl.Scope.Table({"db1": ["t1"]})),
            RawAcl([RawAcl.Action.Read], RawAcl.Scope.Table({"db1": ["t1", "t2"]})),
            RawAcl([RawAcl.Action.Read], RawAcl.Scope.Table({"db2": ["t1", "t2"]})),
            *extra_existing,
        ]
        desired = [
            RawAcl([RawAcl.Action.Read], RawAcl.Scope.Table({"db1": ["t1", "t2", "t3"]})),
            RawAcl([RawAcl.Action.Write], RawAcl.Scope.Table({"db1": ["t1"]})),
        ]
        missing = cognite_client.iam.compare_capabilities(existing, desired)
        if extra_existing:
            assert missing == [RawAcl([RawAcl.Action.Write], RawAcl.Scope.Table({"db1": ["t1"]}))]
        else:
            assert len(missing) == 2
            assert RawAcl([RawAcl.Action.Read], RawAcl.Scope.Table({"db1": ["t3"]})) in missing
            assert RawAcl([RawAcl.Action.Write], RawAcl.Scope.Table({"db1": ["t1"]})) in missing

    def test_unknown_existing_capability(self, cognite_client):
        desired = [Capability.load({"datasetsAcl": {"actions": ["READ"], "scope": {"all": {}}}})]
        unknown = Capability.load(
            {"dataproductAcl": {"actions": ["UTILIZE"], "scope": {"components": {"ids": [1, 2, 3]}}}},
            allow_unknown=True,
        )
        missing = cognite_client.iam.compare_capabilities(unknown, desired)
        assert missing == desired

    def test_legacy_capability(self, cognite_client):
        legacy = [Capability.load({"modelHostingAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}})]
        desired = [Capability.load({"modelHostingAcl": {"actions": ["READ"], "scope": {"all": {}}}})]

        missing = cognite_client.iam.compare_capabilities(legacy, desired)
        assert not missing


@pytest.mark.parametrize(
    "dct",
    [
        {
            "securityCategoriesAcl": {
                "actions": ["MEMBEROF", "LIST", "CREATE", "UPDATE", "DELETE"],
                "scope": {"idScope": {"ids": ["2495"]}},
            }
        },
        {"timeSeriesAcl": {"actions": ["READ"], "scope": {"idScope": {"ids": ["2495"]}}}},
    ],
)
def test_idscopes_lower_case(dct):
    # These Acls expect "idscope", not "idScope":
    with pytest.raises(
        ValueError,
        match=re.escape("got an unknown scope: IDScope(ids=[2495]), expected an instance of one of: "),
    ):
        Capability.load(dct)


def test_idscopes_camel_case():
    # This Acl expect "idScope", not "idscope":
    dct = {"datasetsAcl": {"actions": ["READ", "WRITE", "OWNER"], "scope": {"idscope": {"ids": ["2495"]}}}}
    with pytest.raises(ValueError) as err:
        Capability.load(dct)
    assert err.value.args[0].startswith(
        "Could not instantiate DataSetsAcl due to: DataSetsAcl got an unknown scope: IDScopeLowerCase(ids=[2495]), "
        "expected an instance of one of: [DataSetsAcl.Scope.All, DataSetsAcl.Scope.ID]"
    )


@pytest.mark.parametrize("capability", Capability.__subclasses__())
def test_show_example_usage(capability):
    # This test ensures that the example usage given in error messages etc. works by executing it.
    if capability is UnknownAcl:
        assert not capability.show_example_usage()
    elif capability is capabilities_module.LegacyCapability:
        pytest.skip("LegacyCapability is abstract")
    else:
        cmd = capability.show_example_usage().removeprefix("Example usage: ")
        exec(f"{capability.__name__} = capabilities_module.{capability.__name__}", globals())
        exec(cmd)
