from __future__ import annotations

import re
from typing import Any

import pytest

import cognite.client.data_classes.capabilities as capabilities_module  # noqa F401
from cognite.client.data_classes.capabilities import (
    AllProjectsScope,
    AllScope,
    Capability,
    CurrentUserScope,
    DataSetsAcl,
    EventsAcl,
    ExperimentsAcl,
    GroupsAcl,
    ProjectCapability,
    ProjectCapabilityList,
    ProjectsAcl,
    RawAcl,
    TableScope,
    UnknownAcl,
    UnknownScope,
)


def all_acls():
    yield from [
        {"annotationsAcl": {"actions": ["WRITE", "READ", "SUGGEST", "REVIEW"], "scope": {"all": {}}}},
        {"assetsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"assetsAcl": {"actions": ["READ", "WRITE"], "scope": {"datasetScope": {"ids": ["372"]}}}},
        {"dataModelInstancesAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"dataModelsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"datasetsAcl": {"actions": ["READ", "WRITE", "OWNER"], "scope": {"all": {}}}},
        {"datasetsAcl": {"actions": ["READ", "WRITE", "OWNER"], "scope": {"idScope": {"ids": ["2918026428"]}}}},
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
        {"groupsAcl": {"actions": ["LIST", "READ", "DELETE", "UPDATE", "CREATE"], "scope": {"all": {}}}},
        {"groupsAcl": {"actions": ["READ", "CREATE", "UPDATE", "DELETE"], "scope": {"currentuserscope": {}}}},
        {"hostedExtractorsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"labelsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"monitoringTasksAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"notificationsAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"pipelinesAcl": {"actions": ["READ", "WRITE"], "scope": {"all": {}}}},
        {"projectsAcl": {"actions": ["UPDATE", "LIST", "READ"], "scope": {"all": {}}}},
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
        cap = Capability.load(access_control)

        assert not isinstance(cap, UnknownAcl)
        assert not isinstance(cap.scope, UnknownScope)
        for action in cap.actions:
            assert action is not UnknownAcl.Action.Unknown

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
        with pytest.raises(ValueError, match="^Unable to parse capability from API-response"):
            Capability.load(unknown_cap_with_extra_key)

    @pytest.mark.parametrize(
        "raw", [{"dataproductAcl": {"actions": ["UTILIZE"], "scope": {"components": {"ids": [1, 2, 3]}}}}]
    )
    def test_load_dump_unknown(self, raw: dict[str, Any]) -> None:
        capability = Capability.load(raw)
        assert isinstance(capability, UnknownAcl)
        assert isinstance(capability.scope, UnknownScope)
        assert all(action is UnknownAcl.Action.Unknown for action in capability.actions)
        assert capability.raw_data == raw
        assert capability.dump(camel_case=True) == raw

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
        with pytest.raises(ValueError, match=rf"^'{bad_action}' is not a valid {acl_cls_name} Action$"):
            Capability.load(dumped)

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
        with pytest.raises(ValueError, match="^DataSetsAcl got an unknown scope"):
            DataSetsAcl(actions=[DataSetsAcl.Action.Read], scope=CurrentUserScope)

        with pytest.raises(ValueError, match="^DataSetsAcl got an unknown scope"):
            DataSetsAcl(actions=[DataSetsAcl.Action.Read], scope=GroupsAcl.Scope.CurrentUser)

        with pytest.raises(ValueError, match="^ExperimentsAcl got an unknown scope"):
            ExperimentsAcl(actions=[ExperimentsAcl.Action.Use], scope=AllScope)


@pytest.fixture
def proj_cap_allprojects_dct():
    return {
        "labelsAcl": {"actions": ["READ"], "scope": {"all": {}}},
        "projectScope": {"allProjects": {}},
    }


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
        ]
    )


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
        "extra_existing, no_read_missing",
        [
            ([], False),
            ([RawAcl(actions=[RawAcl.Action.Read], scope=AllScope)], True),
            ([RawAcl(actions=[RawAcl.Action.Read], scope=TableScope({"db1": []}))], True),
            ([RawAcl(actions=[RawAcl.Action.Read], scope=TableScope({"db1": {"tables": []}}))], True),
        ],
    )
    def test_raw_acl_database_scope(self, cognite_client, extra_existing, no_read_missing):
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
        if no_read_missing:
            assert missing == [RawAcl([RawAcl.Action.Write], RawAcl.Scope.Table(dbs_to_tables={"db1": ["t1"]}))]
        else:
            assert missing == [
                RawAcl([RawAcl.Action.Read], RawAcl.Scope.Table(dbs_to_tables={"db1": ["t3"]})),
                RawAcl([RawAcl.Action.Write], RawAcl.Scope.Table(dbs_to_tables={"db1": ["t1"]})),
            ]


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
        "DataSetsAcl got an unknown scope: IDScopeLowerCase(ids=[2495]), expected an instance of one of: "
        "[DataSetsAcl.Scope.All, DataSetsAcl.Scope.ID]"
    )


@pytest.mark.parametrize("capability", Capability.__subclasses__())
def test_show_example_usage(capability):
    if capability is UnknownAcl:
        with pytest.raises(NotImplementedError):
            capability.show_example_usage()
    else:
        cmd = capability.show_example_usage()[15:]  # TODO PY39: .removeprefix
        exec(f"{capability.__name__} = capabilities_module.{capability.__name__}")
        exec(cmd)
