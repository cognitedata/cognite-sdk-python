from __future__ import annotations

from typing import Any

import pytest

from cognite.client.data_classes.capabilities import (
    AllScope,
    Capability,
    ProjectsAcl,
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
                "securityCategoriesAcl": {"actions": ["MEMBEROF", "LIST"], "scope": {"idScope": {"ids": [1, 2, 3]}}},
            },
            {
                "rawAcl": {
                    "actions": ["READ", "WRITE", "LIST"],
                    "scope": {"tableScope": {"dbsToTables": {"databaseName": ["my_db1", "my_db2"]}}},
                },
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
