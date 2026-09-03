from __future__ import annotations

from cognite.client.data_classes.integrations import (
    Action,
    ConfigRevision,
    Extractor,
    Integration,
    IntegrationError,
    IntegrationUpdate,
    Task,
)
from cognite.client.data_classes.integrations.tasks import SyncResult, TaskHistory

INTEGRATION_DUMPED = {
    "externalId": "my-integration",
    "extractor": {"externalId": "cognite-simple-influxdb-extractor", "version": "1.0.0"},
    "name": "My integration",
    "description": "A test integration",
    "metadata": {"key": "value"},
    "allowedNotSeenMinutes": 60,
    "lastSeen": 123,
    "lastConfigRevision": 2,
    "activeConfigRevision": "local",
    "tasks": [{"type": "continuous", "name": "poll", "action": True, "description": "Polls for data"}],
    "createdTime": 1,
    "lastUpdatedTime": 2,
}


class TestIntegration:
    def test_load_dump_round_trip(self) -> None:
        loaded = Integration._load(INTEGRATION_DUMPED)

        assert loaded.external_id == "my-integration"
        assert loaded.extractor.external_id == "cognite-simple-influxdb-extractor"
        assert loaded.tasks[0].name == "poll"
        assert loaded.tasks[0].action is True
        assert loaded.active_config_revision == "local"

        assert loaded.dump(camel_case=True) == INTEGRATION_DUMPED

    def test_as_write(self) -> None:
        loaded = Integration._load(INTEGRATION_DUMPED)
        write = loaded.as_write()

        assert write.external_id == loaded.external_id
        assert write.extractor.external_id == loaded.extractor.external_id
        assert write.dump(camel_case=True) == {
            "externalId": "my-integration",
            "extractor": {"externalId": "cognite-simple-influxdb-extractor", "version": "1.0.0"},
            "name": "My integration",
            "description": "A test integration",
            "metadata": {"key": "value"},
            "allowedNotSeenMinutes": 60,
        }


class TestIntegrationUpdate:
    def test_set_and_set_null(self) -> None:
        update = IntegrationUpdate(external_id="my-integration")
        update.name.set("New name")
        update.description.set(None)

        assert update.dump() == {
            "externalId": "my-integration",
            "update": {"name": {"set": "New name"}, "description": {"setNull": True}},
        }

    def test_metadata_add_remove(self) -> None:
        update = IntegrationUpdate(external_id="my-integration")
        update.metadata.add({"key": "value"})

        assert update.dump() == {
            "externalId": "my-integration",
            "update": {"metadata": {"add": {"key": "value"}}},
        }

    def test_metadata_set(self) -> None:
        update = IntegrationUpdate(external_id="my-integration")
        update.metadata.set({"key": "value"})

        assert update.dump() == {
            "externalId": "my-integration",
            "update": {"metadata": {"set": {"key": "value"}}},
        }


class TestAction:
    def test_load_dump_round_trip(self) -> None:
        dumped = {
            "externalId": "my-action",
            "actionName": "restart",
            "status": "succeeded",
            "callMetadata": {"reason": "manual"},
            "resultMessage": "Done",
            "resultMetadata": {"durationMs": "42"},
            "createdTime": 1,
            "lastUpdatedTime": 2,
        }
        loaded = Action._load(dumped)

        assert loaded.status == "succeeded"
        assert loaded.dump(camel_case=True) == dumped

    def test_as_write(self) -> None:
        loaded = Action._load(
            {
                "externalId": "my-action",
                "actionName": "restart",
                "status": "pending",
                "createdTime": 1,
                "lastUpdatedTime": 2,
            }
        )
        write = loaded.as_write()

        assert write.dump(camel_case=True) == {"externalId": "my-action", "actionName": "restart"}


class TestSyncResult:
    def test_load_dump_round_trip(self) -> None:
        dumped = {
            "nextCursor": "abc123",
            "moreData": True,
            "history": [
                {
                    "externalId": "my-integration",
                    "taskName": "poll",
                    "startTime": 100,
                    "errorCount": 0,
                    "warningCount": 0,
                    "fatalCount": 0,
                }
            ],
            "errors": [
                {
                    "externalId": "my-integration",
                    "level": "warning",
                    "description": "Slow response",
                    "startTime": 100,
                }
            ],
        }
        loaded = SyncResult._load(dumped)

        assert loaded.next_cursor == "abc123"
        assert loaded.more_data is True
        assert isinstance(loaded.history[0], TaskHistory)
        assert isinstance(loaded.errors[0], IntegrationError)

        assert loaded.dump(camel_case=True) == dumped


class TestConfigRevision:
    def test_load_dump_round_trip(self) -> None:
        dumped = {
            "externalId": "my-integration",
            "revision": 3,
            "description": "A config revision",
            "config": "key: value",
            "createdTime": 1,
            "lastUpdatedTime": 2,
        }
        loaded = ConfigRevision._load(dumped)

        assert loaded.revision == 3
        assert loaded.dump(camel_case=True) == dumped

        write = loaded.as_write()
        assert write.dump(camel_case=True) == {
            "externalId": "my-integration",
            "config": "key: value",
            "description": "A config revision",
        }


def test_extractor_load_dump() -> None:
    dumped = {"externalId": "cognite-simple-influxdb-extractor", "version": "1.0.0"}
    assert Extractor._load(dumped).dump(camel_case=True) == dumped


def test_task_load_dump() -> None:
    dumped = {
        "type": "batch",
        "name": "sync",
        "action": True,
        "description": "Syncs data",
        "sources": ["cdf://cluster/project/service/resource"],
        "targets": ["cdf://cluster/project/timeseries/my-ts"],
    }
    assert Task._load(dumped).dump(camel_case=True) == dumped
