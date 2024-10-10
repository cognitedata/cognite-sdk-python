import yaml

from cognite.client.data_classes.hosted_extractors.jobs import Job


class TestJob:
    def test_load_yaml_dump_unknown_config(self) -> None:
        raw_yaml = """externalId: myJob
sourceId: my_eventhub
destinationId: EventHubTarget
targetStatus: running
status: running
createdTime: 123
lastUpdatedTime: 1234
format:
  type: value
  encoding: utf16
  compression: gzip
config:
  some: new_config
  that: has not been seen before"""

        assert Job.load(raw_yaml).dump() == yaml.safe_load(raw_yaml)
