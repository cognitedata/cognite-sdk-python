import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import Space
from cognite.client.data_classes.data_modeling.statistics import ProjectStatistics, SpaceStatistics, SpaceStatisticsList


class TestStatistics:
    def test_project(self, cognite_client: CogniteClient) -> None:
        stats = cognite_client.data_modeling.statistics.project()
        assert isinstance(stats, ProjectStatistics)
        assert stats.data_models.count >= 0
        assert stats.data_models.limit >= stats.data_models.count

    def test_retrieve(self, cognite_client: CogniteClient, integration_test_space: Space) -> None:
        result = cognite_client.data_modeling.statistics.spaces.retrieve(integration_test_space.space)

        assert isinstance(result, SpaceStatistics)
        assert result.space == integration_test_space.space

    def test_retrieve_non_existent_space(self, cognite_client: CogniteClient) -> None:
        result = cognite_client.data_modeling.statistics.spaces.retrieve("non-existent-space")

        assert result is None

    def test_retrieve_multiple_non_existent_spaces(
        self, cognite_client: CogniteClient, integration_test_space: Space
    ) -> None:
        spaces = [integration_test_space.space, "non-existent-space", "another-non-existent-space"]
        result = cognite_client.data_modeling.statistics.spaces.retrieve(spaces)

        assert isinstance(result, SpaceStatisticsList)
        assert len(result) == 1
        assert result[0].space == integration_test_space.space

    def test_retrieve_multiple(self, cognite_client: CogniteClient) -> None:
        spaces = cognite_client.data_modeling.spaces.list(limit=2)
        assert len(spaces) >= 1, "At least two space should exist for this test."
        result = cognite_client.data_modeling.statistics.spaces.retrieve(spaces.as_ids())

        assert isinstance(result, SpaceStatisticsList)
        retrieved_spaces = {item.space for item in result}
        assert retrieved_spaces == set(spaces.as_ids())

    @pytest.mark.usefixtures("integration_test_space")
    def test_list(self, cognite_client: CogniteClient) -> None:
        result = cognite_client.data_modeling.statistics.spaces.list()
        assert isinstance(result, SpaceStatisticsList)
        assert len(result) > 0, "There should be at least one space with statistics."
