import pytest

from tests.tests_unit.test_api.test_data_modeling.conftest import make_test_view


class TestViewsRetrieveLatest:
    @pytest.fixture
    def views(self):
        return [
            make_test_view("mySpace", "myView", "v1", created_time=1),
            make_test_view("mySpace", "myView", "v2", created_time=2),
            make_test_view("mySpace", "myOtherView", "v1", created_time=1),
            make_test_view("mySpace", "myOtherView", "v2", created_time=3),
            make_test_view("myOtherSpace", "myView", "v1", created_time=1),
            make_test_view("myOtherSpace", "myView", "v2", created_time=2),
        ]

    def test_different_versions(self, cognite_client, views):
        views = (views[0], views[1])
        result = cognite_client.data_modeling.views._get_latest_views(views)
        assert result == [views[1]]

    def test_different_external_ids(self, cognite_client, views):
        views = [views[0], views[1], views[2], views[3]]

        result = cognite_client.data_modeling.views._get_latest_views(views)
        assert result == [views[1], views[3]]

    def test_different_spaces(self, cognite_client, views):
        result = cognite_client.data_modeling.views._get_latest_views(views)
        assert result == [views[1], views[3], views[5]]
