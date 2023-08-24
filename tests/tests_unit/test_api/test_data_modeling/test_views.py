import pytest

from cognite.client.data_classes.data_modeling import View


class TestViewsRetrieveLatest:
    @pytest.fixture
    def views(self):
        return [
            View("mySpace", "myView", "v1", created_time=1, properties={}, last_updated_time=9),
            View("mySpace", "myView", "v2", created_time=2, properties={}, last_updated_time=9),
            View("mySpace", "myOtherView", "v1", created_time=1, properties={}, last_updated_time=9),
            View("mySpace", "myOtherView", "v2", created_time=3, properties={}, last_updated_time=9),
            View("myOtherSpace", "myView", "v1", created_time=1, properties={}, last_updated_time=9),
            View("myOtherSpace", "myView", "v2", created_time=2, properties={}, last_updated_time=9),
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
