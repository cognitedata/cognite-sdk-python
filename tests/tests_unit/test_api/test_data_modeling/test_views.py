import pytest

from cognite.client.data_classes.data_modeling import View


@pytest.fixture
def view1():
    return View(space="mySpace", external_id="myView", version="v1", created_time=1, properties={}, last_updated_time=9)


@pytest.fixture
def view2():
    return View(space="mySpace", external_id="myView", version="v2", created_time=2, properties={}, last_updated_time=9)


@pytest.fixture
def view3():
    return View(
        space="mySpace", external_id="myOtherView", version="v1", created_time=1, properties={}, last_updated_time=9
    )


@pytest.fixture
def view4():
    return View(
        space="mySpace", external_id="myOtherView", version="v2", created_time=3, properties={}, last_updated_time=9
    )


@pytest.fixture
def view5():
    return View(
        space="myOtherSpace", external_id="myView", version="v1", created_time=1, properties={}, last_updated_time=9
    )


@pytest.fixture
def view6():
    return View(
        space="myOtherSpace", external_id="myView", version="v2", created_time=2, properties={}, last_updated_time=9
    )


class TestViewsRetrieveLatest:
    def test_view_nr_same_view(self, cognite_client, view1, view2):
        views = [view1, view2]

        result = cognite_client.data_modeling.views._get_latest_views(views)
        assert len(result) == 1

    def test_view_nr_different_views(self, cognite_client, view1, view2, view3, view4):
        views = [view1, view2, view3, view4]

        result = cognite_client.data_modeling.views._get_latest_views(views)
        assert len(result) == 2

    def test_view_nr_different_spaces_views(self, cognite_client, view1, view2, view3, view4, view5, view6):
        views = [view1, view2, view3, view4, view5, view6]

        result = cognite_client.data_modeling.views._get_latest_views(views)
        assert len(result) == 3

    def test_view_get_latest(self, cognite_client, view1, view2):
        views = [view1, view2]

        result = cognite_client.data_modeling.views._get_latest_views(views)
        assert result[0].version == "v2"
