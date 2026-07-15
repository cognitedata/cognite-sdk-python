from __future__ import annotations

import re
from urllib.parse import parse_qs, urlparse

import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.data_modeling import (
    ContainerId,
    MappedPropertyApply,
    RecordViewApply,
    View,
    ViewApply,
    ViewList,
)
from cognite.client.exceptions import CogniteAPIError
from tests.tests_unit.test_api.test_data_modeling.conftest import make_test_view
from tests.utils import get_url, jsgz_load


class TestViewsRetrieveLatest:
    @pytest.fixture
    def views(self) -> ViewList:
        return ViewList(
            [
                make_test_view("mySpace", "myView", "v1", created_time=1),
                make_test_view("mySpace", "myView", "v2", created_time=2),
                make_test_view("mySpace", "myOtherView", "v1", created_time=1),
                make_test_view("mySpace", "myOtherView", "v2", created_time=3),
                make_test_view("myOtherSpace", "myView", "v1", created_time=1),
                make_test_view("myOtherSpace", "myView", "v2", created_time=2),
            ]
        )

    def test_different_versions(self, async_client: AsyncCogniteClient, views: ViewList) -> None:
        views = ViewList([views[0], views[1]])
        result = async_client.data_modeling.views._get_latest_views(views)
        assert result == ViewList([views[1]])

    def test_different_external_ids(self, async_client: AsyncCogniteClient, views: ViewList) -> None:
        views = ViewList([views[0], views[1], views[2], views[3]])

        result = async_client.data_modeling.views._get_latest_views(views)
        assert result == ViewList([views[1], views[3]])

    def test_different_spaces(self, async_client: AsyncCogniteClient, views: ViewList) -> None:
        result = async_client.data_modeling.views._get_latest_views(views)
        assert result == ViewList([views[1], views[3], views[5]])


VIEW_RESPONSE = {
    "space": "sp",
    "externalId": "v",
    "version": "v1",
    "createdTime": 1,
    "lastUpdatedTime": 2,
    "writable": True,
    "usedFor": "all",
    "isGlobal": False,
    "properties": {},
}

RECORD_VIEW_RESPONSE = {
    "space": "sp",
    "externalId": "rv",
    "version": "v1",
    "streamId": ["my-stream"],
    "createdTime": 1,
    "lastUpdatedTime": 2,
    "writable": True,
    "usedFor": "record",
    "isGlobal": False,
    "properties": {},
}


def make_record_view_apply(stream_id: str | list[str] = "my-stream") -> RecordViewApply:
    return RecordViewApply(
        space="sp",
        external_id="rv",
        version="v1",
        stream_id=stream_id,
        properties={
            "title": MappedPropertyApply(
                container=ContainerId("sp", "recordContainer"), container_property_identifier="title"
            )
        },
    )


class TestViewsApiForRecordViews:
    @pytest.fixture
    def views_url_pattern(self, async_client: AsyncCogniteClient) -> re.Pattern:
        return re.compile("^" + re.escape(get_url(async_client.data_modeling.views, "/models/views")))

    def test_apply_single_record_view(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        views_url_pattern: re.Pattern,
    ) -> None:
        record_view = make_record_view_apply()
        httpx_mock.add_response(
            method="POST", url=views_url_pattern, status_code=200, json={"items": [RECORD_VIEW_RESPONSE]}
        )

        result = cognite_client.data_modeling.views.apply(record_view)

        assert isinstance(result, View)
        assert not isinstance(result, ViewList)

    def test_apply_record_view_failure(
        self,
        cognite_client: CogniteClient,
        httpx_mock: HTTPXMock,
        views_url_pattern: re.Pattern,
    ) -> None:
        real_error_message = (
            "Cannot update view 'sp:rv/v1', Referenced container does not exist: 'sp:recordContainer/v1'."
        )
        record_view = make_record_view_apply()
        httpx_mock.add_response(
            method="POST", url=views_url_pattern, status_code=400, json={"error": {"message": real_error_message}}
        )

        with pytest.raises(CogniteAPIError) as error:
            cognite_client.data_modeling.views.apply(record_view)

        assert error.value.message == real_error_message
        assert error.value.failed == [record_view]
        assert error.value.code == 400

    def test_apply_mixed_batch_warns_and_sends_alpha_header(
        self,
        cognite_client: CogniteClient,
        async_client: AsyncCogniteClient,
        httpx_mock: HTTPXMock,
        views_url_pattern: re.Pattern,
    ) -> None:
        plain_view = ViewApply(space="sp", external_id="v", version="v1")
        record_view = make_record_view_apply()
        httpx_mock.add_response(
            method="POST",
            url=views_url_pattern,
            status_code=200,
            json={"items": [VIEW_RESPONSE, RECORD_VIEW_RESPONSE]},
        )

        with pytest.warns(FutureWarning, match="Views on Records"):
            cognite_client.data_modeling.views.apply([plain_view, record_view])

        request = httpx_mock.get_requests()[0]
        assert request.headers["cdf-version"] == f"{async_client.config.api_subversion}-alpha"
        body = jsgz_load(request.content)
        assert body["items"][1]["streamId"] == ["my-stream"]

    def test_apply_plain_views_does_not_warn_or_send_alpha_header(
        self,
        cognite_client: CogniteClient,
        async_client: AsyncCogniteClient,
        httpx_mock: HTTPXMock,
        views_url_pattern: re.Pattern,
        recwarn: pytest.WarningsRecorder,
    ) -> None:
        plain_view = ViewApply(space="sp", external_id="v", version="v1")
        httpx_mock.add_response(method="POST", url=views_url_pattern, status_code=200, json={"items": [VIEW_RESPONSE]})

        cognite_client.data_modeling.views.apply(plain_view)

        assert not any("Views on Records" in str(w.message) for w in recwarn.list)
        request = httpx_mock.get_requests()[0]
        assert request.headers["cdf-version"] == async_client.config.api_subversion

    def test_list_used_for_mixed_warns_and_sends_alpha_header(
        self,
        cognite_client: CogniteClient,
        async_client: AsyncCogniteClient,
        httpx_mock: HTTPXMock,
        views_url_pattern: re.Pattern,
    ) -> None:
        httpx_mock.add_response(method="GET", url=views_url_pattern, status_code=200, json={"items": []})

        with pytest.warns(FutureWarning, match="Views on Records"):
            cognite_client.data_modeling.views.list(used_for=["node", "record"])

        request = httpx_mock.get_requests()[0]
        assert request.headers["cdf-version"] == f"{async_client.config.api_subversion}-alpha"
        qs = parse_qs(urlparse(str(request.url)).query)
        assert qs.get("usedFor") == ["node", "record"]

    def test_list_default_does_not_warn_or_send_alpha_header(
        self,
        cognite_client: CogniteClient,
        async_client: AsyncCogniteClient,
        httpx_mock: HTTPXMock,
        views_url_pattern: re.Pattern,
        recwarn: pytest.WarningsRecorder,
    ) -> None:
        httpx_mock.add_response(method="GET", url=views_url_pattern, status_code=200, json={"items": []})

        cognite_client.data_modeling.views.list()

        assert not any("Views on Records" in str(w.message) for w in recwarn.list)
        request = httpx_mock.get_requests()[0]
        assert request.headers["cdf-version"] == async_client.config.api_subversion
        qs = parse_qs(urlparse(str(request.url)).query)
        assert "usedFor" not in qs
