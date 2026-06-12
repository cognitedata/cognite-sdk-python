from __future__ import annotations

from typing import Any

import pytest

from cognite.client.data_classes.data_modeling import ViewId
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteFile
from cognite.client.utils._data_modeling import resolve_source
from tests.tests_unit.test_api.test_data_modeling.conftest import make_test_view

CANONICAL_VIEW_ID = CogniteFile.get_source()
CUSTOM_VIEW_ID = ViewId("my-space", "MyView", "v1")


class TestResolveSource:
    @pytest.mark.parametrize(
        "source, expected_sources, expected_strip",
        [
            (CANONICAL_VIEW_ID, [CANONICAL_VIEW_ID], False),
            (("cdf_cdm", "CogniteFile", "v1"), [CANONICAL_VIEW_ID], False),
            (CUSTOM_VIEW_ID, [CUSTOM_VIEW_ID, CANONICAL_VIEW_ID], True),
            (("my-space", "MyView", "v1"), [CUSTOM_VIEW_ID, CANONICAL_VIEW_ID], True),
            (make_test_view("my-space", "MyView", "v1"), [CUSTOM_VIEW_ID, CANONICAL_VIEW_ID], True),
        ],
    )
    def test_resolve_source(self, source: Any, expected_sources: list[ViewId], expected_strip: bool) -> None:
        sources, strip = resolve_source(source, CANONICAL_VIEW_ID)
        assert sources == expected_sources
        assert strip is expected_strip

    def test_invalid_source_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="Expected View, ViewId"):
            resolve_source("not-a-valid-source", CANONICAL_VIEW_ID)  # type: ignore[arg-type]
