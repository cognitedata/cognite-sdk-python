from __future__ import annotations

import pytest

from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.query import SourceSelector
from tests.tests_unit.test_api.test_data_modeling.conftest import make_test_view

SINGLE_SRC_DUMP = {"source": {"space": "a", "externalId": "b", "version": "c", "type": "view"}}
SINGLE_SRC_DUMP_NO_VERSION = {"source": {"space": "a", "externalId": "b", "version": None, "type": "view"}}


class TestSourceDef:
    @pytest.mark.parametrize(
        "sources, expected",
        (
            # Single
            (("a", "b", "c"), [SINGLE_SRC_DUMP]),
            (("a", "b"), [SINGLE_SRC_DUMP_NO_VERSION]),
            (ViewId("a", "b", "c"), [SINGLE_SRC_DUMP]),
            (ViewId("a", "b"), [SINGLE_SRC_DUMP_NO_VERSION]),
            (make_test_view("a", "b", "c"), [SINGLE_SRC_DUMP]),
            # Multiple
            ((("a", "b", "c"), ("a", "b", "c")), [SINGLE_SRC_DUMP, SINGLE_SRC_DUMP]),
            ([("a", "b", "c"), ("a", "b")], [SINGLE_SRC_DUMP, SINGLE_SRC_DUMP_NO_VERSION]),
            ((ViewId("a", "b"), ("a", "b", "c")), [SINGLE_SRC_DUMP_NO_VERSION, SINGLE_SRC_DUMP]),
            ([ViewId("a", "b"), ViewId("a", "b", "c")], [SINGLE_SRC_DUMP_NO_VERSION, SINGLE_SRC_DUMP]),
            (
                [make_test_view("a", "b", None), ViewId("a", "b", "c")],
                [SINGLE_SRC_DUMP_NO_VERSION, SINGLE_SRC_DUMP],
            ),
        ),
    )
    def test_instances_api_dump_instance_source(self, sources, expected):
        # We need to support:
        # ViewIdentifier = Union[ViewId, Tuple[str, str], Tuple[str, str, str]]
        # ViewIdentifier | Sequence[ViewIdentifier] | View | Sequence[View]
        assert expected == [source.dump() for source in SourceSelector._load_list(sources)]
