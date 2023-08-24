import pytest

from cognite.client._api.data_modeling.instances import InstancesAPI
from cognite.client.data_classes.data_modeling import View
from cognite.client.data_classes.data_modeling.ids import ViewId

SINGLE_SRC_DUMP = {"source": {"space": "a", "externalId": "b", "version": "c", "type": "view"}}
SINGLE_SRC_DUMP_NO_VERSION = {"source": {"space": "a", "externalId": "b", "version": None, "type": "view"}}


@pytest.mark.parametrize(
    "sources, expected",
    (
        # Single
        (("a", "b", "c"), [SINGLE_SRC_DUMP]),
        (("a", "b"), [SINGLE_SRC_DUMP_NO_VERSION]),
        (ViewId("a", "b", "c"), [SINGLE_SRC_DUMP]),
        (ViewId("a", "b"), [SINGLE_SRC_DUMP_NO_VERSION]),
        (View("a", "b", "c", created_time=1, properties={}, last_updated_time=2), [SINGLE_SRC_DUMP]),
        # Multiple
        ((("a", "b", "c"), ("a", "b", "c")), [SINGLE_SRC_DUMP, SINGLE_SRC_DUMP]),
        ([("a", "b", "c"), ("a", "b")], [SINGLE_SRC_DUMP, SINGLE_SRC_DUMP_NO_VERSION]),
        ((ViewId("a", "b"), ("a", "b", "c")), [SINGLE_SRC_DUMP_NO_VERSION, SINGLE_SRC_DUMP]),
        ([ViewId("a", "b"), ViewId("a", "b", "c")], [SINGLE_SRC_DUMP_NO_VERSION, SINGLE_SRC_DUMP]),
        (
            [View("a", "b", None, created_time=1, properties={}, last_updated_time=2), ViewId("a", "b", "c")],
            [SINGLE_SRC_DUMP_NO_VERSION, SINGLE_SRC_DUMP],
        ),
    ),
)
def test_instances_api_dump_instance_source(sources, expected):
    # We need to support:
    # ViewIdentifier = Union[ViewId, Tuple[str, str], Tuple[str, str, str]]
    # ViewIdentifier | Sequence[ViewIdentifier] | View | Sequence[View]
    assert expected == InstancesAPI._dump_instance_source(sources)
