from __future__ import annotations

from cognite.client.utils.useful_types import SequenceNotStr, is_sequence_not_str

RESERVED_EXTERNAL_IDS = frozenset(
    {
        "Query",
        "Mutation",
        "Subscription",
        "String",
        "Int32",
        "Int64",
        "Int",
        "Float32",
        "Float64",
        "Float",
        "Timestamp",
        "JSONObject",
        "Date",
        "Numeric",
        "Boolean",
        "PageInfo",
        "File",
        "Sequence",
        "TimeSeries",
    }
)
RESERVED_SPACE_IDS = frozenset({"space", "cdf", "dms", "pg3", "shared", "system", "node", "edge"})

RESERVED_PROPERTIES = frozenset(
    {
        "space",
        "externalId",
        "createdTime",
        "lastUpdatedTime",
        "deletedTime",
        "edge_id",
        "node_id",
        "project_id",
        "property_group",
        "seq",
        "tg_table_name",
        "extensions",
    }
)


PROPERTY_PATH_HINT = (
    "A property is addressed by its path: [space, container_external_id, property_id], e.g. "
    '["my_space", "my_container", "temperature"]. Endpoints that allow top level properties take '
    'them as a single segment, e.g. ["lastUpdatedTime"].'
)


def validate_data_modeling_identifier(space: str | None, external_id: str | None = None) -> None:
    if space and space in RESERVED_SPACE_IDS:
        raise ValueError(f"The space ID: {space!r} is reserved. Please use another ID.")
    if external_id and external_id in RESERVED_EXTERNAL_IDS:
        raise ValueError(f"The external ID: {external_id!r} is reserved. Please use another ID.")


def validate_property_path(
    property: SequenceNotStr[str], argument: str = "property", hint: str = PROPERTY_PATH_HINT
) -> list[str]:
    """Validate a property path and return it as a list of segments.

    A bare string is a sequence of characters, so passing one where a sequence of strings is
    expected silently produces one segment per character instead of failing; reject it here.

    Args:
        property (SequenceNotStr[str]): The user-provided property path.
        argument (str): Name of the argument, used in the error message.
        hint (str): Actionable follow-up appended to the error message. Defaults to describing a
            fully qualified property path, which is what most arguments taking one expect.

    Returns:
        list[str]: The validated path as a list.
    """
    if not is_sequence_not_str(property):
        got = f"the string {property!r}" if isinstance(property, str) else type(property).__name__
        raise TypeError(f"{argument!r} must be a sequence of strings, not {got}. {hint}")
    return list(property)
