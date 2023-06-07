from __future__ import annotations

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
        "TimeSerie",
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


def validate_data_modeling_identifier(space: str | None, external_id: str | None = None) -> None:
    if space and space in RESERVED_SPACE_IDS:
        raise ValueError(f"The space ID: {space} is reserved. Please use another ID.")
    if external_id and external_id in RESERVED_EXTERNAL_IDS:
        raise ValueError(f"The external ID: {external_id} is reserved. Please use another ID.")
