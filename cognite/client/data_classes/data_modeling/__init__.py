from __future__ import annotations

from cognite.client.data_classes import aggregations, filters
from cognite.client.data_classes.aggregations import AggregatedValue, Aggregation
from cognite.client.data_classes.data_modeling import query
from cognite.client.data_classes.data_modeling.containers import (
    Constraint,
    Container,
    ContainerApply,
    ContainerApplyList,
    ContainerFilter,
    ContainerList,
    ContainerProperty,
    Index,
    RequiresConstraint,
    UniquenessConstraint,
)
from cognite.client.data_classes.data_modeling.data_models import (
    DataModel,
    DataModelApply,
    DataModelApplyList,
    DataModelFilter,
    DataModelList,
    DataModelsSort,
)
from cognite.client.data_classes.data_modeling.data_types import (
    Boolean,
    CDFExternalIdReference,
    Date,
    DirectRelation,
    DirectRelationReference,
    FileReference,
    Float32,
    Float64,
    Int32,
    Int64,
    Json,
    PropertyType,
    SequenceReference,
    Text,
    TimeSeriesReference,
    Timestamp,
)
from cognite.client.data_classes.data_modeling.ids import (
    ContainerId,
    ContainerIdentifier,
    DataModelId,
    DataModelIdentifier,
    DataModelingId,
    EdgeId,
    NodeId,
    PropertyId,
    VersionedDataModelingId,
    ViewId,
    ViewIdentifier,
)
from cognite.client.data_classes.data_modeling.instances import (
    Edge,
    EdgeApply,
    EdgeApplyList,
    EdgeApplyResult,
    EdgeApplyResultList,
    EdgeList,
    EdgeListWithCursor,
    InstanceApply,
    InstancesApply,
    InstancesApplyResult,
    InstancesDeleteResult,
    InstanceSort,
    InstancesResult,
    Node,
    NodeApply,
    NodeApplyList,
    NodeApplyResult,
    NodeApplyResultList,
    NodeList,
    NodeListWithCursor,
    NodeOrEdgeData,
    PropertyOptions,
    TypedEdge,
    TypedEdgeApply,
    TypedNode,
    TypedNodeApply,
)
from cognite.client.data_classes.data_modeling.spaces import Space, SpaceApply, SpaceApplyList, SpaceList
from cognite.client.data_classes.data_modeling.views import (
    ConnectionDefinition,
    EdgeConnection,
    EdgeConnectionApply,
    MappedProperty,
    MappedPropertyApply,
    MultiEdgeConnection,
    MultiEdgeConnectionApply,
    MultiReverseDirectRelation,
    MultiReverseDirectRelationApply,
    SingleHopConnectionDefinition,
    View,
    ViewApply,
    ViewApplyList,
    ViewFilter,
    ViewList,
)
from cognite.client.data_classes.filters import Filter

__all__ = [
    "Aggregation",
    "AggregatedValue",
    "aggregations",
    "ViewIdentifier",
    "ViewApply",
    "ViewApplyList",
    "PropertyId",
    "MappedPropertyApply",
    "VersionedDataModelingId",
    "DataModelingId",
    "ContainerIdentifier",
    "DataModelIdentifier",
    "DirectRelation",
    "Filter",
    "filters",
    "DirectRelationReference",
    "DataModel",
    "DataModelList",
    "DataModelsSort",
    "DataModelFilter",
    "DataModelApply",
    "DataModelApplyList",
    "ContainerFilter",
    "ViewFilter",
    "MappedProperty",
    "ConnectionDefinition",
    "SingleHopConnectionDefinition",
    "EdgeConnection",
    "EdgeConnectionApply",
    "MultiEdgeConnection",
    "MultiEdgeConnectionApply",
    "MultiReverseDirectRelation",
    "MultiReverseDirectRelationApply",
    "Space",
    "SpaceList",
    "SpaceApply",
    "SpaceApplyList",
    "View",
    "ViewList",
    "Container",
    "ContainerList",
    "ContainerApply",
    "ContainerApplyList",
    "Index",
    "Constraint",
    "ContainerProperty",
    "CDFExternalIdReference",
    "RequiresConstraint",
    "ContainerId",
    "UniquenessConstraint",
    "ViewId",
    "DataModelId",
    "Text",
    "Boolean",
    "Float32",
    "Float64",
    "Int32",
    "Int64",
    "Timestamp",
    "Date",
    "Json",
    "TimeSeriesReference",
    "FileReference",
    "SequenceReference",
    "PropertyType",
    "Node",
    "NodeList",
    "NodeListWithCursor",
    "NodeApply",
    "NodeApplyResult",
    "NodeApplyResultList",
    "NodeApplyList",
    "Edge",
    "EdgeList",
    "EdgeListWithCursor",
    "EdgeApply",
    "EdgeApplyResult",
    "EdgeApplyResultList",
    "EdgeApplyList",
    "InstanceSort",
    "NodeOrEdgeData",
    "NodeId",
    "EdgeId",
    "InstancesApplyResult",
    "InstancesApply",
    "InstancesDeleteResult",
    "InstancesResult",
    "InstanceApply",
    "query",
    "PropertyOptions",
    "TypedEdgeApply",
    "TypedNodeApply",
    "TypedNode",
    "TypedEdge",
]
