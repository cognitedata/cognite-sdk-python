from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar

from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.instances import PropertyLike


@dataclass
class Sourceable(PropertyLike):
    _source: ClassVar[ViewId] = ViewId("cdf_cdm_experimental", "Sourceable", "v1")
    source_id: str | None = None
    source: str | None = None
    source_created_time: datetime | None = None
    source_updated_time: datetime | None = None
    source_created_user: str | None = None
    source_updated_user: str | None = None


@dataclass
class Describable(PropertyLike):
    _source: ClassVar[ViewId] = ViewId("cdf_cdm_experimental", "Describable", "v1")
    name: str | None = None
    description: str | None = None
    tags: list[str] | None = None
    aliases: list[str] | None = None


@dataclass
class Schedulable(PropertyLike):
    _source: ClassVar[ViewId] = ViewId("cdf_cdm_experimental", "Schedulable", "v1")
    start_time: datetime | None = None
    end_time: datetime | None = None
    scheduled_start_time: datetime | None = None
    scheduled_end_time: datetime | None = None


@dataclass
class Connection3D(PropertyLike):
    _source: ClassVar[ViewId] = ViewId("cdf_cdm_experimental", "Connection3D", "v1")
    revision_id: int
    revision_node_id: int


@dataclass
class Model3D(Sourceable, Describable):
    _source = ViewId("cdf_cdm_experimental", "Model3D", "v1")


@dataclass
class Object3D(PropertyLike):
    _source = ViewId("cdf_cdm_experimental", "Object3D", "v1")
    source_id: str | None = None
    source: str | None = None
    source_created_time: datetime | None = None
    source_updated_time: datetime | None = None
    source_created_user: str | None = None
    source_updated_user: str | None = None
    name: str | None = None
    description: str | None = None
    tags: list[str] | None = None
    aliases: list[str] | None = None


@dataclass
class AssetType(Describable):
    _source = ViewId("cdf_cdm_experimental", "Assettype", "v1")
    code: str | None = None


@dataclass
class Asset(Object3D):
    _source = ViewId("cdf_cdm_experimental", "Asset", "v1")
    children: list[Asset] | None = None
    parent: Asset | None = None
    path: Asset | None = None
    last_path_materialization_time: datetime | None = None
    equipment: Equipment | None = None
    type: AssetType | None = None
    root: Asset | None = None


@dataclass
class Equipment(Object3D):
    _source = ViewId("cdf_cdm_experimental", "Equipment", "v1")
    asset: Asset | None = None
    serial_number: str | None = None
    manufacturer: str | None = None


@dataclass
class Activity(Describable, Sourceable, Schedulable):
    _source = ViewId("cdf_cdm_experimental", "Activity", "v1")
    assets: list[Asset] | None = None


@dataclass
class TimeSeriesBase(PropertyLike):
    _source = ViewId("cdf_cdm_experimental", "TimeSeriesBase", "v1")
    is_step: bool
    is_string: bool
    name: str | None = None
    description: str | None = None
    tags: list[str] | None = None
    aliases: list[str] | None = None
    source_id: str | None = None
    source: str | None = None
    source_created_time: datetime | None = None
    source_updated_time: datetime | None = None
    source_created_user: str | None = None
    source_updated_user: str | None = None
    source_unit: str | None = None
    unit: str | None = None
    assets: list[Asset] | None = None
    equipment: list[Equipment] | None = None
