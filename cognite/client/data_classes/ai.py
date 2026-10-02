from __future__ import annotations

from collections.abc import Hashable, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, TypeVar

from typing_extensions import Self

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList, IdTransformerMixin
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.utils._identifier import InstanceId
from cognite.client.utils._importing import local_import
from cognite.client.utils._pandas_helpers import to_pandas_datetime_index
from cognite.client.utils._time import timestamp_to_ms

if TYPE_CHECKING:
    import pandas as pd

_ColumnKey = TypeVar("_ColumnKey", bound=Hashable)


class AnswerLanguage(Enum):
    Chinese = "Chinese"
    Dutch = "Dutch"
    English = "English"
    French = "French"
    German = "German"
    Italian = "Italian"
    Japanese = "Japanese"
    Korean = "Korean"
    Latvian = "Latvian"
    Norwegian = "Norwegian"
    Portuguese = "Portuguese"
    Spanish = "Spanish"
    Swedish = "Swedish"


@dataclass
class Summary:
    """
    A summary object consisting of a textual summary plus the id of the summarized document

    Args:
        summary (str): The textual summary of the document
        id (int | None): The id of the document
        external_id (str | None): The external id of the document
        instance_id (NodeId | None): The instance id of the document
    """

    summary: str
    id: int | None = None
    external_id: str | None = None
    instance_id: NodeId | None = None

    @classmethod
    def _load(cls, data: dict[str, Any]) -> Self:
        return cls(
            summary=data["summary"],
            id=data.get("id"),
            external_id=data.get("externalId"),
            instance_id=NodeId._load_if(data.get("instanceId")),
        )


@dataclass
class AnswerLocation:
    """
    A location object

    The location object consists of a page number and a bounding box. This
    specifies exactly where inside a document an answer can be found.

    Args:
        page_number (int): Page number, starting with 1
        left (float): Leftmost edge of the bounding box
        right (float): Rightmost edge of the bounding box
        top (float): Topmost edge of the bounding box
        bottom (float): Bottommost edge of the bounding box
    """

    page_number: int
    left: float
    right: float
    top: float
    bottom: float

    @classmethod
    def _load(cls, data: dict[str, Any]) -> Self:
        return cls(
            page_number=data["pageNumber"],
            left=data["left"],
            right=data["right"],
            top=data["top"],
            bottom=data["bottom"],
        )

    def __hash__(self) -> int:
        # Hashing floats, what can go wrong? :)
        return hash((self.page_number, self.left, self.right, self.top, self.bottom))


@dataclass
class AnswerReference:
    """
    A single reference object.

    The reference object specifies which file an answer is based on, and
    has a list of locations pointing to the specific pages and bounding boxes
    where the answer was found.

    Args:
        file_id (int): The internal id of the document
        external_id (str | None): The external id of the document
        instance_id (NodeId | None): The instance id of the document
        file_name (str): The name of the document
        locations (list[AnswerLocation]): A list of locations within the document, where the answer was found
    """

    file_id: int
    external_id: str | None
    instance_id: NodeId | None
    file_name: str
    locations: list[AnswerLocation]

    @classmethod
    def _load(cls, data: dict[str, Any]) -> Self:
        return cls(
            file_id=data["fileId"],
            external_id=data.get("externalId"),
            instance_id=NodeId._load_if(data.get("instanceId")),
            file_name=data["fileName"],
            locations=[AnswerLocation._load(d) for d in data.get("locations", [])],
        )

    def __hash__(self) -> int:
        return hash((self.file_id, self.external_id, self.instance_id, self.file_name, tuple(self.locations)))


@dataclass
class AnswerContent:
    """
    A single content object.

    It consists of part of the answer from the LLM along with references to
    the documents containing the source material for the answer.

    Args:
        text (str): The extracted plain text
        references (list[AnswerReference]): The list of references.
    """

    text: str
    references: list[AnswerReference]

    @classmethod
    def _load(cls, data: dict[str, Any]) -> Self:
        return cls(
            text=data["text"],
            references=[AnswerReference._load(ref) for ref in data.get("references", [])],
        )


@dataclass
class Answer:
    """
    An answer returned from the Document Question Answering API.

    The answer is essentially a list of content objects. Each content object
    consists of a chunk of text along with a set of references.

    Each reference contains information about which document this part
    of the answer was found in, and gives the bounding box and page number
    of the piece of text the answer was constructed from.

    Args:
        content (list[AnswerContent]): The list of content objects.
    """

    content: list[AnswerContent]

    def __str__(self) -> str:
        return f"Answer({self.full_answer!r})"

    def _repr_html_(self) -> str:
        return str(self)

    @property
    def full_answer(self) -> str:
        """
        Get the full answer text. This is the concatenation of the texts from
        all the content objects.
        """
        return "".join(cnt.text for cnt in self.content)

    @property
    def all_references(self) -> set[AnswerReference]:
        """
        Get all unique references. This is the full set of references from
        all the content objects.
        """
        return set().union(*(cnt.references for cnt in self.content))

    @classmethod
    def _load(cls, data: list[dict[str, Any]]) -> Self:
        return cls(content=[AnswerContent._load(cnt) for cnt in data])


@dataclass
class InputDatapoint:
    """One history point sent to the forecasting model.

    Args:
        timestamp (int | float | str | datetime): Milliseconds since epoch, a datetime, or a time-shift string like "2d-ago".
        value (float | None): The observed value, or None when there is no reading.
        missing (bool): Hide this point from the model. For impute, the missing points are the ones reconstructed.
    """

    timestamp: int | float | str | datetime
    value: float | None
    missing: bool = False

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {"timestamp": timestamp_to_ms(self.timestamp), "value": self.value}
        if self.missing:
            output["missing"] = True
        return output


@dataclass
class InputTimeSeries:
    """One series sent to the forecast or impute endpoint.

    Args:
        label (str): Caller-chosen identifier, echoed back in the result.
        datapoints (Sequence[InputDatapoint | tuple[int | float | str | datetime, float | None] | dict[str, Any]]):
            Evenly spaced history. Tuples are `(timestamp, value)`; dicts use the API keys `timestamp`, `value`
            and `missing`.
        cohort (str | None): Series sharing a cohort are forecast jointly and must have identical timestamps.
            Omit it to forecast the series independently.
    """

    label: str
    datapoints: Sequence[InputDatapoint | tuple[int | float | str | datetime, float | None] | dict[str, Any]]
    cohort: str | None = None

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {
            "label": self.label,
            "datapoints": [_to_input_datapoint(dp).dump(camel_case) for dp in self.datapoints],
        }
        if self.cohort is not None:
            output["cohort"] = self.cohort
        return output


def _to_input_datapoint(
    dp: InputDatapoint | tuple[int | float | str | datetime, float | None] | dict[str, Any],
) -> InputDatapoint:
    if isinstance(dp, InputDatapoint):
        return dp
    if isinstance(dp, tuple):
        return InputDatapoint(*dp)
    return InputDatapoint(dp["timestamp"], dp.get("value"), dp.get("missing", False))


def _is_number(value: str) -> bool:
    try:
        float(value)
    except ValueError:
        return False
    return True


class QuantileDatapoint(CogniteResource):
    """One forecasted or reconstructed step.

    Args:
        timestamp (int): Milliseconds since epoch.
        quantiles (dict[float, float]): Quantile level -> value.
    """

    def __init__(self, timestamp: int, quantiles: dict[float, float]) -> None:
        self.timestamp = timestamp
        self.quantiles = quantiles

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        # Only numeric keys are quantile levels; ignore anything else, like unknown fields elsewhere in the SDK.
        quantiles = {float(level): value for level, value in resource["quantiles"].items() if _is_number(level)}
        return cls(timestamp=resource["timestamp"], quantiles=quantiles)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"timestamp": self.timestamp, "quantiles": {str(level): v for level, v in self.quantiles.items()}}


def _points_to_pandas(
    points: Sequence[QuantileDatapoint], quantile_levels: Sequence[float] | None = None
) -> pd.DataFrame:
    pd = local_import("pandas")
    index = to_pandas_datetime_index([p.timestamp for p in points], timezone=None)
    df = pd.DataFrame([p.quantiles for p in points], index=index)
    return df if quantile_levels is None else df.reindex(columns=list(quantile_levels))


def _concat_columns(frames: dict[_ColumnKey, pd.DataFrame]) -> pd.DataFrame:
    pd = local_import("pandas")
    return pd.concat(frames, axis=1)


class ForecastResult(CogniteResource):
    """The forecast for one input series.

    Args:
        label (str): The label of the input series.
        forecast (list[QuantileDatapoint]): One point per forecasted step.
        cohort (str | None): The cohort of the input series, if any.
    """

    def __init__(self, label: str, forecast: list[QuantileDatapoint], cohort: str | None = None) -> None:
        self.label = label
        self.forecast = forecast
        self.cohort = cohort

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            label=resource["label"],
            forecast=[QuantileDatapoint._load(p) for p in resource["forecast"]],
            cohort=resource.get("cohort"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {"label": self.label, "forecast": [p.dump(camel_case) for p in self.forecast]}
        if self.cohort is not None:
            output["cohort"] = self.cohort
        return output

    def to_pandas(self) -> pd.DataFrame:  # type: ignore[override]
        """Convert to a DataFrame with a DatetimeIndex and one column per quantile level.

        Returns:
            pd.DataFrame: The forecast.
        """
        return _points_to_pandas(self.forecast)


class ForecastResultList(CogniteResourceList[ForecastResult]):
    """Forecasts for several input series, with the response metadata.

    Args:
        resources (Sequence[ForecastResult]): One result per input series.
        quantile_levels (list[float] | None): The quantile levels in every result.
        cohorts (list[str] | None): The distinct cohorts in the request.
    """

    _RESOURCE = ForecastResult

    def __init__(
        self,
        resources: Sequence[ForecastResult],
        quantile_levels: list[float] | None = None,
        cohorts: list[str] | None = None,
    ) -> None:
        super().__init__(resources)
        self.quantile_levels = quantile_levels or []
        self.cohorts = cohorts or []

    @classmethod
    def _load_response(cls, response: dict[str, Any]) -> Self:
        metadata = response.get("metadata", {})
        return cls(
            [ForecastResult._load(item) for item in response["timeSeries"]],
            quantile_levels=[float(q) for q in metadata.get("quantileLevels", [])],
            cohorts=metadata.get("cohorts", []),
        )

    def get(
        self,
        id: int | None = None,
        external_id: str | None = None,
        instance_id: InstanceId | tuple[str, str] | None = None,
        *,
        label: str | None = None,
    ) -> ForecastResult | None:
        """Get the result for an input label.

        Args:
            id (int | None): Not used; input series have no CDF id.
            external_id (str | None): Not used; input series have no CDF external id.
            instance_id (InstanceId | tuple[str, str] | None): Not used; input series have no CDF instance id.
            label (str | None): The label of the input series.

        Returns:
            ForecastResult | None: The result, or None if no series has that label.
        """
        if label is None:
            return super().get(id, external_id, instance_id)
        return next((item for item in self.data if item.label == label), None)

    def to_pandas(self) -> pd.DataFrame:  # type: ignore[override]
        """Convert to a DataFrame with a DatetimeIndex and `(label, quantile)` columns.

        Returns:
            pd.DataFrame: The forecasts.
        """
        return _concat_columns({r.label: _points_to_pandas(r.forecast, self.quantile_levels) for r in self.data})


class ImputeResult(CogniteResource):
    """The reconstructed points for one input series.

    Args:
        label (str): The label of the input series.
        imputed (list[QuantileDatapoint]): One point per point marked missing. Empty if none were.
        cohort (str | None): The cohort of the input series, if any.
    """

    def __init__(self, label: str, imputed: list[QuantileDatapoint], cohort: str | None = None) -> None:
        self.label = label
        self.imputed = imputed
        self.cohort = cohort

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            label=resource["label"],
            imputed=[QuantileDatapoint._load(p) for p in resource["imputed"]],
            cohort=resource.get("cohort"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {"label": self.label, "imputed": [p.dump(camel_case) for p in self.imputed]}
        if self.cohort is not None:
            output["cohort"] = self.cohort
        return output

    def to_pandas(self) -> pd.DataFrame:  # type: ignore[override]
        """Convert to a DataFrame with a DatetimeIndex and one column per quantile level.

        Returns:
            pd.DataFrame: The reconstructed points.
        """
        return _points_to_pandas(self.imputed)


class ImputeResultList(CogniteResourceList[ImputeResult]):
    """Reconstructed points for several input series, with the response metadata.

    Args:
        resources (Sequence[ImputeResult]): One result per input series.
        quantile_levels (list[float] | None): The quantile levels in every result.
        cohorts (list[str] | None): The distinct cohorts in the request.
    """

    _RESOURCE = ImputeResult

    def __init__(
        self,
        resources: Sequence[ImputeResult],
        quantile_levels: list[float] | None = None,
        cohorts: list[str] | None = None,
    ) -> None:
        super().__init__(resources)
        self.quantile_levels = quantile_levels or []
        self.cohorts = cohorts or []

    @classmethod
    def _load_response(cls, response: dict[str, Any]) -> Self:
        metadata = response.get("metadata", {})
        return cls(
            [ImputeResult._load(item) for item in response["timeSeries"]],
            quantile_levels=[float(q) for q in metadata.get("quantileLevels", [])],
            cohorts=metadata.get("cohorts", []),
        )

    def get(
        self,
        id: int | None = None,
        external_id: str | None = None,
        instance_id: InstanceId | tuple[str, str] | None = None,
        *,
        label: str | None = None,
    ) -> ImputeResult | None:
        """Get the result for an input label.

        Args:
            id (int | None): Not used; input series have no CDF id.
            external_id (str | None): Not used; input series have no CDF external id.
            instance_id (InstanceId | tuple[str, str] | None): Not used; input series have no CDF instance id.
            label (str | None): The label of the input series.

        Returns:
            ImputeResult | None: The result, or None if no series has that label.
        """
        if label is None:
            return super().get(id, external_id, instance_id)
        return next((item for item in self.data if item.label == label), None)

    def to_pandas(self) -> pd.DataFrame:  # type: ignore[override]
        """Convert to a DataFrame with a DatetimeIndex and `(label, quantile)` columns.

        Returns:
            pd.DataFrame: The reconstructed points.
        """
        return _concat_columns({r.label: _points_to_pandas(r.imputed, self.quantile_levels) for r in self.data})


def _dump_identifiers(
    output: dict[str, Any], id: int | None, external_id: str | None, instance_id: NodeId | None, camel_case: bool
) -> dict[str, Any]:
    if id is not None:
        output["id"] = id
    if external_id is not None:
        output["externalId" if camel_case else "external_id"] = external_id
    if instance_id is not None:
        output["instanceId" if camel_case else "instance_id"] = instance_id.dump(
            camel_case, include_instance_type=False
        )
    return output


def _column_name(id: int | None, external_id: str | None, instance_id: NodeId | None) -> NodeId | str | int:
    # Same precedence as the columns of client.time_series.data.retrieve_dataframe.
    if instance_id is not None:
        return instance_id
    if external_id is not None:
        return external_id
    if id is not None:
        return id
    raise ValueError("Result has no identifier (id, external_id or instance_id)")


class TimeSeriesForecast(CogniteResource):
    """The forecast for one CDF time series.

    Args:
        forecast (list[QuantileDatapoint]): One point per forecasted step.
        id (int | None): The id of the time series.
        external_id (str | None): The external id of the time series.
        instance_id (NodeId | None): The instance id of the time series.
        cohort (str | None): The cohort the series was forecast in, if any.
    """

    def __init__(
        self,
        forecast: list[QuantileDatapoint],
        id: int | None = None,
        external_id: str | None = None,
        instance_id: NodeId | None = None,
        cohort: str | None = None,
    ) -> None:
        self.forecast = forecast
        self.id = id
        self.external_id = external_id
        self.instance_id = instance_id
        self.cohort = cohort

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            forecast=[QuantileDatapoint._load(p) for p in resource["forecast"]],
            id=resource.get("id"),
            external_id=resource.get("externalId"),
            instance_id=NodeId._load_if(resource.get("instanceId")),
            cohort=resource.get("cohort"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {"forecast": [p.dump(camel_case) for p in self.forecast]}
        if self.cohort is not None:
            output["cohort"] = self.cohort
        return _dump_identifiers(output, self.id, self.external_id, self.instance_id, camel_case)

    def to_pandas(self) -> pd.DataFrame:  # type: ignore[override]
        """Convert to a DataFrame with a DatetimeIndex and one column per quantile level.

        Returns:
            pd.DataFrame: The forecast.
        """
        return _points_to_pandas(self.forecast)


class TimeSeriesForecastList(IdTransformerMixin, CogniteResourceList[TimeSeriesForecast]):
    """Forecasts for several CDF time series.

    Args:
        resources (Sequence[TimeSeriesForecast]): One forecast per time series.
        quantile_levels (list[float] | None): The quantile levels in every forecast.
    """

    _RESOURCE = TimeSeriesForecast

    def __init__(self, resources: Sequence[TimeSeriesForecast], quantile_levels: list[float] | None = None) -> None:
        super().__init__(resources)
        self.quantile_levels = quantile_levels or []

    def to_pandas(self) -> pd.DataFrame:  # type: ignore[override]
        """Convert to a DataFrame with a DatetimeIndex and `(time series, quantile)` columns.

        Time series are named like the columns of `client.time_series.data.retrieve_dataframe`: instance id, then
        external id, then id.

        Returns:
            pd.DataFrame: The forecasts.
        """
        return _concat_columns(
            {
                _column_name(r.id, r.external_id, r.instance_id): _points_to_pandas(r.forecast, self.quantile_levels)
                for r in self.data
            }
        )


class TimeSeriesImpute(CogniteResource):
    """The reconstructed points for one CDF time series.

    Args:
        imputed (list[QuantileDatapoint]): One point per reconstructed grid point.
        id (int | None): The id of the time series.
        external_id (str | None): The external id of the time series.
        instance_id (NodeId | None): The instance id of the time series.
        cohort (str | None): The cohort the series was imputed in, if any.
    """

    def __init__(
        self,
        imputed: list[QuantileDatapoint],
        id: int | None = None,
        external_id: str | None = None,
        instance_id: NodeId | None = None,
        cohort: str | None = None,
    ) -> None:
        self.imputed = imputed
        self.id = id
        self.external_id = external_id
        self.instance_id = instance_id
        self.cohort = cohort

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            imputed=[QuantileDatapoint._load(p) for p in resource["imputed"]],
            id=resource.get("id"),
            external_id=resource.get("externalId"),
            instance_id=NodeId._load_if(resource.get("instanceId")),
            cohort=resource.get("cohort"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {"imputed": [p.dump(camel_case) for p in self.imputed]}
        if self.cohort is not None:
            output["cohort"] = self.cohort
        return _dump_identifiers(output, self.id, self.external_id, self.instance_id, camel_case)

    def to_pandas(self) -> pd.DataFrame:  # type: ignore[override]
        """Convert to a DataFrame with a DatetimeIndex and one column per quantile level.

        Returns:
            pd.DataFrame: The reconstructed points.
        """
        return _points_to_pandas(self.imputed)


class TimeSeriesImputeList(IdTransformerMixin, CogniteResourceList[TimeSeriesImpute]):
    """Reconstructed points for several CDF time series.

    Args:
        resources (Sequence[TimeSeriesImpute]): One result per time series.
        quantile_levels (list[float] | None): The quantile levels in every result.
    """

    _RESOURCE = TimeSeriesImpute

    def __init__(self, resources: Sequence[TimeSeriesImpute], quantile_levels: list[float] | None = None) -> None:
        super().__init__(resources)
        self.quantile_levels = quantile_levels or []

    def to_pandas(self) -> pd.DataFrame:  # type: ignore[override]
        """Convert to a DataFrame with a DatetimeIndex and `(time series, quantile)` columns.

        Time series are named like the columns of `client.time_series.data.retrieve_dataframe`: instance id, then
        external id, then id.

        Returns:
            pd.DataFrame: The reconstructed points.
        """
        return _concat_columns(
            {
                _column_name(r.id, r.external_id, r.instance_id): _points_to_pandas(r.imputed, self.quantile_levels)
                for r in self.data
            }
        )
