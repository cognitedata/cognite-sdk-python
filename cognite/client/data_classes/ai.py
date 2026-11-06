from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.utils._importing import local_import
from cognite.client.utils._pandas_helpers import to_pandas_datetime_index
from cognite.client.utils._time import timestamp_to_ms

if TYPE_CHECKING:
    import pandas as pd


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
class ForecastInputDatapoint:
    """One history point sent to the forecast or impute endpoint.

    The datapoints of a series must be evenly spaced. To represent a gap in the history, such as a sensor outage,
    keep the timestamp and set the value to None or mark the datapoint as missing.

    A datapoint with value None has no reading. The model does not see it, and impute does not return it.

    A datapoint marked missing is hidden from the model, whatever its value. Impute predicts its value and returns
    it: mark a gap as missing to fill it, or mark observed datapoints as missing to compare each value with its
    prediction. For forecast, marking datapoints as missing excludes a segment of the history, for example to see
    how it affects the result.

    At least one datapoint per series must have a value and not be marked missing.

    Args:
        timestamp (int | float | str | datetime): Milliseconds since epoch, a datetime, or a time-shift string like "2d-ago".
        value (float | None): The value, or None when there is no reading.
        missing (bool): Hide the datapoint from the model, whatever its value. Impute returns a prediction for it.
    """

    timestamp: int | float | str | datetime
    value: float | None
    missing: bool = False

    @classmethod
    def load(
        cls, data: ForecastInputDatapoint | tuple[Any, ...] | list[Any] | dict[str, Any]
    ) -> ForecastInputDatapoint:
        """Create a datapoint from any of the supported formats.

        Args:
            data (ForecastInputDatapoint | tuple[Any, ...] | list[Any] | dict[str, Any]): A datapoint, returned as is; a
                `(timestamp, value)` or `(timestamp, value, missing)` tuple or list; or a dict with the API keys
                `timestamp`, `value` and `missing`, where an absent `value` means None.

        Returns:
            ForecastInputDatapoint: The datapoint.

        Examples:

            Create datapoints from tuples and dicts:

                >>> from cognite.client.data_classes.ai import ForecastInputDatapoint
                >>> dps = [
                ...     ForecastInputDatapoint.load((1790812800000, 42.1)),
                ...     ForecastInputDatapoint.load({"timestamp": 1790812860000, "missing": True}),
                ... ]
        """
        if isinstance(data, ForecastInputDatapoint):
            return data
        if isinstance(data, dict):
            return cls(data["timestamp"], data.get("value"), data.get("missing", False))
        if isinstance(data, (tuple, list)):
            return cls(*data)
        raise TypeError(f"Can't load a ForecastInputDatapoint from {type(data).__name__}")

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {"timestamp": timestamp_to_ms(self.timestamp), "value": self.value}
        if self.missing:
            output["missing"] = True
        return output


@dataclass
class ForecastInputTimeSeries:
    """One series sent to the forecast or impute endpoint.

    Args:
        label (str): Caller-chosen identifier, echoed back in the result.
        datapoints (Sequence[ForecastInputDatapoint]): Evenly spaced history. Use `ForecastInputDatapoint.load` to
            create datapoints from tuples or dicts.
        cohort (str | None): A group name. Give related series, such as the sensors on one compressor, the same
            cohort to process them jointly, so the model can use the relationships between them. Series in a cohort
            must have identical timestamps: the same start, interval and number of datapoints. Leave it as None to
            process the series independently.
    """

    label: str
    datapoints: Sequence[ForecastInputDatapoint]
    cohort: str | None = None

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {
            "label": self.label,
            "datapoints": [dp.dump(camel_case) for dp in self.datapoints],
        }
        if self.cohort is not None:
            output["cohort"] = self.cohort
        return output


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
        quantiles = {float(level): value for level, value in resource["quantiles"].items()}
        return cls(timestamp=resource["timestamp"], quantiles=quantiles)

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"timestamp": self.timestamp, "quantiles": {str(level): v for level, v in self.quantiles.items()}}


def _points_to_pandas(points: Sequence[QuantileDatapoint], quantile_levels: Sequence[float] | None) -> pd.DataFrame:
    pd = local_import("pandas")
    index = to_pandas_datetime_index([p.timestamp for p in points], timezone=None)
    df = pd.DataFrame([p.quantiles for p in points], index=index)
    return df if not quantile_levels else df.reindex(columns=list(quantile_levels))


def _concat_columns(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    pd = local_import("pandas")
    if not frames:
        return pd.DataFrame(index=to_pandas_datetime_index([], timezone=None))
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

    def to_pandas(self, quantile_levels: Sequence[float] | None = (0.5,)) -> pd.DataFrame:  # type: ignore[override]
        """Convert to a DataFrame with a DatetimeIndex and one column per quantile level.

        Args:
            quantile_levels (Sequence[float] | None): The quantile levels to include. Defaults to the median, 0.5. Pass None
                to include every level in the result.

        Returns:
            pd.DataFrame: The forecast.
        """
        return _points_to_pandas(self.forecast, quantile_levels)


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
        metadata = response["metadata"]
        return cls(
            [ForecastResult._load(item) for item in response["items"]],
            quantile_levels=metadata["quantileLevels"],
            cohorts=metadata["cohorts"],
        )

    def get(self, label: str) -> ForecastResult | None:  # type: ignore[override]
        """Get the result for an input series by its label.

        Labels are unique within a result, since duplicate labels in a request are rejected.

        Args:
            label (str): The label of the input series.

        Returns:
            ForecastResult | None: The result, or None if no series has that label.
        """
        return next((item for item in self.data if item.label == label), None)

    def to_pandas(self, quantile_levels: Sequence[float] | None = (0.5,)) -> pd.DataFrame:  # type: ignore[override]
        """Convert to a DataFrame with a DatetimeIndex and `(label, quantile level)` columns.

        Args:
            quantile_levels (Sequence[float] | None): The quantile levels to include. Defaults to the median, 0.5. Pass None
                to include every level in the result.

        Returns:
            pd.DataFrame: The forecasts.
        """
        # Every series gets the same columns, even one without points.
        levels = self.quantile_levels if quantile_levels is None else quantile_levels
        return _concat_columns({r.label: _points_to_pandas(r.forecast, levels) for r in self.data})


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

    def to_pandas(self, quantile_levels: Sequence[float] | None = (0.5,)) -> pd.DataFrame:  # type: ignore[override]
        """Convert to a DataFrame with a DatetimeIndex and one column per quantile level.

        Args:
            quantile_levels (Sequence[float] | None): The quantile levels to include. Defaults to the median, 0.5. Pass None
                to include every level in the result.

        Returns:
            pd.DataFrame: The reconstructed points.
        """
        return _points_to_pandas(self.imputed, quantile_levels)


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
        metadata = response["metadata"]
        return cls(
            [ImputeResult._load(item) for item in response["items"]],
            quantile_levels=metadata["quantileLevels"],
            cohorts=metadata["cohorts"],
        )

    def get(self, label: str) -> ImputeResult | None:  # type: ignore[override]
        """Get the result for an input series by its label.

        Labels are unique within a result, since duplicate labels in a request are rejected.

        Args:
            label (str): The label of the input series.

        Returns:
            ImputeResult | None: The result, or None if no series has that label.
        """
        return next((item for item in self.data if item.label == label), None)

    def to_pandas(self, quantile_levels: Sequence[float] | None = (0.5,)) -> pd.DataFrame:  # type: ignore[override]
        """Convert to a DataFrame with a DatetimeIndex and `(label, quantile level)` columns.

        Args:
            quantile_levels (Sequence[float] | None): The quantile levels to include. Defaults to the median, 0.5. Pass None
                to include every level in the result.

        Returns:
            pd.DataFrame: The reconstructed points.
        """
        # Every series gets the same columns, even one without points.
        levels = self.quantile_levels if quantile_levels is None else quantile_levels
        return _concat_columns({r.label: _points_to_pandas(r.imputed, levels) for r in self.data})
