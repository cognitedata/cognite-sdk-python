from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal, NoReturn

from cognite.client.data_classes.ai import InputDatapoint, InputTimeSeries

MAX_SERIES_PER_REQUEST = 256
MAX_SERIES_PER_COHORT = 256
MIN_HISTORY = 2
MAX_HISTORY = 4096

AlignmentProblem = Literal["too_short", "irregular", "mixed_spacing", "off_grid", "too_long", "mask_miss"]


def split_into_requests(series: Sequence[InputTimeSeries]) -> list[list[InputTimeSeries]]:
    """Pack series into requests of at most 256, never splitting a cohort across requests.

    Args:
        series (Sequence[InputTimeSeries]): All series for one call.

    Returns:
        list[list[InputTimeSeries]]: The series for each request.
    """
    groups: dict[str | int, list[InputTimeSeries]] = defaultdict(list)
    for i, s in enumerate(series):
        groups[s.cohort if s.cohort is not None else i].append(s)

    requests: list[list[InputTimeSeries]] = [[]]
    for key, group in groups.items():
        if len(group) > MAX_SERIES_PER_COHORT:
            raise ValueError(f"Cohort {key!r} has {len(group)} series; the API allows {MAX_SERIES_PER_COHORT}.")
        if len(requests[-1]) + len(group) > MAX_SERIES_PER_REQUEST:
            requests.append([])
        requests[-1].extend(group)
    return requests


@dataclass
class SourceSeries:
    """History read from a CDF source, before alignment.

    Args:
        label (str): Unique label for the series, used in error messages and the request.
        timestamps (Sequence[int]): Sorted timestamps, in milliseconds since epoch.
        values (Sequence[float | None]): One value per timestamp. None or NaN means no reading.
    """

    label: str
    timestamps: Sequence[int]
    values: Sequence[float | None]


def build_inputs(
    sources: Sequence[SourceSeries],
    step_ms: int | None,
    cohort: str | None,
    mask: Sequence[tuple[int, int]] = (),
    fill_gaps: bool = True,
    hints: Mapping[AlignmentProblem, str] | None = None,
) -> list[InputTimeSeries]:
    """Place every series on an evenly spaced grid and mark gaps and masked points as missing.

    Raises instead of resampling or truncating the data. Nothing here is specific to time series, so other CDF
    sources can reuse it.

    Args:
        sources (Sequence[SourceSeries]): One entry per series.
        step_ms (int | None): Grid step. None means each series must already sit on a regular grid; gaps are allowed.
        cohort (str | None): If set, all series share this cohort, and therefore one common grid.
        mask (Sequence[tuple[int, int]]): Inclusive `(start, end)` ranges, in ms, to hide from the model even where a
            value exists.
        fill_gaps (bool): Send grid points without data as `missing=True` (returned by impute), or as `value=None`
            (hidden from the model, not returned).
        hints (Mapping[AlignmentProblem, str] | None): Advice to append to the error message for each kind of
            problem, in terms of the caller's parameters.

    Returns:
        list[InputTimeSeries]: One input series per source, in order.
    """
    hints = hints or {}
    for s in sources:
        if len(s.timestamps) < MIN_HISTORY:
            _fail(
                f"'{s.label}' has {len(s.timestamps)} datapoints in the requested window; the model needs at least "
                f"{MIN_HISTORY}.",
                "too_short",
                hints,
            )
    steps = {s.label: step_ms if step_ms is not None else _infer_step(s, hints) for s in sources}
    if cohort is not None:
        if len(set(steps.values())) > 1:
            _fail(f"Series in a cohort must share one spacing, got {steps}.", "mixed_spacing", hints)
        start = min(s.timestamps[0] for s in sources)
        end = max(s.timestamps[-1] for s in sources)
        grids = {s.label: _grid(start, end, steps[s.label], s, hints) for s in sources}
    else:
        grids = {s.label: _grid(s.timestamps[0], s.timestamps[-1], steps[s.label], s, hints) for s in sources}

    for lo, hi in mask:
        if not any(lo <= ts <= hi for grid in grids.values() for ts in grid):
            _fail(f"Mask entry ({lo}, {hi}) doesn't cover any point on the grid.", "mask_miss", hints)

    return [InputTimeSeries(s.label, _datapoints(s, grids[s.label], mask, fill_gaps), cohort) for s in sources]


def _fail(message: str, problem: AlignmentProblem, hints: Mapping[AlignmentProblem, str]) -> NoReturn:
    raise ValueError(f"{message} {hints[problem]}" if problem in hints else message)


def _infer_step(source: SourceSeries, hints: Mapping[AlignmentProblem, str]) -> int:
    # Gaps are allowed (they become missing points), but every point must sit on one regular grid.
    diffs = [b - a for a, b in zip(source.timestamps, source.timestamps[1:])]
    step = min(diffs)
    if step <= 0 or any(d % step for d in diffs):
        _fail(f"'{source.label}' is not evenly spaced.", "irregular", hints)
    return step


def _grid(start: int, end: int, step: int, source: SourceSeries, hints: Mapping[AlignmentProblem, str]) -> range:
    if off_grid := sum(1 for ts in source.timestamps if (ts - start) % step):
        _fail(f"'{source.label}' has {off_grid} timestamps off the common grid.", "off_grid", hints)
    grid = range(start, end + 1, step)
    if not MIN_HISTORY <= len(grid) <= MAX_HISTORY:
        _fail(
            f"'{source.label}' spans {len(grid)} grid points; the model accepts {MIN_HISTORY} to {MAX_HISTORY}.",
            "too_long" if len(grid) > MAX_HISTORY else "too_short",
            hints,
        )
    return grid


def _datapoints(
    source: SourceSeries, grid: range, mask: Sequence[tuple[int, int]], fill_gaps: bool
) -> list[InputDatapoint]:
    value_by_ts = dict(zip(source.timestamps, source.values))
    points = []
    for ts in grid:
        value = value_by_ts.get(ts)
        masked = any(lo <= ts <= hi for lo, hi in mask)
        if value is None or math.isnan(value):
            points.append(InputDatapoint(ts, None, missing=fill_gaps or masked))
        else:
            points.append(InputDatapoint(ts, value, missing=masked))
    return points
