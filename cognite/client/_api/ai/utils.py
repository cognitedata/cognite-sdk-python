from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import Any

from cognite.client._basic_api_client import BasicAsyncAPIClient
from cognite.client.data_classes.ai import ForecastInputTimeSeries

MAX_SERIES_PER_REQUEST = 32
# The API limits the request body as sent: compressed, unless gzip is disabled.
MAX_REQUEST_BYTES = 5 * 1024 * 1024


def request_body(series: Sequence[ForecastInputTimeSeries]) -> dict[str, Any]:
    """The body of a forecast or impute request for the series, in the order given.

    Args:
        series (Sequence[ForecastInputTimeSeries]): The series to send.

    Returns:
        dict[str, Any]: The request body.
    """
    return {"items": [s.dump(camel_case=True) for s in series]}


def size_as_sent(series: Sequence[ForecastInputTimeSeries]) -> int:
    """The size in bytes of the request body for the series, serialized and compressed as the SDK sends it.

    Args:
        series (Sequence[ForecastInputTimeSeries]): The series to send.

    Returns:
        int: The size of the body as sent.
    """
    content = BasicAsyncAPIClient._handle_json_dump(request_body(series), {}) or b""
    return len(content.encode() if isinstance(content, str) else content)


def group_series_by_cohort(series: Sequence[ForecastInputTimeSeries]) -> list[list[int]]:
    """Group the series that must be sent in the same request: a cohort is one group, and each series without a cohort
    is a group of its own.

    Args:
        series (Sequence[ForecastInputTimeSeries]): All series for one call.

    Returns:
        list[list[int]]: The positions in `series` of each group, in the order the groups first appear.
    """
    groups: dict[str | int, list[int]] = defaultdict(list)
    for position, s in enumerate(series):
        groups[s.cohort if s.cohort is not None else position].append(position)
    return list(groups.values())


def validate_series_groups(series: Sequence[ForecastInputTimeSeries], groups: Sequence[Sequence[int]]) -> None:
    """Check that every group fits in a request on its own.

    Args:
        series (Sequence[ForecastInputTimeSeries]): All series for one call.
        groups (Sequence[Sequence[int]]): The positions in `series` of each group.

    Raises:
        ValueError: If a cohort has more than 32 series, or a cohort or a series is over 5 MiB on its own.
    """
    for group in groups:
        members = [series[position] for position in group]
        name = f"Cohort {members[0].cohort!r}" if members[0].cohort is not None else f"Series {members[0].label!r}"
        if len(group) > MAX_SERIES_PER_REQUEST:
            raise ValueError(
                f"{name} has {len(group)} series, but a cohort must fit in one request of at most "
                f"{MAX_SERIES_PER_REQUEST} series."
            )
        if (group_bytes := size_as_sent(members)) > MAX_REQUEST_BYTES:
            raise ValueError(
                f"{name} is {group_bytes / 2**20:.1f} MiB as sent, but a request can be at most "
                f"{MAX_REQUEST_BYTES / 2**20:g} MiB. Send fewer or shorter series."
            )


def pack_series_requests(
    series: Sequence[ForecastInputTimeSeries], groups: Sequence[Sequence[int]]
) -> list[list[ForecastInputTimeSeries]]:
    """Pack the groups into requests of at most 32 series and 5 MiB, never splitting a group across requests.

    Each group is measured on its own and the sizes are added up, so the size of a request is approximate.
    The series keep their input order within each request.

    Args:
        series (Sequence[ForecastInputTimeSeries]): All series for one call.
        groups (Sequence[Sequence[int]]): The positions in `series` of each group, checked with
            `validate_series_groups`.

    Returns:
        list[list[ForecastInputTimeSeries]]: The series for each request.
    """
    requests: list[list[int]] = []
    request_bytes = 0
    for group in groups:
        group_bytes = size_as_sent([series[position] for position in group])
        if (
            not requests
            or len(requests[-1]) + len(group) > MAX_SERIES_PER_REQUEST
            or request_bytes + group_bytes > MAX_REQUEST_BYTES
        ):
            requests.append([])
            request_bytes = 0
        requests[-1].extend(group)
        request_bytes += group_bytes
    # Keep the input order within each request, so that the API's error locations (`items[i]`) match it.
    return [[series[position] for position in sorted(request)] for request in requests]
