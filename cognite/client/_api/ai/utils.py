from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Sequence

from cognite.client.data_classes.ai import ForecastInputTimeSeries

MAX_SERIES_PER_REQUEST = 32
# The API limits the request body as sent: compressed, unless gzip is disabled.
MAX_REQUEST_BYTES = 5 * 1024 * 1024


def split_into_requests(
    series: Sequence[ForecastInputTimeSeries], size_of: Callable[[Sequence[ForecastInputTimeSeries]], int]
) -> list[list[ForecastInputTimeSeries]]:
    """Pack series into requests of at most 32 series and 5 MiB, never splitting a cohort across requests.

    Series keep their input order within each request.

    Args:
        series (Sequence[ForecastInputTimeSeries]): All series for one call.
        size_of (Callable[[Sequence[ForecastInputTimeSeries]], int]): The size in bytes, as sent, of a request body
            with the given series. Each cohort, and each series without one, is measured on its own, and those sizes
            are added up, which is at least the size of the combined body.

    Returns:
        list[list[ForecastInputTimeSeries]]: The series for each request.

    Raises:
        ValueError: If a cohort has more than 32 series, or a cohort or a series is over 5 MiB on its own.
    """
    groups: dict[str | int, list[int]] = defaultdict(list)
    for i, s in enumerate(series):
        groups[s.cohort if s.cohort is not None else i].append(i)

    requests: list[list[int]] = []
    request_bytes = 0
    for positions in groups.values():
        group = [series[i] for i in positions]
        name = f"Cohort {group[0].cohort!r}" if group[0].cohort is not None else f"Series {group[0].label!r}"
        if len(group) > MAX_SERIES_PER_REQUEST:
            raise ValueError(
                f"{name} has {len(group)} series, but a cohort must fit in one request of at most "
                f"{MAX_SERIES_PER_REQUEST} series."
            )
        group_bytes = size_of(group)
        if group_bytes > MAX_REQUEST_BYTES:
            raise ValueError(
                f"{name} is {group_bytes / 2**20:.1f} MiB as sent, but a request can be at most "
                f"{MAX_REQUEST_BYTES / 2**20:g} MiB. Send fewer or shorter series."
            )
        if (
            not requests
            or len(requests[-1]) + len(group) > MAX_SERIES_PER_REQUEST
            or request_bytes + group_bytes > MAX_REQUEST_BYTES
        ):
            requests.append([])
            request_bytes = 0
        requests[-1].extend(positions)
        request_bytes += group_bytes
    # Keep the input order within each request, so that the API's error locations (`items[i]`) match it.
    return [[series[i] for i in sorted(request)] for request in requests]
