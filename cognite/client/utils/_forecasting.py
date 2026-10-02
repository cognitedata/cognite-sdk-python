from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence

from cognite.client.data_classes.ai import InputTimeSeries

MAX_SERIES_PER_REQUEST = 256
MAX_SERIES_PER_COHORT = 256


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
