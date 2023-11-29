from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING

from typing_extensions import Self

if TYPE_CHECKING:
    from requests import Response


@dataclass
class RequestDetails:
    """
    SDK users wanting to track their own API usage (with the SDK) - for metrics or surveilance, may set
    a callback on the global_config object that will then receive instances of this class, one per
    actual request.

    Note that due to concurrency, the sum of time_elapsed is (much) greater than the actual wall clock
    waiting time.

    Args:
        url (str): The API endpoint that was called.
        status_code (int): The status code of the API response.
        content_length (int | None): The size of the response if available.
        time_elapsed (timedelta): The amount of time elapsed between sending the request and the arrival of the response.

    Example:

        Store info on the last 1000 requests made:

            >>> from cognite.client.config import global_config
            >>> from collections import deque
            >>> usage_info = deque(maxlen=1000)
            >>> global_config.usage_tracking_callback = usage_info.append

        Store the time elapsed per request, grouped per API endpoint, for all requests:

            >>> from collections import defaultdict
            >>> usage_info = defaultdict(list)
            >>> def callback(details):
            ...     usage_info[details.url].append(details.time_elapsed)
            >>> global_config.usage_tracking_callback = callback

    Tip:
        Ensure the provided callback is fast to execute, or it might negatively impact the overall performance.

    Warning:
        Your provided callback function will be called from several different threads and thus any operation
        executed must be thread-safe (or while holding a thread lock, not recommended). Best practise is to dump
        the required details to a container like in the examples above, then inspect those separately in your code.
    """

    url: str
    status_code: int
    content_length: int | None
    time_elapsed: timedelta

    @classmethod
    def from_response(cls, resp: Response) -> Self:
        # If header not set, we don't report the size. We could do len(resp.content), but
        # for streaming requests this would fetch everything into memory...
        content_length = int(resp.headers.get("Content-length", 0)) or None
        return cls(
            url=resp.url,
            status_code=resp.status_code,
            content_length=content_length,
            time_elapsed=resp.elapsed,
        )
