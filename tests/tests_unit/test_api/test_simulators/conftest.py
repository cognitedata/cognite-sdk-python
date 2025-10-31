import json
import re
from typing import Callable
from requests import PreparedRequest
from responses import RequestsMock

from tests.utils import jsgz_load


def add_mocked_request(rsps: RequestsMock, url: str, request_callback: Callable[[dict], tuple[int, dict]]) -> None:
    url_pattern = re.compile(re.escape(url))

    def request_callback_wrapper(request: PreparedRequest) -> tuple[int, dict, str]:
        request_payload = jsgz_load(request.body) if request.body else {}
        status, response_item = request_callback(request_payload)
        response_body = {"items": [response_item]}
        return status, {}, json.dumps(response_body)
    
    rsps.add_callback(
        method=rsps.POST,
        url=url_pattern,
        callback=request_callback_wrapper
    )
