from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, overload

from cognite.client._async_api_client import AsyncAPIClient
from cognite.client._constants import DEFAULT_LIMIT_READ


class AsyncThreeDAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/3d"
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # 3D API has sub-APIs for models, revisions, etc.
        # For now, implement as placeholders - full implementation would need sub-APIs