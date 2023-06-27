from __future__ import annotations

import sys

from cognite.client._cognite_client import CogniteClient
from cognite.client._version import __version__
from cognite.client.config import ClientConfig, global_config
from cognite.client.data_classes import data_modeling

__all__ = ["ClientConfig", "CogniteClient", "__version__", "global_config", "data_modeling"]

_RUNNING_IN_BROWSER = sys.platform == "emscripten" and "pyodide" in sys.modules

if _RUNNING_IN_BROWSER:
    from cognite.client.utils._pyodide_helpers import patch_sdk_for_pyodide

    patch_sdk_for_pyodide()
