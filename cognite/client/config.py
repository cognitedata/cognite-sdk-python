import pprint
from contextlib import suppress

from cognite.client import utils
from cognite.client._version import __api_subversion__


class GlobalConfig:
    def __init__(self):
        self.default_client_config: Optional[ClientConfig] = None
        self.disable_gzip: bool = False
        self.disable_pypi_version_check: bool = False
        self.status_forcelist: Set[int] = {429, 502, 503, 504}
        self.max_retries: int = 10
        self.max_retry_backoff: int = 30
        self.max_connection_pool_size: int = 50
        self.disable_ssl: bool = False
        self.proxies: Optional[Dict[(str, str)]] = {}


global_config = GlobalConfig()


class ClientConfig:
    def __init__(
        self,
        client_name,
        project,
        credentials,
        api_subversion=None,
        base_url=None,
        max_workers=None,
        headers=None,
        timeout=None,
        file_transfer_timeout=None,
        debug=False,
    ):
        self.client_name = client_name
        self.project = project
        self.credentials = credentials
        self.api_subversion = api_subversion or __api_subversion__
        self.base_url = (base_url or "https://api.cognitedata.com").rstrip("/")
        self.max_workers = max_workers if (max_workers is not None) else 10
        self.headers = headers or {}
        self.timeout = timeout or 30
        self.file_transfer_timeout = file_transfer_timeout or 600
        self.debug = debug
        if debug:
            utils._logging._configure_logger_for_debug_mode()
        if not global_config.disable_pypi_version_check:
            with suppress(Exception):
                utils._auxiliary._check_client_has_newest_major_version()

    def __str__(self):
        return pprint.pformat(self.__dict__, indent=4)

    def _repr_html_(self):
        return str(self)
