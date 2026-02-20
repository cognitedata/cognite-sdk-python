from __future__ import annotations

import logging
import os

import cognite.client as cc  # Do not import individual entities
from cognite.client.config import ClientConfig, global_config
from cognite.client.credentials import CredentialProvider

logger = logging.getLogger(__name__)


def patch_sdk_for_pyodide() -> None:
    # Patch Pyodide related issues
    # -----------------
    # Patch Cognite SDK
    # - For good measure ;)
    global_config.disable_pypi_version_check = True

    # - Disable gzip. Although supported in pyodide, setting header 'content-encoding = gzip'
    #   currently gets blocked by the browser policy. If we return 'content-encoding' as part
    #   of 'access-control-allow-headers' from the pre-flight OPTIONS request, this prob goes away:
    global_config.disable_gzip = True

    # - Inject these magic classes into the correct modules so that the user may import them normally:
    cc.config.FusionNotebookConfig = FusionNotebookConfig  # type: ignore [attr-defined]

    # - If we are running inside of a JupyterLite Notebook spawned from Cognite Data Fusion, we set
    #   the default config to FusionNotebookConfig(). This allows the user to:
    #   >>> from cognite.client import CogniteClient
    #   >>> client = CogniteClient()
    if os.getenv("COGNITE_FUSION_NOTEBOOK") is not None:
        global_config.default_client_config = FusionNotebookConfig()

    # - We attempt to load the package 'tzdata' automatically, as pyodide can't read IANA timezone info from
    #   the OS and thus need this extra package. We need the timezone info because we use zoneinfo.ZoneInfo
    #   internally for e.g. datapoints and workflows.
    #   Note: This convenience will only work in chromium-based browsers (as of Sept 2025)
    try:
        import micropip  # type: ignore [import-not-found]
        from pyodide.ffi import run_sync  # type: ignore [import-not-found]

        run_sync(micropip.install("tzdata"))
    except Exception:
        logger.debug(
            "Could not load 'tzdata' package automatically in pyodide. You may need to do this manually:"
            "import micropip; await micropip.install('tzdata')"
        )


class EnvVarToken(CredentialProvider):
    """Credential provider that always reads token from an environment variable just-in-time,
    allowing refreshing the value by another entity.

    Args:
        key: The name of the env.var. to read from. Default: 'COGNITE_TOKEN'
    Raises:
        KeyError: If the env.var. is not set.
    """

    def __init__(self, key: str = "COGNITE_TOKEN") -> None:
        self.key = key

    def __token_factory(self) -> str:
        return os.environ[self.key]

    def authorization_header(self) -> tuple[str, str]:
        return "Authorization", f"Bearer {self.__token_factory()}"


class FusionNotebookConfig(ClientConfig):
    def __init__(
        self,
        client_name: str = "DSHubLite",
        api_subversion: str | None = None,
        headers: dict[str, str] | None = None,
        timeout: int | None = None,
        file_transfer_timeout: int | None = None,
        debug: bool = False,
    ) -> None:
        super().__init__(
            client_name=client_name,
            api_subversion=api_subversion,
            headers=headers,
            timeout=timeout,
            file_transfer_timeout=file_transfer_timeout,
            debug=debug,
            project=os.environ["COGNITE_PROJECT"],
            credentials=EnvVarToken(),  # Magic!
            base_url=os.environ["COGNITE_BASE_URL"],
        )
