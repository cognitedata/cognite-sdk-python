from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client._api_client import APIClient
from cognite.client.data_classes.iam import TokenInspection

if TYPE_CHECKING:
    pass


class TokenAPI(APIClient):
    def inspect(self) -> TokenInspection:
        """Inspect a token.

        Get details about which projects it belongs to and which capabilities are granted to it.

        Returns:
            TokenInspection: The object with token inspection details.

        Example:

            Inspect token::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.iam.token.inspect()
        """
        # To not raise whenever new Acls/actions/scopes are added to the API, we specifically allow the unknown:
        return TokenInspection.load(self._get("/api/v1/token/inspect").json(), self._cognite_client, allow_unknown=True)
