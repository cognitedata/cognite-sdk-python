from __future__ import annotations

from cognite.client._api_client import APIClient
from cognite.client.data_classes.iam import TokenInspection


class TokenAPI(APIClient):
    async def inspect(self) -> TokenInspection:
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
        response = await self._get("/api/v1/token/inspect")
        return TokenInspection.load(response.json(), self._cognite_client, allow_unknown=True)
