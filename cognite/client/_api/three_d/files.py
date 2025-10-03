from __future__ import annotations

from cognite.client._api_client import APIClient
from cognite.client.utils._auxiliary import interpolate_and_url_encode


class ThreeDFilesAPI(APIClient):
    _RESOURCE_PATH = "/3d/files"

    def retrieve(self, id: int) -> bytes:
        """`Retrieve the contents of a 3d file by id. <https://developer.cognite.com/api#tag/3D-Files/operation/get3DFile>`_

        Args:
            id (int): The id of the file to retrieve.

        Returns:
            bytes: The contents of the file.

        Example:

            Retrieve the contents of a 3d file by id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.three_d.files.retrieve(1)
        """
        path = interpolate_and_url_encode(self._RESOURCE_PATH + "/{}", id)
        return self._get(path).content
