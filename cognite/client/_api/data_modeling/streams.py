from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client._api_client import APIClient
from cognite.client.data_classes.data_modeling.streams import Stream, StreamList, StreamWrite
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._experimental import FeaturePreviewWarning

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class StreamsAPI(APIClient):
    _RESOURCE_PATH = "/streams"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._CREATE_LIMIT = 1
        self._warning = FeaturePreviewWarning(api_maturity="beta", sdk_maturity="alpha", feature_name="Streams API")

    def retrieve(self, external_id: str, include_statistics: bool = False) -> Stream | None:
        """`Retrieve a stream by external ID. <https://developer.cognite.com/api#tag/Streams/operation/getStream>`_

        Args:
            external_id (str): Stream external ID
            include_statistics (bool): If set to True, usage statistics will be returned together with stream settings. Defaults to False.

        Returns:
            Stream | None: Requested stream or None if it does not exist.

        Examples:

            Retrieve a single stream by external id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.data_modeling.streams.retrieve(external_id='myStream')

            Retrieve a stream with statistics:

                >>> res = client.data_modeling.streams.retrieve(external_id='myStream', include_statistics=True)

        """
        self._warning.warn()
        params = {"includeStatistics": "true"} if include_statistics else None
        try:
            result = self._get(url_path=f"{self._RESOURCE_PATH}/{external_id}", params=params)
            return Stream._load(result.json(), cognite_client=self._cognite_client)
        except CogniteAPIError as e:
            if e.code == 404:
                return None
            raise

    def delete(self, external_id: str) -> None:
        """`Delete a stream <https://developer.cognite.com/api#tag/Streams/operation/deleteStreams>`_

        Args:
            external_id (str): External ID of stream.

        Examples:

            Delete a stream by external id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.data_modeling.streams.delete(external_id="myStream")
        """
        self._warning.warn()
        self._post(url_path=f"{self._RESOURCE_PATH}/delete", json={"items": [{"externalId": external_id}]})

    def list(self) -> StreamList:
        """`List streams <https://developer.cognite.com/api#tag/Streams/operation/listStreams>`_

        Returns all streams available in the project.

        Returns:
            StreamList: List of all streams in the project

        Examples:

            List all streams:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> stream_list = client.data_modeling.streams.list()

            Iterate over the returned list:

                >>> for stream in stream_list:
                ...     stream # do something with the stream
        """
        self._warning.warn()
        return self._list(
            list_cls=StreamList,
            resource_cls=Stream,
            method="GET",
            limit=None,
        )

    def create(self, stream: StreamWrite) -> Stream:
        """`Create a stream. <https://developer.cognite.com/api#tag/Streams/operation/createStream>`_

        Args:
            stream (StreamWrite): Stream to create.

        Returns:
            Stream: Created stream

        Examples:

            Create a new stream:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import StreamWrite, StreamWriteSettings
                >>> client = CogniteClient()
                >>> stream = StreamWrite(
                ...     external_id="myStream",
                ...     settings=StreamWriteSettings(template_name="ImmutableTestStream")
                ... )
                >>> res = client.data_modeling.streams.create(stream)
        """
        self._warning.warn()
        return (
            self._create_multiple(  # using create multiple to leverage existing code, but only allowing one at a time
                list_cls=StreamList,
                resource_cls=Stream,
                items=stream,
                input_resource_cls=StreamWrite,
            )
        )
