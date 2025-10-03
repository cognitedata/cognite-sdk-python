from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.data_modeling.streams import Stream, StreamList, StreamWrite
from cognite.client.utils._experimental import FeaturePreviewWarning, warn_on_all_method_invocations
from cognite.client.utils._identifier import Identifier

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


@warn_on_all_method_invocations(
    FeaturePreviewWarning(api_maturity="alpha", sdk_maturity="alpha", feature_name="Records API")
)
class StreamsAPI(APIClient):
    _RESOURCE_PATH = "/streams"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._CREATE_LIMIT = 1
        self.__alpha_headers = {
            "cdf-version": "alpha",
        }

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        limit: int | None = None,
    ) -> Iterator[Stream]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        limit: int | None = None,
    ) -> Iterator[StreamList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        limit: int | None = None,
    ) -> Iterator[Stream] | Iterator[StreamList]:
        """Iterate over streams

        Fetches streams as they are iterated over, so you keep a limited number of streams in memory.

        Args:
            chunk_size (int | None): Number of streams to return in each chunk. Defaults to yielding one stream a time.
            limit (int | None): Maximum number of streams to return. Defaults to returning all items.

        Returns:
            Iterator[Stream] | Iterator[StreamList]: yields Stream one by one if chunk_size is not specified, else StreamList objects.
        """
        return self._list_generator(
            list_cls=StreamList,
            resource_cls=Stream,
            method="GET",
            chunk_size=chunk_size,
            limit=limit,
            headers=self.__alpha_headers,
        )

    def __iter__(self) -> Iterator[Stream]:
        """Iterate over streams

        Fetches streams as they are iterated over, so you keep a limited number of streams in memory.

        Returns:
            Iterator[Stream]: yields Streams one by one.
        """
        return self()

    def retrieve(self, external_id: str) -> Stream | None:
        """`Retrieve a stream. <https://developer.cognite.com/api#tag/Streams/operation/byStreamIdsStreams>`_

        Args:
            external_id (str): No description.

        Returns:
            Stream | None: Requested stream or None if it does not exist.

        Examples:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.data_modeling.streams.retrieve(streams='myStream')

            Get multiple streams by id:

                >>> res = client.data_modeling.streams.retrieve(streams=["MyStream", "MyAwesomeStream", "MyOtherStream"])

        """
        identifier = Identifier.load(external_id=external_id)
        return self._retrieve(identifier=identifier, cls=Stream, headers=self.__alpha_headers)

    def delete(self, external_id: str) -> None:
        """`Delete one or more streams <https://developer.cognite.com/api#tag/Streams/operation/deleteStreamsV3>`_

        Args:
            external_id (str): ID of streams.
        Examples:

            Delete streams by id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.data_modeling.streams.delete(streams=["myStream", "myOtherStream"])
        """
        self._delete(url_path=f"{self._RESOURCE_PATH}/{external_id}", headers=self.__alpha_headers)

    def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> StreamList:
        """`List streams <https://developer.cognite.com/api#tag/Streams/operation/listStreamsV3>`_

        Args:
            limit (int | None): Maximum number of streams to return. Defaults to 10. Set to -1, float("inf") or None to return all items.

        Returns:
            StreamList: List of requested streams

        Examples:

            List streams and filter on max start time:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> stream_list = client.data_modeling.streams.list(limit=5)

            Iterate over streams:

                >>> for stream in client.data_modeling.streams:
                ...     stream # do something with the stream

            Iterate over chunks of streams to reduce memory load:

                >>> for stream_list in client.data_modeling.streams(chunk_size=2500):
                ...     stream_list # do something with the streams
        """
        return self._list(
            list_cls=StreamList, resource_cls=Stream, method="GET", limit=limit, headers=self.__alpha_headers
        )

    @overload
    def create(self, streams: Sequence[StreamWrite]) -> StreamList: ...

    @overload
    def create(self, streams: StreamWrite) -> Stream: ...

    def create(self, streams: StreamWrite | Sequence[StreamWrite]) -> Stream | StreamList:
        """`Create one or more streams. <https://developer.cognite.com/api#tag/Streams/operation/ApplyStreams>`_

        Args:
            streams (StreamWrite | Sequence[StreamWrite]): Stream | Sequence[Stream]): Stream or streams of streamsda to create or update.

        Returns:
            Stream | StreamList: Created stream(s)

        Examples:

            Create new streams:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling.streams import StreamWrite
                >>> client = CogniteClient()
                >>> streams = [StreamWrite(stream="myStream", description="My first stream", name="My Stream"),
                ... StreamWrite(stream="myOtherStream", description="My second stream", name="My Other Stream")]
                >>> res = client.data_modeling.streams.create(streams)
        """
        return self._create_multiple(
            list_cls=StreamList,
            resource_cls=Stream,
            items=streams,
            input_resource_cls=StreamWrite,
            headers=self.__alpha_headers,
        )
