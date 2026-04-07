from __future__ import annotations

from collections.abc import Callable, Iterator
from unittest import mock

import pytest

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes import DataSet, DataSetFilter, DataSetUpdate, DataSetWrite
from cognite.client.exceptions import CogniteNotFoundError


@pytest.fixture(scope="class")
def new_dataset(cognite_client: CogniteClient, os_and_py_version: str) -> Iterator[DataSet]:
    xid = "pysdk-test-dataset-" + os_and_py_version
    dataset = cognite_client.data_sets.retrieve(external_id=xid)
    if dataset:
        yield dataset
    else:
        # Datasets can't be deleted so we persist one per test runner:
        yield cognite_client.data_sets.create(
            DataSetWrite(
                name=xid.replace("-", " "),
                description="Delete me, I dare you",
                external_id=xid,
            )
        )


@pytest.fixture
def post_spy(async_client: AsyncCogniteClient) -> Iterator[None]:
    with mock.patch.object(async_client.data_sets, "_post", wraps=async_client.data_sets._post) as _:
        yield


class TestDataSetsAPI:
    def test_retrieve(self, cognite_client: CogniteClient, new_dataset: DataSet) -> None:
        retrieved = cognite_client.data_sets.retrieve(new_dataset.id)
        assert retrieved and new_dataset.id == retrieved.id

    def test_retrieve_multiple(self, cognite_client: CogniteClient) -> None:
        res_listed_ids = cognite_client.data_sets.list(limit=2).as_ids()
        res_lookup_ids = cognite_client.data_sets.retrieve_multiple(res_listed_ids).as_ids()
        assert len(res_listed_ids) == len(res_lookup_ids) == 2
        assert set(res_listed_ids) == set(res_lookup_ids)

    def test_retrieve_unknown(self, cognite_client: CogniteClient, new_dataset: DataSet) -> None:
        invalid_external_id = "this does not exist"
        with pytest.raises(CogniteNotFoundError) as error:
            cognite_client.data_sets.retrieve_multiple(ids=[new_dataset.id], external_ids=[invalid_external_id])
        assert error.value.not_found[0]["externalId"] == invalid_external_id

    def test_list(
        self,
        cognite_client: CogniteClient,
        async_client: AsyncCogniteClient,
        post_spy: None,
        set_request_limit: Callable,
    ) -> None:
        set_request_limit(async_client.data_sets, 1)
        res = cognite_client.data_sets.list(limit=2)

        assert 2 == len(res)
        assert 2 == async_client.data_sets._post.call_count  # type: ignore[attr-defined]

    def test_aggregate(self, cognite_client: CogniteClient) -> None:
        res = cognite_client.data_sets.aggregate_count(filter=DataSetFilter(metadata={"1": "1"}))
        assert res > 0

    def test_update(self, cognite_client: CogniteClient, new_dataset: DataSet) -> None:
        update_asset = DataSetUpdate(new_dataset.id).metadata.set({"1": "1"}).name.set("newname")
        res = cognite_client.data_sets.update(update_asset)
        assert {"1": "1"} == res.metadata
        assert "newname" == res.name
