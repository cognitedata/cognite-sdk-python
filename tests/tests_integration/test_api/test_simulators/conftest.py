from __future__ import annotations

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import DataSet, DataSetWrite


@pytest.fixture(scope="session")
def a_data_set(cognite_client: CogniteClient) -> DataSet:
    ds = DataSetWrite(external_id="test-dataset-simulators", name="test-dataset-simulators")
    retrieved = cognite_client.data_sets.retrieve(external_id=ds.external_id)
    if retrieved:
        return retrieved
    created = cognite_client.data_sets.create(ds)
    return created
