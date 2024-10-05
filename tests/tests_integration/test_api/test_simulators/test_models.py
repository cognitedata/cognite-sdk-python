from __future__ import annotations

from contextlib import suppress

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import DataSet
from cognite.client.data_classes.simulators import (
    SimulatorModel,
    SimulatorModelUpdate,
    SimulatorModelWrite,
)
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._text import random_string


class TestSimulatorModels:
    def test_create_update_retrieve_delete(self, cognite_client: CogniteClient, a_data_set: DataSet) -> None:
        my_model = SimulatorModelWrite(
            external_id=f"myNewSimulatorModelForTesting-{random_string(10)}",
            simulator_external_id="mySimulator",
            name="myModel",
            data_set_id=a_data_set.id,
            type="classification",
        )
        created: SimulatorModel | None = None
        try:
            created = cognite_client.simulators.models.create(my_model)
            assert isinstance(created, SimulatorModel)
            update = SimulatorModelUpdate(id=created.id).name.set("newName")
            updated = cognite_client.simulators.mappings.update(update)
            assert updated.name == "newName"
            retrieved = cognite_client.simulators.models.retrieve(external_id=created.external_id)
            assert retrieved is not None
            assert retrieved.external_id == my_model.external_id
            assert retrieved.name == updated.name

            cognite_client.simulators.models.delete(external_id=created.external_id)

            with pytest.raises(CogniteAPIError):
                cognite_client.simulators.models.retrieve(external_id=created.external_id)

        finally:
            if created:
                with suppress(CogniteAPIError):
                    cognite_client.simulators.models.delete(external_id=created.external_id)
