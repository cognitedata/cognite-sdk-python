
from cognite.client.data_classes.data_sets import DataSet
from cognite.client.exceptions import CogniteAPIError
import pytest
import time

from cognite.client._cognite_client import CogniteClient

def get_workflow_seed_data(data_set_id: int, file_id: int):
    timestamp = int(time.time() * 1000)
    simulator = {
        "name": "test_sim_for_workflow",
        "externalId": "test_sim_for_workflow",
        "fileExtensionTypes": ["json"],
        "modelTypes": [{"name": "Steady State", "key": "SteadyState"}],
        "stepFields": [
            {
                "stepType": "get/set",
                "fields": [
                    {
                        "info": "The address of the input/output",
                        "name": "address",
                        "label": "Address",
                    }
                ],
            },
            {
                "stepType": "command",
                "fields": [
                    {
                        "info": "The command to execute",
                        "name": "command",
                        "label": "Command",
                        "options": [{"label": "Solve Flowsheet", "value": "Solve"}],
                    }
                ],
            },
        ],
        "unitQuantities": [
            {
                "name": "temperature",
                "label": "Temperature",
                "units": [
                    {"label": "K", "name": "K"},
                    {"label": "R", "name": "R"},
                    {"label": "C", "name": "C"},
                    {"label": "F", "name": "F"},
                ],
            },
        ],
    }

    simulator_integration = {
        "externalId": "integration_tests_workflow_connector",
        "simulatorExternalId": simulator["externalId"],
        "heartbeat": timestamp,
        "dataSetId": data_set_id,
        "connectorVersion": "1.0.0",
        "simulatorVersion": "1.0.0",
    }

    simulator_model = {
        "externalId": "integration_tests_workflow_model",
        "simulatorExternalId": simulator["externalId"],
        "name": "Test Simulator Model",
        "description": "Test Simulator Model Desc",
        "dataSetId": data_set_id,
        "type": "SteadyState",
    }

    simulator_model_revision = {
        "externalId": "integration_tests_workflow_model_revision",
        "modelExternalId": "integration_tests_workflow_model",
        "description": "test sim model revision description",
        "fileId": file_id,
    }

    simulator_routine = {
        "externalId": "integration_tests_workflow_routine",
        "modelExternalId": simulator_model["externalId"],
        "simulatorIntegrationExternalId": simulator_integration["externalId"],
        "name": "Routine test",
    }

    simulator_routine_revision = {
        "externalId": "integration_tests_workflow_routine_revision",
        "routineExternalId": simulator_routine["externalId"],
        "configuration": {
            "schedule": {"enabled": False },
            "dataSampling": {"enabled": False},
            "logicalCheck": [],
            "steadyStateDetection": [],
            "inputs": [
                {
                    "name": "Cold Water Temperature",
                    "referenceId": "CWT",
                    "value": 10,
                    "valueType": "DOUBLE",
                    "unit": {"name": "C", "quantity": "temperature"},
                },
            ],
            "outputs": [
                {
                    "name": "Shower Temperature",
                    "referenceId": "ST",
                    "unit": {"name": "C", "quantity": "temperature"},
                    "valueType": "DOUBLE",
                }
            ],
        },
        "script": [
            {
                "order": 1,
                "description": "Set Inputs",
                "steps": [
                    {
                        "order": 1,
                        "stepType": "Set",
                        "arguments": {"referenceId": "CWT", "address": "input.com"},
                    }
                ],
            },
            {
                "order": 2,
                "description": "Solve the flowsheet",
                "steps": [{"order": 1, "stepType": "Command", "arguments": {"command": "Solve"}}],
            },
            {
                "order": 3,
                "description": "Set simulation outputs",
                "steps": [
                    {
                        "order": 1,
                        "stepType": "Get",
                        "arguments": {"referenceId": "ST", "address": "output.com"},
                    },
                ],
            },
        ],
    }

    return {
        "": simulator,
        "/integrations": simulator_integration,
        "/models": simulator_model,
        "/models/revisions": simulator_model_revision,
        "/routines": simulator_routine,
        "/routines/revisions": simulator_routine_revision,
    }

def update_seed_integration(integration_id: int, cognite_client: CogniteClient):
    cognite_client.post(f"/api/v1/projects/{cognite_client.config.project}/simulators/integrations/update", json={
        "items": [{
            "id": integration_id,
            "update": {
                "heartbeat": {
                    "set": int(time.time() * 1000)
                }
            }
        }]
    })

def get_seed_simulator_integration(cognite_client: CogniteClient):
    integrations_list = cognite_client.post(
        f"/api/v1/projects/{cognite_client.config.project}/simulators/integrations/list",
        json={},
    ).json()["items"]
    integrations = [item for item in integrations_list if item["externalId"] == "integration_tests_workflow_connector"]
    if len(integrations) == 0:
        return None
    return integrations[0]

@pytest.fixture
def workflow_simint_routine(cognite_client: CogniteClient) -> str:
    data_set = cognite_client.data_sets.retrieve(external_id="integration_tests_workflow")


    if data_set is None:
        data_set = cognite_client.data_sets.create(
            DataSet(
                external_id="integration_tests_workflow",
                name="Integration Tests Workflow",
                description="Data set for integration tests of the workflow API",
            )
        )

    file = cognite_client.files.retrieve(external_id="integration_tests_workflow_model_file")

    if file is None:
        file = cognite_client.files.upload(
            path="tests/tests_integration/test_api/test_workflows/seed_model.json",
            external_id="integration_tests_workflow_model_file",
            name="seed_mode.json",
            data_set_id=data_set.id,
        )

    integration = get_seed_simulator_integration(cognite_client)

    seed_data = get_workflow_seed_data(data_set.id, file.id)
    if integration is None:
        for path, item in seed_data.items():
            try:
                cognite_client.post(
                    f"/api/v1/projects/{cognite_client.config.project}/simulators{path}",
                    json={"items": [item]},
                )
            except CogniteAPIError as e:
                pass

    integration = get_seed_simulator_integration(cognite_client)
    update_seed_integration(integration["id"], cognite_client)

    return seed_data["/routines"]["externalId"]

def finish_simulation_runs(cognite_client: CogniteClient, routine_external_id: str):
    list_runs = cognite_client.post(
        f"/api/v1/projects/{cognite_client.config.project}/simulators/runs/list",
        json={ 
            "filter": {
                "routineExternalIds": [routine_external_id],
                "status": "ready",
                "createdTime": { "min": int(time.time() * 1000) - 1000 * 60 }
            },
            "limit": 10
        }
    ).json()["items"]

    for run in list_runs:
        try:
            cognite_client.post(
                f"/api/v1/projects/{cognite_client.config.project}/simulators/run/callback",
                json={ "items": [{ "id": run["id"], "status": "success" }] }
            )
        except CogniteAPIError as e:
            pass
