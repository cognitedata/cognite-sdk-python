# This file contains the data used to seed the test environment for the simulator tests
from cognite.client.data_classes.simulators.routine_revisions import (
    SimulationValueUnitInput,
    SimulatorRoutineConfiguration,
    SimulatorRoutineDataSampling,
    SimulatorRoutineInputConstant,
    SimulatorRoutineLogicalCheck,
    SimulatorRoutineOutput,
    SimulatorRoutineRevisionWrite,
    SimulatorRoutineSchedule,
    SimulatorRoutineStage,
    SimulatorRoutineSteadyStateDetection,
    SimulatorRoutineStep,
    SimulatorRoutineStepArguments,
)
from cognite.client.utils._text import random_string

data_set_external_id = "sdk_tests_dwsim1"

random_str = random_string(10)

resource_names = {
    "simulator_external_id": "py_sdk_integration_tests",
    "simulator_integration_external_id": "py_sdk_integration_tests_connector",
    "simulator_model_external_id": f"py_sdk_integration_tests_model_{random_str}",
    "simulator_model_revision_external_id": f"py_sdk_integration_tests_model_{random_str}_v1",
    "simulator_model_file_external_id": "ShowerMixer_simulator_model_file_5",
    "simulator_routine_external_id": f"pysdk_routine_{random_str}",
    "simulator_test_data_set_id": None,
    "simulator_test_data_set_external_id": data_set_external_id,
}

simulator = {
    "name": resource_names["simulator_external_id"],
    "externalId": resource_names["simulator_external_id"],
    "fileExtensionTypes": ["txt"],
    "modelTypes": [{"name": "Steady State", "key": "SteadyState"}],
    "stepFields": [
        {
            "stepType": "get/set",
            "fields": [
                {
                    "name": "objectName",
                    "label": "Simulation Object Name",
                    "info": "Enter the name of the DWSIM object, i.e. Feed",
                },
                {
                    "name": "objectProperty",
                    "label": "Simulation Object Property",
                    "info": "Enter the property of the DWSIM object, i.e. Temperature",
                },
            ],
        },
        {
            "stepType": "command",
            "fields": [
                {
                    "name": "command",
                    "label": "Command",
                    "info": "Select a command",
                    "options": [{"label": "Solve Flowsheet", "value": "Solve"}],
                }
            ],
        },
    ],
    "unitQuantities": [
        {
            "name": "mass",
            "label": "Mass",
            "units": [{"label": "kg", "name": "kg"}, {"label": "g", "name": "g"}, {"label": "lb", "name": "lb"}],
        },
        {
            "name": "time",
            "label": "Time",
            "units": [{"label": "s", "name": "s"}, {"label": "min.", "name": "min."}, {"label": "h", "name": "h"}],
        },
        {
            "name": "accel",
            "label": "Acceleration",
            "units": [
                {"label": "m/s2", "name": "m/s2"},
                {"label": "cm/s2", "name": "cm/s2"},
                {"label": "ft/s2", "name": "ft/s2"},
            ],
        },
        {
            "name": "force",
            "label": "Force",
            "units": [
                {"label": "N", "name": "N"},
                {"label": "dyn", "name": "dyn"},
                {"label": "kgf", "name": "kgf"},
                {"label": "lbf", "name": "lbf"},
            ],
        },
        {
            "name": "volume",
            "label": "Volume",
            "units": [
                {"label": "m3", "name": "m3"},
                {"label": "cm3", "name": "cm3"},
                {"label": "L", "name": "L"},
                {"label": "ft3", "name": "ft3"},
                {"label": "bbl", "name": "bbl"},
                {"label": "gal[US]", "name": "gal[US]"},
                {"label": "gal[UK]", "name": "gal[UK]"},
            ],
        },
        {
            "name": "density",
            "label": "Density",
            "units": [
                {"label": "kg/m3", "name": "kg/m3"},
                {"label": "g/cm3", "name": "g/cm3"},
                {"label": "lbm/ft3", "name": "lbm/ft3"},
            ],
        },
        {
            "name": "diameter",
            "label": "Diameter",
            "units": [{"label": "mm", "name": "mm"}, {"label": "in", "name": "in"}],
        },
        {
            "name": "distance",
            "label": "Distance",
            "units": [{"label": "m", "name": "m"}, {"label": "ft", "name": "ft"}, {"label": "cm", "name": "cm"}],
        },
        {
            "name": "heatflow",
            "label": "Heat Flow",
            "units": [
                {"label": "kW", "name": "kW"},
                {"label": "kcal/h", "name": "kcal/h"},
                {"label": "BTU/h", "name": "BTU/h"},
                {"label": "BTU/s", "name": "BTU/s"},
                {"label": "cal/s", "name": "cal/s"},
                {"label": "HP", "name": "HP"},
                {"label": "kJ/h", "name": "kJ/h"},
                {"label": "kJ/d", "name": "kJ/d"},
                {"label": "MW", "name": "MW"},
                {"label": "W", "name": "W"},
                {"label": "BTU/d", "name": "BTU/d"},
                {"label": "MMBTU/d", "name": "MMBTU/d"},
                {"label": "MMBTU/h", "name": "MMBTU/h"},
                {"label": "kcal/s", "name": "kcal/s"},
                {"label": "kcal/h", "name": "kcal/h"},
                {"label": "kcal/d", "name": "kcal/d"},
            ],
        },
        {
            "name": "pressure",
            "label": "Pressure",
            "units": [
                {"label": "Pa", "name": "Pa"},
                {"label": "atm", "name": "atm"},
                {"label": "kgf/cm2", "name": "kgf/cm2"},
                {"label": "kgf/cm2g", "name": "kgf/cm2g"},
                {"label": "lbf/ft2", "name": "lbf/ft2"},
                {"label": "kPa", "name": "kPa"},
                {"label": "kPag", "name": "kPag"},
                {"label": "bar", "name": "bar"},
                {"label": "barg", "name": "barg"},
                {"label": "ftH2O", "name": "ftH2O"},
                {"label": "inH2O", "name": "inH2O"},
                {"label": "inHg", "name": "inHg"},
                {"label": "mbar", "name": "mbar"},
                {"label": "mH2O", "name": "mH2O"},
                {"label": "mmH2O", "name": "mmH2O"},
                {"label": "mmHg", "name": "mmHg"},
                {"label": "MPa", "name": "MPa"},
                {"label": "psi", "name": "psi"},
                {"label": "psig", "name": "psig"},
            ],
        },
        {
            "name": "velocity",
            "label": "Velocity",
            "units": [
                {"label": "m/s", "name": "m/s"},
                {"label": "cm/s", "name": "cm/s"},
                {"label": "mm/s", "name": "mm/s"},
                {"label": "km/h", "name": "km/h"},
                {"label": "ft/h", "name": "ft/h"},
                {"label": "ft/min", "name": "ft/min"},
                {"label": "ft/s", "name": "ft/s"},
                {"label": "in/s", "name": "in/s"},
            ],
        },
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
        {
            "name": "volumetricFlow",
            "label": "Volumetric Flow",
            "units": [
                {"label": "m3/h", "name": "m3/h"},
                {"label": "cm3/s", "name": "cm3/s"},
                {"label": "L/h", "name": "L/h"},
                {"label": "L/min", "name": "L/min"},
                {"label": "L/s", "name": "L/s"},
                {"label": "ft3/h", "name": "ft3/h"},
                {"label": "ft3/min", "name": "ft3/min"},
                {"label": "ft3/s", "name": "ft3/s"},
                {"label": "gal[US]/h", "name": "gal[US]/h"},
                {"label": "gal[US]/min", "name": "gal[US]/min"},
                {"label": "gal[US]/s", "name": "gal[US]/s"},
                {"label": "gal[UK]/h", "name": "gal[UK]/h"},
                {"label": "gal[UK]/min", "name": "gal[UK]/min"},
                {"label": "gal[UK]/s", "name": "gal[UK]/s"},
            ],
        },
    ],
}

simulator_integration = {
    "externalId": resource_names["simulator_integration_external_id"],
    "simulatorExternalId": resource_names["simulator_external_id"],
    "heartbeat": 0,
    "dataSetId": resource_names["simulator_test_data_set_id"],
    "connectorVersion": "1.0.0",
    "simulatorVersion": "1.0.0",
    "licenseStatus": "AVAILABLE",
    "licenseLastCheckedTime": 0,
    "connectorStatus": "IDLE",
    "connectorStatusUpdatedTime": 0,
}

simulator_model = {
    "externalId": resource_names["simulator_model_external_id"],
    "simulatorExternalId": resource_names["simulator_external_id"],
    "name": "Test Simulator Model",
    "description": "Test Simulator Model Desc",
    "dataSetId": resource_names["simulator_test_data_set_id"],
    "type": "SteadyState",
}


simulator_routine = {
    "externalId": resource_names["simulator_routine_external_id"],
    "modelExternalId": resource_names["simulator_model_external_id"],
    "simulatorIntegrationExternalId": resource_names["simulator_integration_external_id"],
    "name": "Simulator Routine - Test",
    "description": "Simulator Routine - Description Test",
}

simulator_routine_revision_config_obj = {
    "schedule": {"enabled": True, "cronExpression": "*/10 * * * *"},
    "dataSampling": {"enabled": True, "samplingWindow": 15, "granularity": 1, "validationWindow": 5},
    "logicalCheck": [
        {
            "enabled": True,
            "aggregate": "average",
            "operator": "lt",
            "value": 75.8,
            "timeseriesExternalId": "VAL-45-PT-92608_test",
        }
    ],
    "steadyStateDetection": [
        {
            "enabled": True,
            "aggregate": "average",
            "minSectionSize": 1,
            "varThreshold": 3.5,
            "slopeThreshold": -7.5,
            "timeseriesExternalId": "VAL-45-PT-92608",
        }
    ],
    "inputs": [
        {
            "name": "Cold Water Temperature",
            "referenceId": "CWT",
            "value": 10.0,
            "valueType": "DOUBLE",
            "unit": {"name": "C", "quantity": "temperature"},
            "saveTimeseriesExternalId": "TEST-ROUTINE-INPUT-CWT",
        },
        {
            "name": "Cold Water Pressure",
            "referenceId": "CWP",
            "value": [3.6],
            "valueType": "DOUBLE_ARRAY",
            "unit": {"name": "bar", "quantity": "pressure"},
        },
    ],
    "outputs": [
        {
            "name": "Shower Temperature",
            "referenceId": "ST",
            "unit": {"name": "C", "quantity": "temperature"},
            "valueType": "DOUBLE",
            "saveTimeseriesExternalId": "TEST-ROUTINE-OUTPUT-ST",
        },
        {
            "name": "Shower Pressure",
            "referenceId": "SP",
            "unit": {"name": "bar", "quantity": "pressure"},
            "valueType": "DOUBLE",
        },
    ],
}

simulator_routine_revision_script_obj = [
    {
        "order": 1,
        "description": "Set Inputs",
        "steps": [
            {
                "order": 1,
                "stepType": "Set",
                "description": "Set Cold Water Temperature",
                "arguments": {"referenceId": "CWT", "objectName": "Cold water", "objectProperty": "Temperature"},
            },
            {
                "order": 2,
                "stepType": "Set",
                "description": "Set Cold Water Pressure",
                "arguments": {"referenceId": "CWP", "objectName": "Cold water", "objectProperty": "Pressure"},
            },
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
                "arguments": {"referenceId": "ST", "objectName": "Shower", "objectProperty": "Temperature"},
            },
            {
                "order": 2,
                "stepType": "Get",
                "arguments": {"referenceId": "SP", "objectName": "Shower", "objectProperty": "Pressure"},
            },
        ],
    },
]

simulator_routine_revision_obj = {
    "externalId": None,
    "routineExternalId": resource_names["simulator_routine_external_id"],
    "configuration": simulator_routine_revision_config_obj,
    "script": simulator_routine_revision_script_obj,
}


def create_simulator_routine_revision(
    external_id: str,
    routine_external_id: str,
) -> SimulatorRoutineRevisionWrite:
    """Create a test simulator routine revision object."""
    return SimulatorRoutineRevisionWrite(
        external_id=external_id,
        routine_external_id=routine_external_id,
        configuration=SimulatorRoutineConfiguration(
            schedule=SimulatorRoutineSchedule(
                cron_expression=simulator_routine_revision_config_obj["schedule"]["cronExpression"],
            ),
            data_sampling=SimulatorRoutineDataSampling(
                sampling_window=simulator_routine_revision_config_obj["dataSampling"]["samplingWindow"],
                granularity=simulator_routine_revision_config_obj["dataSampling"]["granularity"],
                validation_window=simulator_routine_revision_config_obj["dataSampling"]["validationWindow"],
            ),
            logical_check=[
                SimulatorRoutineLogicalCheck(
                    aggregate=logical_check["aggregate"],
                    operator=logical_check["operator"],
                    value=logical_check["value"],
                    timeseries_external_id=logical_check["timeseriesExternalId"],
                )
                for logical_check in simulator_routine_revision_config_obj["logicalCheck"]
            ],
            steady_state_detection=[
                SimulatorRoutineSteadyStateDetection(
                    aggregate=steady_state_detection["aggregate"],
                    min_section_size=steady_state_detection["minSectionSize"],
                    var_threshold=steady_state_detection["varThreshold"],
                    slope_threshold=steady_state_detection["slopeThreshold"],
                    timeseries_external_id=steady_state_detection["timeseriesExternalId"],
                )
                for steady_state_detection in simulator_routine_revision_config_obj["steadyStateDetection"]
            ],
            inputs=[
                SimulatorRoutineInputConstant(
                    name=input_cfg["name"],
                    reference_id=input_cfg["referenceId"],
                    value=input_cfg["value"],
                    value_type=input_cfg["valueType"],
                    unit=SimulationValueUnitInput(
                        name=input_cfg["unit"]["name"],
                        quantity=input_cfg["unit"]["quantity"],
                    ),
                    save_timeseries_external_id=input_cfg.get("saveTimeseriesExternalId"),
                )
                for input_cfg in simulator_routine_revision_config_obj["inputs"]
            ],
            outputs=[
                SimulatorRoutineOutput(
                    name=output_cfg["name"],
                    reference_id=output_cfg["referenceId"],
                    unit=SimulationValueUnitInput(
                        name=output_cfg["unit"]["name"],
                        quantity=output_cfg["unit"]["quantity"],
                    ),
                    value_type=output_cfg["valueType"],
                    save_timeseries_external_id=output_cfg.get("saveTimeseriesExternalId"),
                )
                for output_cfg in simulator_routine_revision_config_obj["outputs"]
            ],
        ),
        script=[
            SimulatorRoutineStage(
                order=stage_cfg["order"],
                description=stage_cfg["description"],
                steps=[
                    SimulatorRoutineStep(
                        order=step_cfg["order"],
                        step_type=step_cfg["stepType"],
                        description=step_cfg.get("description"),
                        arguments=SimulatorRoutineStepArguments(step_cfg["arguments"]),
                    )
                    for step_cfg in stage_cfg["steps"]
                ],
            )
            for stage_cfg in simulator_routine_revision_script_obj
        ],
    )
