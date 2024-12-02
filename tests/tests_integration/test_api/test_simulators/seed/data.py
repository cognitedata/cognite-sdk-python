simulator = {
    "name": "DWSIM",
    "externalId": "integration_tests_workflow",
    "fileExtensionTypes": ["dwxmz"],
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
    ],
}


simulator_integration = {
    "externalId": "integration_tests_workflow_connector",
    "simulatorExternalId": "integration_tests_workflow",
    "heartbeat": 1706396950969,
    "dataSetId": 97552494921583,
    "connectorVersion": "1.0.0",
    "simulatorVersion": "1.0.0",
    "licenseStatus": "AVAILABLE",
    "licenseLastCheckedTime": 0,
    "connectorStatus": "IDLE",
    "connectorStatusUpdatedTime": 0,
}


simulator_model = {
    "externalId": "integration_tests_workflow_model",
    "simulatorExternalId": "integration_tests_workflow",
    "name": "Test Simulator Model",
    "description": "Test Simulator Model Desc",
    "dataSetId": 97552494921583,
    "labels": [{"externalId": "simconfig-labels-PROSPER"}],
    "type": "SteadyState",
}

simulator_model_revision = {
    "externalId": "integration_tests_workflow_model_revision",
    "modelExternalId": "integration_tests_workflow_model",
    "description": "test sim model revision description",
    "fileId": 00000000000000,
}

simulator_routine = {
    "externalId": "integration_tests_workflow_routine",
    "modelExternalId": "integration_tests_workflow_model",
    "simulatorIntegrationExternalId": "integration_tests_workflow_connector",
    "name": "Routine test",
    "description": "test",
}

simulator_routine_revision = {
    "externalId": "integration_tests_workflow_routine_revision",
    "routineExternalId": "integration_tests_workflow_routine",
    "configuration": {
        "schedule": {"enabled": True, "cronExpression": "*/10 * * * *"},
        "dataSampling": {"enabled": True, "validationWindow": None, "samplingWindow": 15, "granularity": 1},
        "logicalCheck": [],
        "steadyStateDetection": [],
        "inputs": [
            {
                "name": "Cold Water Temperature - Constant",
                "referenceId": "CWTC",
                "value": 10,
                "valueType": "DOUBLE",
                "unit": {"name": "C", "quantity": "temperature"},
                "saveTimeseriesExternalId": "DWSIM-INPUT-constinput-CWTC-ShowerMixer-AI",
            },
            {
                "name": "Cold Water Pressure - Constant",
                "referenceId": "CWPC",
                "value": 3.6,
                "valueType": "DOUBLE",
                "unit": {"name": "bar", "quantity": "pressure"},
                "saveTimeseriesExternalId": "DWSIM-INPUT-constinput-CWPC-ShowerMixer",
            },
            {
                "name": "Cold Water Volumetric Flow - Constant",
                "referenceId": "CWVFC",
                "value": 0.37,
                "valueType": "DOUBLE",
                "unit": {"name": "m3/h", "quantity": "volumetricFlow"},
                "saveTimeseriesExternalId": "DWSIM-INPUT-constinput-CWVFC-ShowerMixer-AI",
            },
            {
                "name": "Hot Water Temperature - Constant",
                "referenceId": "HWTC",
                "value": 69,
                "valueType": "DOUBLE",
                "unit": {"name": "C", "quantity": "temperature"},
                "saveTimeseriesExternalId": "DWSIM-INPUT-constinput-HWTC-ShowerMixer-AI",
            },
            {
                "name": "Hot Water Pressure - Constant",
                "referenceId": "HWPC",
                "value": 2.8,
                "valueType": "DOUBLE",
                "unit": {"name": "bar", "quantity": "pressure"},
                "saveTimeseriesExternalId": "DWSIM-INPUT-constinput-HWPC-ShowerMixer-AI",
            },
            {
                "name": "Hot Water Volumetric Flow - Constant",
                "referenceId": "HWVFC",
                "value": 0.19,
                "valueType": "DOUBLE",
                "unit": {"name": "m3/h", "quantity": "volumetricFlow"},
                "saveTimeseriesExternalId": "DWSIM-INPUT-constinput-HWVFC-ShowerMixer-AI",
            },
        ],
        "outputs": [
            {
                "name": "Shower Temperature",
                "referenceId": "ST",
                "unit": {"name": "C", "quantity": "temperature"},
                "valueType": "DOUBLE",
                "saveTimeseriesExternalId": "DWSIM-OUTPUT-const_input-ST-ShowerMixer-AI",
            },
            {
                "name": "Shower Pressure",
                "referenceId": "SP",
                "unit": {"name": "bar", "quantity": "pressure"},
                "valueType": "DOUBLE",
                "saveTimeseriesExternalId": "DWSIM-OUTPUT-const_input-SP-ShowerMixer-AI",
            },
            {
                "name": "Shower Volumetric Flow",
                "referenceId": "SVF",
                "unit": {"name": "m3/h", "quantity": "volumetricFlow"},
                "valueType": "DOUBLE",
                "saveTimeseriesExternalId": "DWSIM-OUTPUT-const_input-SVF-ShowerMixer-AI",
            },
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
                    "arguments": {"referenceId": "CWTC", "objectName": "Cold water", "objectProperty": "Temperature"},
                },
                {
                    "order": 2,
                    "stepType": "Set",
                    "arguments": {"referenceId": "CWPC", "objectName": "Cold water", "objectProperty": "Pressure"},
                },
                {
                    "order": 3,
                    "stepType": "Set",
                    "arguments": {
                        "referenceId": "CWVFC",
                        "objectName": "Cold water",
                        "objectProperty": "Volumetric Flow",
                    },
                },
                {
                    "order": 4,
                    "stepType": "Set",
                    "arguments": {"referenceId": "HWTC", "objectName": "Hot water", "objectProperty": "Temperature"},
                },
                {
                    "order": 5,
                    "stepType": "Set",
                    "arguments": {"referenceId": "HWPC", "objectName": "Hot water", "objectProperty": "Pressure"},
                },
                {
                    "order": 6,
                    "stepType": "Set",
                    "arguments": {
                        "referenceId": "HWVFC",
                        "objectName": "Hot water",
                        "objectProperty": "Volumetric Flow",
                    },
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
                {
                    "order": 3,
                    "stepType": "Get",
                    "arguments": {"referenceId": "SVF", "objectName": "Shower", "objectProperty": "Volumetric Flow"},
                },
            ],
        },
    ],
}
