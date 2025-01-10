class SimulatorModelWrite(SimulatorModelCore):
    def __init__(
        self,
        external_id: str,
        simulator_external_id: str,
        data_set_id: int,
        name: str,
        type: str,
        description: str | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            simulator_external_id=simulator_external_id,
            data_set_id=data_set_id,
            name=name,
            type=type,
            description=description,
        )

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> SimulatorModelWrite:
        return cls(
            external_id=resource["externalId"],
            simulator_external_id=resource["simulatorExternalId"],
            data_set_id=resource["dataSetId"],
            name=resource["name"],
            type=resource["type"],
            description=resource.get("description"),
        )

    def as_write(self) -> SimulatorModelWrite:
        return self


class SimulatorModelCore(WriteableCogniteResource["SimulatorModelWrite"], ABC):
    """
    The simulator model resource represents an asset modeled in a simulator.
    This asset could range from a pump or well to a complete processing facility or refinery.
    The simulator model is the root of its associated revisions, routines, runs, and results.
    The dataset assigned to a model is inherited by its children. Deleting a model also deletes all its children, thereby
    maintaining the integrity and hierarchy of the simulation data.
    Simulator model revisions track changes and updates to a simulator model over time.
    Each revision ensures that modifications to models are traceable and allows users to understand the evolution of a given model.
    This is the read/response format of a simulator model.
    Args:
        external_id (str): External id of the simulator model
        simulator_external_id (str): External id of the associated simulator
        data_set_id (int): The id of the dataset associated with the simulator model
        name (str): The name of the simulator model
        type (str): The type key of the simulator model
        description (str | None): The description of the simulator model
    """

    def __init__(
        self,
        external_id: str,
        simulator_external_id: str,
        data_set_id: int,
        name: str,
        type: str,
        description: str | None = None,
    ) -> None:
        self.external_id = external_id
        self.simulator_external_id = simulator_external_id
        self.data_set_id = data_set_id
        self.name = name
        self.type = type
        self.description = description

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Self:
        instance = super()._load(resource, cognite_client)
        return instance

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return super().dump(camel_case=camel_case)


class SimulatorModel(SimulatorModelCore):
    """
    The simulator model resource represents an asset modeled in a simulator.
    This asset could range from a pump or well to a complete processing facility or refinery.
    The simulator model is the root of its associated revisions, routines, runs, and results.
    The dataset assigned to a model is inherited by its children. Deleting a model also deletes all its children, thereby
    maintaining the integrity and hierarchy of the simulation data.
    Simulator model revisions track changes and updates to a simulator model over time.
    Each revision ensures that modifications to models are traceable and allows users to understand the evolution of a given model.
    This is the read/response format of a simulator model.
    Args:
        id (int | None): A unique id of a simulator model
        external_id (str | None): External id of the simulator model
        simulator_external_id (str | None): External id of the associated simulator
        data_set_id (int | None): The id of the dataset associated with the simulator model
        name (str | None): The name of the simulator model
        type (str | None): The type key of the simulator model
        description (str | None): The description of the simulator model
        created_time (int | None): The time when the simulator model was created
        last_updated_time (int | None): The time when the simulator model was last updated
    """

    def __init__(
        self,
        id: int | None = None,
        external_id: str | None = None,
        simulator_external_id: str | None = None,
        data_set_id: int | None = None,
        name: str | None = None,
        type: str | None = None,
        description: str | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            simulator_external_id=simulator_external_id,
            data_set_id=data_set_id,
            name=name,
            type=type,
            description=description,
        )
        # id/created_time/last_updated_time are required when using the class to read,
        # but don't make sense passing in when creating a new object. So in order to make the typing
        # correct here (i.e. int and not Optional[int]), we force the type to be int rather than
        # Optional[int].
        self.id: int = id  # type: ignore
        self.created_time: int = created_time  # type: ignore
        self.last_updated_time: int = last_updated_time  # type: ignore