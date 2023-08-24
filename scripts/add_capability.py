import json
from pathlib import Path

from cognite.client import CogniteClient

TMP_DIR = Path(__file__).resolve().parent / "tmp"


def main(client: CogniteClient):
    new_capabilities = [{"geospatialCrsAcl": {"scope": {"all": {}}, "actions": ["READ", "WRITE"]}}]

    source_id = "4521b63c-b914-44fe-9f86-f42b17fcb6c1"
    name = "integration-test-runner"
    groups = client.iam.groups.list()
    selected_group = next(
        (group for group in groups if group.name == name and group.source_id == source_id and group.capabilities), None
    )

    if not selected_group:
        print(f"Group {name} not found")
        return

    # Store a backup of the group in case you do something stupid.
    TMP_DIR.mkdir(exist_ok=True)
    (TMP_DIR / f"{selected_group.name}.json").write_text(json.dumps(selected_group.dump(camel_case=True), indent=4))

    available_capabilities = {next(iter(capability.keys())) for capability in selected_group.capabilities}
    added = []
    for new_capability in new_capabilities:
        (capability_name,) = new_capability.keys()
        if capability_name not in available_capabilities:
            selected_group.capabilities.append(new_capability)
            added.append(capability_name)
        else:
            print(f"Capability {capability_name} already exists")
    if not added:
        print("All capabilities already exists")
        return
    delete_id = selected_group.id
    selected_group.id = None
    selected_group.is_deleted = None
    selected_group.deleted_time = None
    client.iam.groups.create(selected_group)

    client.iam.groups.delete(delete_id)

    print(f"New capabilities {added} added to group {selected_group.name}")


if __name__ == "__main__":
    # The code for getting a client is not committed, this is to avoid accidental runs.
    from scripts import local_client

    main(local_client.get_interactive())
