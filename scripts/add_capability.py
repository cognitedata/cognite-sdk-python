import json
from pathlib import Path

from cognite.client import CogniteClient
from cognite.client.data_classes.capabilities import Capability

TMP_DIR = Path(__file__).resolve().parent / "tmp"


def main(client: CogniteClient):
    new_capabilities = [
        {
            "hostedExtractorsAcl": {
                "actions": ["READ", "WRITE"],
                "scope": {"all": {}},
            }
        },
    ]

    source_id = "4521b63c-b914-44fe-9f86-f42b17fcb6c1"
    name = "integration-test-runner"
    groups = client.iam.groups.list()
    selected_group = next(
        (group for group in groups if group.name == name and group.source_id == source_id and group.capabilities),
        None,
    )

    if not selected_group:
        print(f"Group {name} not found")
        return

    # Store a backup of the group in case you do something stupid.
    TMP_DIR.mkdir(exist_ok=True)
    (TMP_DIR / f"{selected_group.name}.json").write_text(json.dumps(selected_group.dump(camel_case=True), indent=4))

    existing_capability_by_name = {
        capability._capability_name: capability for capability in selected_group.capabilities
    }
    added = []
    for new_capability in new_capabilities:
        (capability_name,) = new_capability.keys()
        if capability_name not in existing_capability_by_name:
            selected_group.capabilities.append(Capability.load(new_capability))
            added.append(capability_name)
        elif new_capability[capability_name] != existing_capability_by_name[capability_name]:
            # Capability exists, but with different scope or actions
            raise NotImplementedError()
            # selected_group.capabilities.append(new_capability)
            # added.append(capability_name)
        else:
            print(f"Capability {capability_name} already exists")
    if not added:
        print("All capabilities already exists")
        return
    delete_id = selected_group.id
    selected_group_write = selected_group.as_write()
    client.iam.groups.create(selected_group_write)

    client.iam.groups.delete(delete_id)

    print(f"New capabilities {added} added to group {selected_group.name}")


if __name__ == "__main__":
    # The code for getting a client is not committed, this is to avoid accidental runs.
    from scripts import local_client

    main(local_client.get_interactive())
