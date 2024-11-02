from cognite.client.data_classes.data_modeling import View


def make_test_view(space: str, external_id: str, version: str | None, created_time: int = 1):
    return View(
        space,
        external_id,
        version,
        created_time=created_time,
        properties={},
        last_updated_time=2,
        description="",
        name="",
        filter=None,
        implements=None,
        writable=False,
        used_for="all",
        is_global=False,
    )
