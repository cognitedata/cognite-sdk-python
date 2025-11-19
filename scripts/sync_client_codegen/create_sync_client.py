from pathlib import Path
from tempfile import NamedTemporaryFile

from scripts.sync_client_codegen.codegen_utils import (
    filter_base_apis_and_sort_alphabetically,
    foolish_cls_name_rewrite,
    get_canonical_source,
    path_as_importable,
    run_ruff,
)
from scripts.sync_client_codegen.constants import (
    ASYNC_API_DIR,
    SYNC_API_DIR,
    SYNC_CLIENT_PATH,
)

COGNITE_CLIENT_TEMPLATE = Path("scripts/sync_client_codegen/sync_client_template.txt").read_text()


def create_sync_cognite_client(
    dot_path_lookup: dict[str, str],
    file_path_lookup: dict[str, str],
) -> str:
    all_apis = []
    all_imports = []
    for api, attr in filter_base_apis_and_sort_alphabetically(dot_path_lookup):
        override_api_name = foolish_cls_name_rewrite(api)
        all_apis.append(f"self.{attr} = Sync{override_api_name}(async_client)\n")

        import_path = path_as_importable(
            SYNC_API_DIR / Path(file_path_lookup[api]).relative_to(ASYNC_API_DIR.resolve())
        ).replace(".__init__", "")
        all_imports.append(f"from {import_path} import Sync{override_api_name}")

    return COGNITE_CLIENT_TEMPLATE.format(
        all_api_imports="\n".join(all_imports),
        nested_apis_init="        ".join(all_apis).rstrip(),
    )


def verify_cognite_client_is_up_to_date(new_source: str) -> bool:
    with NamedTemporaryFile(mode="w+") as f:
        f.write(new_source)
        path = Path(f.name)
        run_ruff([path], verbose=False)
        new_file_ast = get_canonical_source(path)

    current_ast = get_canonical_source(SYNC_CLIENT_PATH)
    return current_ast == new_file_ast
