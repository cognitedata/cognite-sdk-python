from __future__ import annotations

from argparse import ArgumentParser, Namespace

# NOTE: There should only be built-in imports (or third-party) here!


def parse_arguments() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true")

    # We have two run modes, 'verify' and 'run':
    # - verify: CI or pre-commit hooks
    # - run   : Manual invocations to actually perform the codegen
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("verify")
    run_mode = sub.add_parser("run")

    group = run_mode.add_mutually_exclusive_group(required=True)
    group.add_argument("--all-files", action="store_true")
    group.add_argument("--files", nargs="+", metavar="FILE")

    return parser.parse_args()


def handle_missing_api_import_errors(verify: bool = False) -> None:
    # Future devs will most likely delete a file in order to get it regenerated. This will always
    # end up with a ModuleNotFoundError, so we catch that and proceed with a dummy CogniteClient
    # unless we are in verify mode, in which case it is an obvious failure.
    try:
        from cognite.client import AsyncCogniteClient

        del AsyncCogniteClient
    except ImportError as e:
        if verify:
            print(
                "One or more sync API files are missing. Hint, use one of: \n"
                "$ python scripts/sync_client_codegen/main.py run --all-files\n"
                "$ python scripts/sync_client_codegen/main.py run --files FILE1 FILE2\n"
                f"{'':-<40}\n"
                f"ImportError: {e}"
            )
            raise SystemExit(1)

        if "cognite.client._sync_api" in str(e):
            from scripts.sync_client_codegen.constants import SYNC_CLIENT_PATH

            SYNC_CLIENT_PATH.write_text("class CogniteClient:\n    ...\n")
        else:
            raise

    from cognite.client.config import global_config

    global_config.disable_pypi_version_check = True


if __name__ == "__main__":
    args = parse_arguments()
    match args:
        case Namespace(command="verify"):
            handle_missing_api_import_errors(verify=True)
            from scripts.sync_client_codegen.create_sync_api import run_verify

            run_verify(args)

        case Namespace(command="run", all_files=True):
            handle_missing_api_import_errors()
            from scripts.sync_client_codegen.create_sync_api import run_all_files

            run_all_files(args)

        case Namespace(command="run", files=files):
            handle_missing_api_import_errors()
            from scripts.sync_client_codegen.create_sync_api import run_specific_files

            run_specific_files(args, files)

        case _:
            raise RuntimeError(f"Unknown combination of arguments: {args}")
