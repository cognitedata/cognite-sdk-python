"""This script requires pygen to be installed. It generates typed classes for the core data model v1.

`pip install cognite-pygen==0.99.34`

Note that `pygen` requires `Python 3.9` or later`, so if you develop in an older version of Python,
you need to run this script in a Python 3.9 environment.
"""

from pathlib import Path

from dotenv import load_dotenv

from cognite.pygen._generator import generate_typed
from tests.tests_integration.conftest import make_cognite_client

THIS_REPO = Path(__file__).resolve().parent.parent

OUTPUT_FILE = THIS_REPO / "cognite" / "client" / "data_classes" / "data_modeling" / "cdm" / "v1.py"
load_dotenv(THIS_REPO / ".env")


def main() -> None:
    client = make_cognite_client(beta=False)

    typed_classes = generate_typed(("cdf_cdm", "CogniteCore", "v1"), None, client, format_code=True)

    # For some reason, mypy doesn't need the ignore in one location.
    typed_classes = typed_classes.replace(
        "time_series=self.time_series,  # type: ignore[arg-type]", "time_series=self.time_series,"
    )

    with OUTPUT_FILE.open("w", encoding="utf-8", newline="\n") as f:
        f.write(typed_classes)


if __name__ == "__main__":
    main()
