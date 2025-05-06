"""This script requires pygen to be installed. It generates typed classes for the core data model v1.

`pip install cognite-pygen==1.2.5`

"""

from pathlib import Path

from dotenv import load_dotenv

from cognite.pygen._generator import generate_typed
from tests.tests_integration.conftest import make_cognite_client

THIS_REPO = Path(__file__).resolve().parent.parent

OUTPUT_FILE = THIS_REPO / "cognite" / "client" / "data_classes" / "data_modeling" / "extractor_extension" / "v1.py"
load_dotenv(THIS_REPO / ".env")


def main() -> None:
    client = make_cognite_client(beta=False)

    typed_classes = generate_typed(
        ("cdf_extraction_extensions", "CogniteExtractorExtensions", "v1"),
        None,
        client,
        format_code=False,
        module_by_space={"cdf_cdm": "cognite.client.data_classes.cdm.v1"},
    )

    with OUTPUT_FILE.open("w", encoding="utf-8", newline="\n") as f:
        f.write(typed_classes)


if __name__ == "__main__":
    main()
