import re


def get_uri(description: str) -> str | None:
    """Extracts the URI from a description string.

    Args:
        description: Description string.

    Returns:
        URI string.

    !!! note
        This function is used with DMS schema components interrogation. This is temporal
        solution until attribute `uri` is added to the schema components.
    """

    if uri := re.search(r"@uri=(\S+)", description):
        return uri.group(1)
    else:
        return None
