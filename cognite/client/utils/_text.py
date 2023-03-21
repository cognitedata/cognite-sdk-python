from typing import Any


class DrawTables:
    HLINE = "\N{box drawings light horizontal}"
    VLINE = "\N{box drawings light vertical}"
    XLINE = "\N{box drawings light vertical and horizontal}"
    TOPLINE = "\N{box drawings light down and horizontal}"


def shorten(obj: Any, width: int = 20, placeholder: str = "...") -> str:
    # 'textwrap.shorten' skips entire words... so we make our own:
    if width < (n := len(placeholder)):
        raise ValueError("Width must be larger than or equal to the length of 'placeholder'")

    s = obj if isinstance(obj, str) else repr(obj)
    if len(s) <= width:
        return s
    return f"{s[:width-n]}{placeholder}"
