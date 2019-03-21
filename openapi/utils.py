import re

FLAT_TYPE_MAPPING = {
    "string": "str",
    "integer": "int",
    "number": "float",
    "boolean": "bool",
    None: "None",
    "None": "None",
}


def truncate_description(description):
    max_len = 80
    lines = []
    line = []
    for word in description.split():
        line_so_far = " ".join(line)
        if len(line_so_far) >= max_len:
            lines.append(line_so_far)
            line = []
        line.append(word)
    lines.append(" ".join(line))
    return "\n".join(lines)


def get_python_type(open_api_type):
    return FLAT_TYPE_MAPPING[open_api_type]


def to_snake_case(str):
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", str)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def get_full_type_hint(python_type, class_segments):
    if re.match("List", python_type):
        for seg in class_segments:
            if re.search(seg.schema_name, python_type):
                return re.sub(seg.schema_name, seg.class_name, python_type)
        for type in FLAT_TYPE_MAPPING.values():
            if re.search(type, python_type):
                return python_type
        return "List[Dict[str, Any]]"
    for type in FLAT_TYPE_MAPPING.values():
        if re.match(type, python_type):
            return python_type
    return "Dict[str, Any]"
