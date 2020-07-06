import re

TYPE_MAPPING = {
    "string": "str",
    "integer": "int",
    "number": "float",
    "boolean": "bool",
    None: "None",
    "None": "None",
    "array": "List",
    "object": "Dict[str, Any]",
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


def get_type_hint(item):
    if hasattr(item, "type"):
        type = item.type
    elif "type" in item:
        type = item["type"]
    elif "oneOf" in item and "type" in item["oneOf"][0]:
        types = []
        for current in item["oneOf"]:
            types.append(get_type_hint(current))
        types = list(set(types))
        if len(types) == 1:
            return types[0]
        else:
            return "Union[{}]".format(", ".join(types))
    elif "allOf" in item and any("type" in it for it in item["allOf"]):
        types = []
        for current in item["allOf"]:
            if "type" in current:
                types.append(get_type_hint(current))
        types = list(set(types))
        if len(types) == 1:
            return types[0]
        else:
            return "Union[{}]".format(", ".join(types))
    else:
        raise Exception("Unrecognized type in '{}'".format(item))

    if type == "array":
        return "List[{}]".format(get_type_hint(item["items"]))
    elif type == "object":
        return "Dict[str, Any]"
    elif type in TYPE_MAPPING:
        return TYPE_MAPPING[type]
    raise Exception("Unrecognized type '{}'".format(type))


def to_snake_case(str):
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", str)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()
