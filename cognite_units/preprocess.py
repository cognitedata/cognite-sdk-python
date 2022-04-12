import itertools


def _fundamental_permutations(fundamentals: list) -> list:
    """
    This takes in the unit_type_fundamental dictionary and creates
    all the permutations a value in the dictionary can be shown. For
    example, one entry is "force": ["mass*distance/(time*time)",
    "mass*acceleration","force"]. The items in force can be shown as
    different permutations. i.e. "mass*accelaration" can also be
    "acceleration*mass" and "mass*distance/(time*time)" can also be
    "distance*mass/(time*time)".
    """
    new_lst = []
    for item in fundamentals:
        lst = item.split("/")
        lst = [item.split("*") for item in lst]
        lst = [list(set(itertools.permutations(i))) for i in lst]
        lst3 = []
        for i in lst:
            lst2 = []
            for j in i:
                lst2.append("*".join(j))
            lst3.append(lst2)

        if len(lst3) == 2:
            lst = list(itertools.product(lst3[0], lst3[1]))
            new_lst.append(["/".join(i) for i in lst])
        else:
            new_lst.append([j for i in lst3 for j in i])

    return [elem for item in new_lst for elem in item]


def dictionary_preprocess(
    UNIT_CONVERSION: dict, UNIT_TYPE_FUNDAMENTAL: dict, SCALERS: dict
) -> list:
    """
    The input to these functions are the three dictionaries. For some of the
    functions downstream, the dictionary has to be reconfigured and other
    dictionaries are made. The reason for the reconfigure is that
    the input dictionaries are nested to many levels and it is hard to
    navigate them as they are.
    """
    all_items = []
    unit_dict = {}
    for k, v in UNIT_CONVERSION.items():
        for k1, v1 in v["units"].items():
            unit_dict[k1] = k1
            if "aliases" in v["units"][k1].keys():
                for item in v["units"][k1]["aliases"]:
                    unit_dict[item] = k1

    all_items.append(unit_dict)

    scalers_dict = {}
    for k, v in SCALERS.items():
        scalers_dict[k] = v["scale factor"]
        if "aliases" in SCALERS[k].keys():
            for item in SCALERS[k]["aliases"]:
                scalers_dict[item] = v["scale factor"]

    all_items.append(scalers_dict)

    for key, val in UNIT_TYPE_FUNDAMENTAL.items():
        UNIT_TYPE_FUNDAMENTAL[key] = _fundamental_permutations(val)

    FLAT_UNIT_CONVERSION_MAP = {}
    for k, v in UNIT_CONVERSION.items():
        FLAT_UNIT_CONVERSION_MAP.update(v["units"].items())

    all_items.append(FLAT_UNIT_CONVERSION_MAP)

    UNIT_TYPE_MAP = {}
    for k, v in UNIT_CONVERSION.items():
        for k1, v1 in v["units"].items():
            UNIT_TYPE_MAP[k1] = k
            if "aliases" in v["units"][k1].keys():
                for item in v["units"][k1]["aliases"]:
                    UNIT_TYPE_MAP[item] = k

    all_items.append(UNIT_TYPE_MAP)

    UNIT_TYPE_FUNDAMENTAL_MAP = {}
    for k, v in UNIT_TYPE_FUNDAMENTAL.items():
        lst = v[0].split("/")
        lst = [i.split("*") for i in lst]
        UNIT_TYPE_FUNDAMENTAL_MAP[k] = lst

    for k, v in UNIT_TYPE_FUNDAMENTAL_MAP.items():
        if len(v) == 1:
            UNIT_TYPE_FUNDAMENTAL_MAP[k] = [v[0], []]

    all_items.append(UNIT_TYPE_FUNDAMENTAL_MAP)

    UNIT_TYPE_FUNDAMENTAL_FLIP = {}
    for k, v in UNIT_TYPE_FUNDAMENTAL.items():
        UNIT_TYPE_FUNDAMENTAL_FLIP[v[0]] = k

    all_items.append(UNIT_TYPE_FUNDAMENTAL_FLIP)

    BASE_UNIT_MAP = {}
    for k, v in UNIT_CONVERSION.items():
        for k1 in v["units"].keys():
            BASE_UNIT_MAP[k1] = v["base_unit"]

    all_items.append(BASE_UNIT_MAP)

    FUNDAMENTAL_UNIT = {}
    for k, v in UNIT_CONVERSION.items():
        if "fundamental" in v.keys():
            FUNDAMENTAL_UNIT[v["base_unit"]] = k

    all_items.append(FUNDAMENTAL_UNIT)

    return all_items
