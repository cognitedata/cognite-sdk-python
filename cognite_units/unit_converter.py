import collections
import functools
import json
import re
from typing import Any, List, Tuple, Union

import numpy as np
import pandas as pd

from cognite_units.preprocess import dictionary_preprocess
from cognite_units.show_conversions import (
    show_conversion_base,
    show_conversion_denom,
    show_conversion_final,
    show_conversion_num,
)

with open("dictionaries/unit_type_fundamental.json") as json_file:
    UNIT_TYPE_FUNDAMENTAL = json.load(json_file)
    for k, v in UNIT_TYPE_FUNDAMENTAL.items():
        v = [item.replace("(", "").replace(")", "") for item in v]
        UNIT_TYPE_FUNDAMENTAL[k] = v

with open("dictionaries/unit_conversion.json") as json_file:
    UNIT_CONVERSION = json.load(json_file)

with open("dictionaries/scalers.json") as json_file:
    SCALERS = json.load(json_file)

with open("dictionaries/unit_type_alias.json") as json_file:
    UNIT_TYPE_ALIAS = json.load(json_file)

(
    UNIT_ALIAS_MAP,
    SCALER_ALIAS_MAP,
    FLAT_UNIT_CONVERSION_MAP,
    UNIT_TYPE_MAP,
    UNIT_TYPE_FUNDAMENTAL_MAP,
    UNIT_TYPE_FUNDAMENTAL_FLIP,
    BASE_UNIT_MAP,
    FUNDAMENTAL_UNIT,
) = dictionary_preprocess(UNIT_CONVERSION, UNIT_TYPE_FUNDAMENTAL, SCALERS)


def unit_type_check(unit_type: str) -> Tuple[str, bool]:
    """
    If a unit type is defined, check if it exists in the dictionaries.
    """
    unit_types = [quantity for quantity in UNIT_CONVERSION.keys()]
    alias = [unit_type for unit_type in UNIT_TYPE_ALIAS.keys()]
    unit_types_alias = unit_types + alias
    if (unit_type not in unit_types_alias) and (unit_type is not None):
        raise ValueError(f"Unit type '{unit_type}' does not exist.")
    # if unit_type not in unit_types:
    #     unit_type = FUNDAMENTAL_ALIAS[unit_type]

    res = unit_type in UNIT_TYPE_ALIAS.keys()

    return unit_type, res


def bracket_check(unit):
    """
    Check that the usage of brackets is correct. Brackets only allowed
    in the denominator. Incorrect examples:
    - "(in/s)*s"
    - "(in)/s"

    Brackets required in the denominator when more than 1 unit is in the
    denominator. Incorrect examples:
    - "in/s*s"
    """
    if re.findall("[(]|[)]", unit) == []:
        if "/" in unit:
            if unit.index("/") == 0:
                raise ValueError(
                    f"Invalid unit [{unit}].'/' cannot be at the beginning."
                )
            if unit.index("/") == len(unit) - 1:
                msg = f"Invalid unit [{unit}].'/' cannot be at the end."
                raise ValueError(msg)
            sub_unit = unit.split("/")[-1]
            if "*" in sub_unit:
                raise ValueError(
                    f"Brackets needed in the denominator for unit [{unit}]."
                )
            else:
                return
        else:

            return

    delimiters = re.findall("[(]|[)]|[/]", unit)
    slash_idx = delimiters.index("/")

    if slash_idx == 0:
        return
    else:
        raise ValueError(
            f"Usage of brackets in unit [{unit}] is "
            "incorrect. Brackets are only allowed in "
            "the denominator (after the '/')."
        )


def _contains_number(value: str) -> bool:
    """
    Check if a str contains a number
    """
    for character in value:
        if character.isdigit():
            return True
    return False


def exponent_split(s: str) -> list:
    """
    Splits any instance where a unit has an exponent.
    i.e. 's4' becomes ['s','s','s','s']
    """
    if _contains_number(s):
        head = s.rstrip("0123456789 ")
        tail = s.replace(head, "")
        return np.repeat(head, int(tail)).tolist()
    else:
        return [s]


def multiplier_extract(units, delimiters) -> float:
    """
    Extracts any numeric multipliers that precede a unit.
    i.e. '10' is extracted from '10mile'.
    """
    multipliers = [i.replace(i.lstrip("0123456789.-"), "") for i in units]
    items = [x for y in zip(multipliers, delimiters) for x in y]
    multiply = "".join(items).strip()
    val = [i for i in multiply.split("/")]
    if len(val) > 1:
        num = [float(num) if num != "" else 1.0 for num in val[0].split("*")]
        num_array = np.prod(num)
        den = [float(num) if num != "" else 1.0 for num in val[1].split("*")]
        den_array = np.prod(den)
        multiplier = num_array / den_array
    else:
        multi = [float(num) if num != "" else 1 for num in val[0].split("*")]
        multiplier = np.prod(multi)

    return multiplier


def unit_from_scaler(units: list) -> List[Any]:
    """
    Extracts any unit from a unit that contains a scaler.
    i.e. Extracts 'psi' from 'megapsi'
    """
    lst = []
    for unit in units:
        if unit not in UNIT_ALIAS_MAP.keys():
            for scaler in SCALER_ALIAS_MAP.keys():
                if len(re.split(scaler, unit)) > 1:
                    if re.split(scaler, unit)[1] in UNIT_ALIAS_MAP.keys():
                        lst.append(re.split(scaler, unit)[1])
        else:
            lst.append(unit)
    return lst


def scaler_extract(unit):
    """
    Extracts any scaler from a unit that contains a scaler.
    i.e. Extracts 'kilo' from 'kilolbf'
    """
    num_dem = unit.split("/")
    output = []
    for units in num_dem:
        if "*" in units:
            units = units.split("*")
        else:
            units = [units]

        units = [unit.lstrip("0123456789.- ") for unit in units]

        if any([_contains_number(unit) for unit in units]):
            tmp = []
            for unit in units:
                if _contains_number(unit):
                    tmp.append(exponent_split(unit))
                else:
                    tmp.append([unit])
            units = [unit for elem in tmp for unit in elem]

        scalers = []
        for unit in units:
            if unit not in UNIT_ALIAS_MAP.keys():
                for scaler in SCALER_ALIAS_MAP.keys():
                    if len(re.split(scaler, unit)) > 1:
                        if re.split(scaler, unit)[1] in UNIT_ALIAS_MAP.keys():
                            scalers.append(SCALER_ALIAS_MAP[scaler])

        if scalers == []:
            output.append([1])
        else:
            output.append(scalers)

    if len(output) > 1:
        scaler = np.prod(output[0]) / np.prod(output[1])
    else:
        scaler = np.prod(output[0])
    return scaler


def base_unit(unit_type):
    """
    Takes a unit type and reduces it to a combination of the fundamental unit
    types [mass, distance, time, temperature]. i.e.:
        volume/time --> distance*distance*distance/time
        area*distance/time --> distance*distance*distance/time
        force/area --> mass/distance*time*time
    """
    unit_type = unit_type.split("/")
    unit_type = [i.split("*") for i in unit_type]
    base = list(FUNDAMENTAL_UNIT.values())

    num = [UNIT_TYPE_FUNDAMENTAL_MAP[i] for i in unit_type[0]]
    item1 = [item[0] for item in num]
    item2 = [item[1] for item in num]
    # item1 = [i[0] for i in item1 if i != []]
    item1 = [sub_item for item in item1 for sub_item in item]
    item2 = [sub_item for item in item2 for sub_item in item]
    num = [item1, item2]

    if len(unit_type) > 1:
        denom = [UNIT_TYPE_FUNDAMENTAL_MAP[i] for i in unit_type[1]]
        item1 = [item[0] for item in denom]
        item2 = [item[1] for item in denom]
        item1 = [sub_item for item in item1 for sub_item in item]
        item2 = [sub_item for item in item2 for sub_item in item]
        denom = [item1, item2]
        denom = [denom[1], denom[0]]

        lst = [num[0] + denom[0], num[1] + denom[1]]

        lst = [lst[0].count(i) - lst[1].count(i) for i in base]
        base_unit = [[], []]
        for idx, i in enumerate(lst):
            if i > 0:
                item = [base[idx]] * abs(i)
                base_unit[0] += item
            elif i < 0:
                item = [base[idx]] * abs(i)
                base_unit[1] += item
            else:
                pass

        if base_unit[0] == []:
            base_unit[0] = ["1"]
        base_unit = ["*".join(i) for i in base_unit]
        if base_unit[1] != "":
            base_unit = "/".join(base_unit)
        else:
            base_unit = base_unit[0]

    else:
        lst = [num[0], num[1]]
        lst = [lst[0].count(i) - lst[1].count(i) for i in base]
        base_unit = [[], []]
        for idx, i in enumerate(lst):
            if i > 0:
                item = [base[idx]] * abs(i)
                base_unit[0] += item
            elif i < 0:
                item = [base[idx]] * abs(i)
                base_unit[1] += item
            else:
                pass

        base_unit = ["*".join(i) for i in base_unit]
        if base_unit[-1] != "":
            base_unit = "/".join(base_unit)
        else:
            base_unit = "".join(base_unit)

    return base_unit


def unit_to_unit_type_check(
    unit: str, tmp: list, delimiters: list, unit_type: str
) -> str:
    """
    Converts the units to their base unit types. i.e.:
        acre*ft/d --> distance*distance*distance/time
        N/m*m --> mass/distance*time*time
    """
    lst2 = []
    for item in tmp:
        lst = item.split("*")
        lst = [UNIT_TYPE_MAP[item] for item in lst]
        lst2.append("*".join(lst))

    lst = [x for y in zip(lst2, delimiters) for x in y]
    unit_type_breakdown = "".join(lst).strip()

    if unit_type is not None:
        if unit_type not in UNIT_TYPE_FUNDAMENTAL.keys():
            unit_type = UNIT_TYPE_ALIAS[unit_type]["unit_type_relation"]
        if unit_type_breakdown not in UNIT_TYPE_FUNDAMENTAL[unit_type]:
            raise ValueError(
                f"'{unit}' is not a valid unit of the unit type '{unit_type}'"
            )
        else:
            return base_unit(unit_type_breakdown)
    else:
        return base_unit(unit_type_breakdown)


def res_unit_check(unit: str, unit_type: str):
    """
    Checks if units defined are valid if a restricted unit type is selected
    """
    units = unit.split("/")
    units_list = [i.split("*") for i in units]
    units_res = list(UNIT_TYPE_ALIAS[unit_type]["unit_restriction"].values())
    units_res = list(set([j for i in units_res for j in i]))
    units_flat = list(set([j for i in units_list for j in i]))
    invalid = [i for i in units_flat if i not in units_res]
    if invalid:
        msg = f"Unit(s) {invalid} not valid for unit type '{unit_type}'"
        raise ValueError(msg)

    units_fund = []
    for group in units:
        tmp = []
        for item in group:
            tmp.append(UNIT_TYPE_MAP[item])

        units_fund.append(tmp)

    units_fund_any = ["*".join(item) for item in units_fund]
    units_fund_str = "/".join(units_fund_any)

    res_list = UNIT_TYPE_ALIAS[unit_type]["breakdown_restriction"]
    if units_fund_str not in res_list:
        raise ValueError(
            f"{unit} [{units_fund_str}] not valid for unit type [{unit_type}]"
        )


def preprocess_unit(unit: str, unit_type: str, res: bool) -> list:
    """
    Preprocessing function to ensure input and output units are
    correctly passed into the conversion functions. Steps include:
    - Extract scalers and multipliers from the unit
    - Splits exponents if there are any (i.e s^2 to s*s)
    - Checks if units defined are valid if a restricted unit type is selected
    - Parse unit type into base SI unit types (i.e mass,
        distance, time, temperature)
    """
    bracket_check(unit)

    unit = unit.replace("(", "")
    unit = unit.replace(")", "")
    unit = unit.replace(".", "*")
    unit = unit.replace("-", "*")
    units = re.split(r"/|\*", unit)
    delimiters = re.findall("[*]|[/]", unit) + [" "]
    multiplier = multiplier_extract(units, delimiters)
    scaler = scaler_extract(unit)
    tmp = []
    for unit in units:
        unit = unit.lstrip("0123456789.- ")
        unit_list = exponent_split(unit)
        prior_unit = unit_list
        unit_list = unit_from_scaler(unit_list)
        if unit_list == []:
            msg1 = f"Unit [{prior_unit[0]}] not "
            msg2 = "defined in unit_conversion dictionary."
            msg = msg1 + msg2
            raise ValueError(msg)

        unit_list = [UNIT_ALIAS_MAP[i] for i in unit_list]
        unit = "*".join(unit_list)
        tmp.append(unit)

    unit = "".join([x for y in zip(tmp, delimiters) for x in y]).strip()

    if res:
        print(f"{unit}, {unit_type}")
        res_unit_check(unit, unit_type)

    unit_type_parse = unit_to_unit_type_check(unit, tmp, delimiters, unit_type)
    
    check = base_to_unit_type(unit_type_parse)
    if check not in UNIT_CONVERSION.keys():
        msg = (
            f"Invalid unit type [{check}]. "
            "Unit type not in unit_conversion dictionary."
        )
        raise ValueError(msg)

    return [unit, multiplier, scaler, unit_type_parse]


def base_to_unit_type(dimension: str) -> str:
    """
    Converts a unit type that only consists of base unit
    types [mass, distance, time, temperature] and checks if it is a
    breakdown of a non-base unit type. i.e.
        distance*distance*distance/time --> volumetric_flowrate
        mass/distance*time*time --> pressure
        mass/distance*distance*distance --> density

    If the combination is not a breakdown of a non-base unit type, then
    it just returns itself.
    """
    lst = dimension.split("/")
    lst2 = [item.split("*") for item in lst]
    if len(lst2) == 1:
        lst2.append([])
    for k, v in UNIT_TYPE_FUNDAMENTAL_MAP.items():
        check = []
        for idx, i in enumerate(v):
            if collections.Counter(i) == collections.Counter(lst2[idx]):
                check.append(True)
            else:
                check.append(False)

        if all(check):
            return k

    else:
        return dimension


def inverse_linear(m, c) -> tuple:
    """
    Inverse of input linear function
    """
    return 1 / m, -c / m


def reduce_transformations(transformations) -> tuple:
    """
    Reduces series of univariate linear transformations
    to single transformation.
    """
    m, c = functools.reduce(
        lambda val, t: (val[0] * t[0], t[0] * val[1] + t[1]), transformations
    )

    return m, c


def base_conversion_factor(
    unit: str, show_conversions: bool, items: List[Any]
) -> tuple:
    """
    Compute conversion factors for input unit to base unit.
    Conversion factors from base unit to input unit can be
    computed by taking inverse of result of this function.

    """
    in_out = items[1]
    unit_dimension = items[0]
    if show_conversions:
        re_map = dict((v, k) for k, v in FUNDAMENTAL_UNIT.items())
        base_str = unit_dimension
        for k, v in re_map.items():
            base_str = base_str.replace(k, v)
        print(f"-----unit [{unit}] to base unit [{base_str}]-----")

    if re.search(".+/.+", unit):
        num_group = re.search("^[^/]*", unit)
        if num_group:
            nums = num_group.group().split("*")

        denom_group = re.search("/.*$", unit)
        if denom_group:
            denoms = denom_group.group()[1:].split("*")
    else:
        nums = unit.split("*")
        denoms = []

    transformations = []
    for num in nums:
        # todo : can plugin section to handle scalars in this place
        if num in FLAT_UNIT_CONVERSION_MAP:
            base_unit_dict = FLAT_UNIT_CONVERSION_MAP[num]
            # todo : need to standardize the way we
            # define conversion factors (multiplier and shift)
            m = float(base_unit_dict["multiplier"])
            if unit_dimension in ["temperature", "mass/distance*time*time"]:
                c = float(base_unit_dict.get("shift", 0))
            else:
                c = 0
            transformations.append(inverse_linear(m, c))

    show_transformations = [[], [], [], []]  # type: List[list]
    if show_conversions and nums != []:
        show_conversion_num(
            nums,
            show_transformations,
            unit_dimension,
            FLAT_UNIT_CONVERSION_MAP,
            BASE_UNIT_MAP,
        )

    for denom in denoms:
        if denom in FLAT_UNIT_CONVERSION_MAP:
            base_unit_dict = FLAT_UNIT_CONVERSION_MAP[denom]
            m = float(base_unit_dict["multiplier"])
            if unit_dimension in ["temperature", "mass/distance*time*time"]:
                c = float(base_unit_dict.get("shift", 0))
            else:
                c = 0
            transformations.append((m, c))

    if show_conversions and denoms != []:
        show_conversion_denom(
            denoms,
            show_transformations,
            unit_dimension,
            m,
            num,
            FLAT_UNIT_CONVERSION_MAP,
            BASE_UNIT_MAP,
        )

    m, c = reduce_transformations(transformations)

    if show_conversions:
        show_conversion_base(unit_dimension, show_transformations, in_out, m)

    return m, c


def unit_convert(
    value: Union[pd.Series, float, int],
    input_unit: str,
    output_unit: str,
    unit_type=None,
    show_conversions=False,
    unit_aware=False,
) -> pd.Series:
    """Unit Converter

    This unit converter is based on a decomposition of the input
    unit and output unit and the conversion is created based on
    fundamental relationships of the unit type. The types of conversions
    that can be made is limited to the unit types in the
    unit_type_fundamental.json dictionary and limited to the
    conversions in the unit_conversion.json dictionary.
    The type of the conversions that can be made is can be any
    combination that is valid based on the fundamental relationships
    of the unit_type.

    Args:
        value (Union[pd.Series, float, int]): Input values.
            Numeric input of values to convert.
        input_unit (str): Input unit.
            Input unit of the values.
        output_unit (str): Output unit.
            Desired output unit of values.
        unit_type (str): Unit type.
            Unit type (i.e. pressure, temperature, mass).
        show_conversions (bool): Show conversion process.
            Outputs report to show the entire conversion process.
        unit_aware (bool): Changes output type.
            If unit_aware is `True`, then output will be a list of
            `[value, output_unit]`. If False, output will only be `value`

    Returns:
        Union[pd.Series, float, int]: Output values.
            Output values after conversion based on output unit.
    """
    logic1 = not isinstance(value, float)
    logic2 = not isinstance(value, int)
    if logic1 and logic2 and show_conversions:
        msg = "Cannot show conversion if value is not an int or float."
        raise ValueError(msg)

    unit_type, res = unit_type_check(unit_type)

    if unit_type == "-":
        return value

    if show_conversions:
        print(f"--CONVERSION from [{input_unit}] to [{output_unit}]--\n")

    in_items = preprocess_unit(input_unit, unit_type, res)
    (in_unit, input_multiplier, input_scaler, in_dimension) = in_items

    out_items = preprocess_unit(output_unit, unit_type, res)
    (out_unit, output_multiplier, output_scaler, out_dimension) = out_items

    if in_dimension != out_dimension:
        in_dimension = base_to_unit_type(in_dimension)
        out_dimension = base_to_unit_type(out_dimension)
        raise ValueError(
            f"Input unit type [{in_dimension}] is not "
            f"equal to output unit type [{out_dimension}]"
        )

    in_items = [in_dimension, "in"]
    out_items = [out_dimension, "out"]
    item1 = base_conversion_factor(in_unit, show_conversions, in_items)
    item2 = base_conversion_factor(out_unit, show_conversions, out_items)
    transformations = [
        # get conversion factor from in_unit to base unit
        item1,
        # get conversion factor from base unit to out_unit
        inverse_linear(*item2),
    ]

    m, c = reduce_transformations(transformations)

    if not isinstance(value, float):
        value = np.array(value)

    temp = m * value + c

    if show_conversions:
        show_conversion_final(
            value,
            transformations,
            temp,
            input_multiplier,
            input_scaler,
            output_multiplier,
            output_scaler,
        )

    if not unit_aware:
        in_terms = input_multiplier * input_scaler
        out_terms = output_multiplier * output_scaler
        return in_terms * temp / out_terms

    else:
        in_terms = input_multiplier * input_scaler
        out_terms = output_multiplier * output_scaler
        return [in_terms * temp / out_terms, output_unit]
