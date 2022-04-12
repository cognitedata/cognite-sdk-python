def show_conversion_base(unit_dim, show_trans, in_out, m):
    """
    Show the final/reduced 'm' and 'c' values from a series of 'm' and 'c'
    values needed for the conversion process from the unit to the base unit.
    """
    res_unit_types = ["temperature", "mass/distance*time*time"]
    if unit_dim in res_unit_types:  # this logic may beed to be changed

        tmp = []
        for item in show_trans:
            tmp.append([f"{val:.3}" for val in item])
        item1 = " * ".join(tmp[0])
        item2 = " * ".join(tmp[1])
        if tmp[1] == []:
            print(f"m_{in_out} = {item1}")
        else:
            print(f"m_{in_out} = {item1} / {item2}")
        if show_trans[2] == []:
            print(f"c_{in_out} = 0\n")
        else:
            print(f"c_{in_out} = {show_trans[2][0]:.3}\n")
    else:
        tmp = []
        for item in show_trans:
            tmp.append([f"{val:.3}" for val in item])
        item1 = " * ".join(tmp[0])
        item2 = " * ".join(tmp[1])
        if tmp[1] == []:
            print(f"m_{in_out} = {item1}")
        else:
            print(f"m_{in_out} = {item1} / {item2}")
        print(f"m_{in_out} = {m:.3}\n")


def show_conversion_num(
    nums, show_trans, unit_dim, FLAT_UNIT_CONVERSION_MAP, BASE_UNIT_MAP
):
    """
    Shows the conversion process from the unit to the
    base unit of the numerator.
    i.e. from acre*ft/d to m*m*m/s the conversion
    shown is from acre*ft to m*m*m
    The conversion steps for numerator and denominator
    are different.
    """
    print("Numerator")
    for num in nums:
        m = float(FLAT_UNIT_CONVERSION_MAP[num]["multiplier"])
        if ("shift" in FLAT_UNIT_CONVERSION_MAP[num].keys()) and (
            unit_dim in ["temperature", "mass/distance*time*time"]
        ):
            c = float(FLAT_UNIT_CONVERSION_MAP[num]["shift"])
            show_trans[0].append(1 / m)
            show_trans[2].append(c)
            print(
                f"{num} --> {BASE_UNIT_MAP[num]}"
                f" (multiplier = {1/m:.3}, shift = {c:.3})"
            )
        else:
            show_trans[0].append(1 / m)
            print(f"{num} --> {BASE_UNIT_MAP[num]} (multiplier = {1/m:.3})")
    print("   ")


def show_conversion_denom(
    denoms,
    show_trans,
    unit_dim,
    m,
    num,
    FLAT_UNIT_CONVERSION_MAP,
    BASE_UNIT_MAP,
):
    """
    Shows the conversion process from the unit to
    the base unit of the denominator. i.e. from acre*ft/d
    to m*m*m/s the conversion shown is from d to s
    The conversion steps for numerator and denominator
    are different.
    """
    print("Denominator")
    for den in denoms:
        if ("shift" in FLAT_UNIT_CONVERSION_MAP[num].keys()) and (
            unit_dim in ["temperature", "mass/distance*time*time"]
        ):
            c = float(FLAT_UNIT_CONVERSION_MAP[den]["shift"])
            show_trans[1].append(1 / m)
            show_trans[2].append(c)
            print(
                f"{den} --> {BASE_UNIT_MAP[den]} "
                f"(multiplier = {1/m:.3}, shift = {c:.3})"
            )
        else:
            show_trans[1].append(1 / m)
            print(f"{den} --> {BASE_UNIT_MAP[den]} (multiplier = {1/m:.3})")
    print("   ")


def show_conversion_final(
    value,
    transformations,
    temp,
    input_multiplier,
    input_scaler,
    output_multiplier,
    output_scaler,
):
    """
    Show the conversion steps from input unit to
    output unit given that 'm_in', 'm_out', 'c_in'
    and 'c_out' values are obtained from the previous
    steps. This function also shows how the input and
    output multipliers and scalers are factored into
    the conversion process.
    """
    print("------Final Conversions------\n")
    print("output_value = ((input_value - c_in) * m_in) / m_out + c_out")
    print(
        f"output_value = (({value} - "
        f"{-transformations[0][1]/transformations[0][0]:.3})"
        f" * {transformations[0][0]:.3}) / {1/transformations[1][0]:.3}"
        f" + {transformations[1][1]:.3}"
    )
    print(f"output_value = {temp:.3}\n")
    print(
        "output_value = input_multiplier * input_scaler * output_value"
        " / (output_multiplier * output_scaler)"
    )
    print(
        f"output_value = {input_multiplier} * {input_scaler} * {temp:.3} "
        f"/ ({output_multiplier} * {output_scaler})\n"
    )
