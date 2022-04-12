# Short Description: Units Dictionaries

There are 4 dictionaries in this repository:

- `unit_conversion.json`
- `scaler.json`
- `unit_type_alias.json`
- `unit_type_fundamental.json`

The `unit_conversion.json` dictionary is the dictionary that contains the conversions from a unit to the `base_unit` for each of the unit types. The `base_unit` is a base SI unit for a given unit type (i.e. the base unit for `pressure` is `kg/(m*s*s)`. For every new unit added to the dictionary, there should be an entry for the `multiplier` and a `description` of that unit and a `shift` if it is necessary in the conversion. The exact way to contribute to this dictionary is described further in the contributing guide.

The `scaler.json` dictionary is the dictionary that contains the scalers that are commonly used as a prefix for units. For example the scaler/prefix `mega` indicates a scaler numeric value of `1000`. The `scaler.json` dictionary is not allowed to be modified.

The `unit_type_alias.json` dictionary is used to add unit types that can cater for conversions between a restricted set of units. For example, `gas_volumetric_flowrate` should only have volume units `scf`, `ft3` and not include volume units `bbl` or `in3`. The exact way to contribute to this dictionary is described further in the contributing guide.

The `unit_type_fundamental.json` is not a dictionary that is used by the unit converter but is utilized as a useful guide to understand how unit types can be broken down into other unit types. This list of breakdowns for a given unit type helps the user in formulating unique combination of units. The exact way to contribute to this dictionary is described further in the contributing guide.

=======
# Unit Converter Dictionaries

Before diving into how unit conversion is handled in InDSL, it is necessary define the terms and basic logic it all
revolves around:

**Unit type**: the physical unit assigned to a time series. Typical unit types are `pressure`,
`temperature`, `length`, `volumetric flow-rate` and so on.

**Unit of measure (UoM)**: the unit, from any particular system (e.g., International System or SI), assigned to the time
series. For instance length can be measured in UoM meters (`m`) or feet (`ft`).

**Base unit of measure**: the reference unit of measure from the Internal System of Units (SI). All other units of
measure are converted using conversion factors relative to the base unit of measure.

Unit conversion is based on the following basic operation:

<img src="https://render.githubusercontent.com/render/math?math=y = A*x + B,">

where `y` is the target UoM and `x` is the base unit of measure. `A` and `-B` are the conversion factors, which are, for
programmatic reasons, named `multiplier` and `shift`, respectively. Also note that the shift is defined as negative
`B`. This is due to the algorithm being based on referencing all conversions to the base unit of measure. Therefore,
when solving for it one obtains :math:`x = (y-B)/A`.

## Dictionaries

The conversion toolbox and algorithms are structured around four JSON dictionaries:

    * Fundamental units: `unit_type_fundamental.json`
    * Unit types, UoM, conversion factors: `unit_conversion.json`
    * Scalers: `scalers.json`
    * Unit type aliases: `unit_type_alias.json`

### Fundamental units

The `unit_type_fundamental.json` dictionary contains all the supported **fundamental** units types supported and
expressed as a combination of other fundamental unit types. For example, `mass` is a fundamental unit type that can only
be defined by itself, and `pressure` is a fundamental unit type that can be expressed as multiple combinations of other
runit types. This is represented in the dictionary as follows:

    {
        "mass": [
            "mass"
            ],
        "pressure": [
            "mass/(distance*time*time)",
            "force/(distance*distance)",
            "force/area",
            "pressure"
            ],
        }

By defining fundamental unit types we can leverage them to curate a list of supported unit types and their associated
conversion factors (more on this below) relative to a base unit; instead of a large matrix of unit types and conversion
factors with a one-to-one relationship.

### Unit conversion

The unit conversion dictionary contains all the supported *unit types* and their respective conversion factors to the
base unit. The dictionary is nested, with the highest branch being the `unit type`. The unit conversion dictionary
currently supports the following `unit types` and corresponding [`base unit of measure`]:

- Mass [*kg*]
- Distance [*m*]
- Time [*s*]
- Temperature [*degC*]
- Pressure [*kg/(m\*s\*s)*]
- Area [*m\*m*]
- Volume [*m\*m\*m*]
- Velocity [*m/s*]
- Volumetric Flowrate [*m\*m\*m/s*]
- Gas Flowrate [*m\*m\*m/s*]
- Mass Flowrate [*kg/s*]
- Acceleration [*m/(s*s)*]
- Force  [*kg\*m/(s\*s)*]
- Density [*kg/(m\*m\*m)*]

A `unit type` in the dictionary must contain of the following fields:

- `fundamental`: "true" if it is a fundamental unit type
- `base unit`: base unit of measure from the SI system
- `units`: dictionary will all the supported units of measure for each unit type
    - `multiplier`: conversion factor value (float) relative to the base unit of measure.
    - `description`: Human readable description of the unit of measure. If it is a base units it should be indicated
      with the label ``[base unit]`` at the end of the description.
    - `shift` (Optional): conversion factor shift value (float) relative to the base unit of measure.
    - `aliases` (Optional): unit of measure aliases

Note that "shift" and "aliases" are optional. Not all conversions require "shift" parameter and aliases are used to
accommodate other notations or names used to designate the same unit of measure.

Below is an example of the dictionary containing the ``base unit`` [``kg``] and an additional unit of mass [``lbm``].

    {
        "mass": {
        "fundamental": "true",
        "base_unit": "kg",
        "units": {
            "kg": {
                "multiplier": "1.000000",
                "description": "kilogram [base unit]"
            },
            "lbm": {
                "multiplier": "0.453592",
                "description": "pound mass"
            },
        }
    }

Note that the that the `multiplier` is `0.453592` which means that 1 lbm = 0.453592 kg. The conversion has to be stated
in this format (1 unit = *x* base unit) *not* the other way (1 base unit = *x* unit).

An example using `shift` is temperature conversion from degrees Celsius (`degC`) to Fahrenheit (`degF`), where
`degF = degC * 1.8 + 32`. In this case, `degC = (degF - 32)/1.8`, therefore, the `multiplier` is 1/1.8 =
0.555555555555556 and the `shift` is -32.000000. The example below shows how this looks in the dictionary and also
contains a few unit of measure aliases for degrees Celsius.

    {
        "temperature": {
        "fundamental": "true",
        "base_unit": "degC",
        "units": {
            "degC": {
                "multiplier": "1.000000",
                "description":"degrees celsius",
                "aliases": ["deg_C", "deg C"]
            },
            "degF": {
                "multiplier": "0.555555555555556",
                "shift": "-32.000000",
                "description":"degrees fahrenheit"
            },
            "degR": {
                "multiplier": "0.555555555555556",
                "shift": "-491.670000",
                "description":"degrees rankine"
            },
            "degK": {
                "multiplier": "1.000000",
                "shift": "-273.150000",
                "description":"degrees kelvin"
            }
        }
    }

### Scalers

Dictionary containing the name, numerical value and aliases of the
[metric prefixes](https://en.wikipedia.org/wiki/Metric_prefix). This dictionary is used to support conversion to and
from units containing prefixes in the unit of measure (e.g. kN)

### Unit type aliases

This dictionary is used to support unit type names that require special treatment or special names used by certain
industries or sectors of an industry. For example, `gas volumetric flowrate` is an alias for
`volumetric flowrate` but given the quantities measured and conditions, the units of measure are sometimes very
different from the traditional ones (e.g. standard liters per minute or slpm)

## Contributing to the unit converter

Contributing to the unit converter means adding new units to the dictionary. To contribute, it is important to follow
the beginning section of the `Contributing` guide under the `Preliminaries and setup` section.

1. If a desired `unit` is missing in the unit_conversion.json dictionary, add a new entry under the appropriate
   `unit type`. The key should be the `unit` and the value should be a dictionary consisting of the
   `multiplier`, `description`,  `shift`, and `aliases`. The items `multiplier` and `shift` must contain at least 6 be
   at least 6 significant digits or figures. This aims to minimize error propagation and to estimate it if necessary.
   For example, even if the conversion is only two decimal points `1.23`, additional four zeroes,
   `1.230000`, must be added for the entry to be correct.

2. If a `unit type` is missing, add the new unit type to the dictionary. The first entry of the `unit type` has to be
   the base SI unit which is typically a combination of `kg`, `m`, `s` and `degC`. An additional entry has to be added
   for the `base unit` entry which is `"base": "true"`.

3. If a new `unit type` is added then add that `unit type` to the `unit_type_fundamental.json` dictionary with all the
   ways the `unit type` can be broken down (see example at the start of this section). The key to this dictionary is
   the `unit type` and the value is the list with the brake down of fundamental combinations.
