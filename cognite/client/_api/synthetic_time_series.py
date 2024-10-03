from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any, Union, cast

from cognite.client._api_client import APIClient
from cognite.client.data_classes import Datapoints, DatapointsList, TimeSeries, TimeSeriesWrite
from cognite.client.data_classes.time_series import TimeSeriesCore
from cognite.client.utils._auxiliary import is_unlimited
from cognite.client.utils._concurrency import execute_tasks
from cognite.client.utils._importing import local_import
from cognite.client.utils._time import timestamp_to_ms
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    import sympy

    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


def _supported_sympy_infix_ops(operation: type[sympy.Basic]) -> str | None:
    sympy = local_import("sympy")
    return {sympy.Add: "+", sympy.Mul: "*"}.get(operation)


def _supported_sympy_functions(operation: type[sympy.Basic]) -> str | None:
    sympy = local_import("sympy")
    return {
        sympy.cos: "cos",
        sympy.sin: "sin",
        sympy.sqrt: "sqrt",
        sympy.log: "ln",
        sympy.exp: "exp",
        sympy.Abs: "abs",
    }.get(operation)


class SyntheticDatapointsAPI(APIClient):
    _RESOURCE_PATH = "/timeseries/synthetic"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._DPS_LIMIT_SYNTH = 10_000

    def query(
        self,
        expressions: str | sympy.Basic | Sequence[str | sympy.Basic],
        start: int | str | datetime,
        end: int | str | datetime,
        limit: int | None = None,
        variables: dict[str | sympy.Symbol, str | TimeSeries | TimeSeriesWrite] | None = None,
        aggregate: str | None = None,
        granularity: str | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
    ) -> Datapoints | DatapointsList:
        """`Calculate the result of a function on time series. <https://developer.cognite.com/api#tag/Synthetic-Time-Series/operation/querySyntheticTimeseries>`_

        Args:
            expressions (str | sympy.Basic | Sequence[str | sympy.Basic]): Functions to be calculated. Supports both strings and sympy expressions. Strings can have either the API `ts{}` syntax, or contain variable names to be replaced using the `variables` parameter.
            start (int | str | datetime): Inclusive start.
            end (int | str | datetime): Exclusive end.
            limit (int | None): Number of datapoints per expression to retrieve.
            variables (dict[str | sympy.Symbol, str | TimeSeries | TimeSeriesWrite] | None): An optional map of symbol replacements.
            aggregate (str | None): use this aggregate when replacing entries from `variables`, does not affect time series given in the `ts{}` syntax.
            granularity (str | None): use this granularity with the aggregate.
            target_unit (str | None): use this target_unit when replacing entries from `variables`, does not affect time series given in the `ts{}` syntax.
            target_unit_system (str | None): Same as target_unit, but with unit system (e.g. SI). Only one of target_unit and target_unit_system can be specified.

        Returns:
            Datapoints | DatapointsList: A DatapointsList object containing the calculated data.

        Examples:

            Request a synthetic time series query with direct syntax:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> dps = client.time_series.data.synthetic.query(
                ...     expressions="ts{id:123} + ts{externalId:'abc'}",
                ...     start="2w-ago",
                ...     end="now")

            Use variables to re-use an expression:

                >>> ts = client.time_series.retrieve(id=123)
                >>> variables = {"A": ts, "B": "my_ts_external_id"}
                >>> dps = client.time_series.data.synthetic.query(
                ...     expressions="A+B", start="2w-ago", end="now", variables=variables)

            Use sympy to build complex expressions:

                >>> from sympy import symbols, cos, sin
                >>> x, y = symbols("x y")
                >>> dps = client.time_series.data.synthetic.query(
                ...     [sin(x), y*cos(x)],
                ...     start="2w-ago",
                ...     end="now",
                ...     variables={x: "foo", y: "bar"},
                ...     aggregate="interpolation",
                ...     granularity="15m",
                ...     target_unit="temperature:deg_c")
        """
        if is_unlimited(limit):
            limit = cast(int, float("inf"))

        if single_expr := not isinstance(expressions, SequenceNotStr):
            expressions = [cast(Union[str, "sympy.Basic"], expressions)]

        tasks = []
        for user_expr in cast(Sequence[Union[str, "sympy.Basic"]], expressions):
            expression, short_expression = self._build_expression(
                user_expr, variables, aggregate, granularity, target_unit, target_unit_system
            )
            query = {"expression": expression, "start": timestamp_to_ms(start), "end": timestamp_to_ms(end)}
            query_datapoints = Datapoints(external_id=short_expression, value=[], error=[])
            tasks.append((query, query_datapoints, limit))

        datapoints_summary = execute_tasks(self._fetch_datapoints, tasks, max_workers=self._config.max_workers)
        datapoints_summary.raise_compound_exception_if_failed_tasks()
        return (
            DatapointsList(datapoints_summary.results, cognite_client=self._cognite_client)
            if not single_expr
            else datapoints_summary.results[0]
        )

    def _fetch_datapoints(self, query: dict[str, Any], datapoints: Datapoints, limit: int) -> Datapoints:
        while True:
            query["limit"] = min(limit, self._DPS_LIMIT_SYNTH)
            resp = self._post(url_path=self._RESOURCE_PATH + "/query", json={"items": [query]})
            data = resp.json()["items"][0]
            datapoints._extend(Datapoints._load_from_synthetic(data))
            limit -= (n_fetched := len(data["datapoints"]))
            if n_fetched < self._DPS_LIMIT_SYNTH or limit <= 0:
                break
            query["start"] = data["datapoints"][-1]["timestamp"] + 1
        return datapoints

    def _build_expression(
        self,
        expression: str | sympy.Basic,
        variables: dict[str | sympy.Symbol, str | TimeSeries | TimeSeriesWrite] | None = None,
        aggregate: str | None = None,
        granularity: str | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
    ) -> tuple[str, str]:
        if getattr(expression, "__sympy__", False) is True:
            if variables:
                expression_str = self._process_sympy_expression(cast("sympy.Basic", expression))
            else:
                raise ValueError(
                    "sympy expressions are only supported in combination with the `variables` parameter to map symbols to time series."
                )
        elif isinstance(expression, str):
            expression_str = expression
        else:
            raise TypeError(f"expression must be str or a sympy expression, not {type(expression)}")

        if aggregate and granularity:
            aggregate_str = f",aggregate:'{aggregate}',granularity:'{granularity}'"
        elif not aggregate and not granularity:
            aggregate_str = ""
        else:
            raise ValueError("Pass either both of 'aggregate' and 'granularity', or neither")

        target_unit_str = ""
        if target_unit:
            if target_unit_system:
                raise ValueError("Only one of 'target_unit' and 'target_unit_system' can be specified.")
            target_unit_str = f",targetUnit:'{target_unit}'"
        elif target_unit_system:
            target_unit_str = f",targetUnitSystem:'{target_unit_system}'"

        if not variables:
            return expression_str, expression_str

        to_substitute = {}
        for k, v in variables.items():
            if isinstance(v, TimeSeriesCore):
                if v.external_id is None:
                    raise ValueError(f"TimeSeries passed in 'variables' is missing required field 'external_id' ({v})")
                v = v.external_id
            # We convert to str to ensure any sympy.Symbol is replaced with its name:
            to_substitute[re.escape(str(k))] = f"ts{{externalId:'{v}'{aggregate_str}{target_unit_str}}}"

        # Substitute all variables in one go to avoid substitution of prior substitutions:
        pattern = re.compile(r"\b" + r"\b|\b".join(to_substitute) + r"\b")  # note: \b marks a word boundary
        expression_with_ts = pattern.sub(lambda match: to_substitute[match[0]], expression_str)
        return expression_with_ts, expression_str

    def _process_sympy_expression(self, expression: sympy.Basic) -> str:
        sympy = local_import("sympy")

        if isinstance(expression, sympy.AtomicExpr):
            if isinstance(expression, sympy.NumberSymbol):
                return str(expression.evalf(15)).rstrip("0")
            else:
                return str(expression).rstrip("0")

        expr_cls = type(expression)
        if infix_op := _supported_sympy_infix_ops(expr_cls):
            return "(" + infix_op.join(self._process_sympy_expression(s) for s in expression.args) + ")"

        if isinstance(expression, sympy.Pow):
            if expression.args[1] == -1:
                return f"(1/{self._process_sympy_expression(expression.args[0])})"
            return f"pow({','.join(map(self._process_sympy_expression, expression.args))})"

        if fn_op := _supported_sympy_functions(expr_cls):
            return f"{fn_op}({','.join(map(self._process_sympy_expression, expression.args))})"
        raise TypeError(f"Unsupported sympy class {expr_cls} encountered in expression")
