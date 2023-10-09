from __future__ import annotations

import re
from datetime import datetime
from typing import TYPE_CHECKING, Any, Sequence, cast

from cognite.client._api_client import APIClient
from cognite.client.data_classes import Datapoints, DatapointsList, TimeSeries
from cognite.client.utils._auxiliary import local_import
from cognite.client.utils._concurrency import execute_tasks
from cognite.client.utils._time import timestamp_to_ms

if TYPE_CHECKING:
    import sympy

    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class SyntheticDatapointsAPI(APIClient):
    _RESOURCE_PATH = "/timeseries/synthetic"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._DPS_LIMIT_SYNTH = 10_000

    def query(
        self,
        expressions: str | sympy.Expr | Sequence[str | sympy.Expr],
        start: int | str | datetime,
        end: int | str | datetime,
        limit: int | None = None,
        variables: dict[str, str | TimeSeries] | None = None,
        aggregate: str | None = None,
        granularity: str | None = None,
    ) -> Datapoints | DatapointsList:
        """`Calculate the result of a function on time series. <https://developer.cognite.com/api#tag/Synthetic-Time-Series/operation/querySyntheticTimeseries>`_

        Args:
            expressions (str | sympy.Expr | Sequence[str | sympy.Expr]): Functions to be calculated. Supports both strings and sympy expressions. Strings can have either the API `ts{}` syntax, or contain variable names to be replaced using the `variables` parameter.
            start (int | str | datetime): Inclusive start.
            end (int | str | datetime): Exclusive end
            limit (int | None): Number of datapoints per expression to retrieve.
            variables (dict[str, str | TimeSeries] | None): An optional map of symbol replacements.
            aggregate (str | None): use this aggregate when replacing entries from `variables`, does not affect time series given in the `ts{}` syntax.
            granularity (str | None): use this granularity with the aggregate.

        Returns:
            Datapoints | DatapointsList: A DatapointsList object containing the calculated data.

        Examples:

            Request a synthetic time series query with direct syntax

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> dps = c.time_series.data.synthetic.query(expressions="TS{id:123} + TS{externalId:'abc'}", start="2w-ago", end="now")

            Use variables to re-use an expression:

                >>> vars = {"A": "my_ts_external_id", "B": client.time_series.retrieve(id=1)}
                >>> dps = c.time_series.data.synthetic.query(expressions="A+B", start="2w-ago", end="now", variables=vars)

            Use sympy to build complex expressions:

                >>> from sympy import symbols, cos, sin
                >>> a = symbols('a')
                >>> dps = c.time_series.data.synthetic.query([sin(a), cos(a)], start="2w-ago", end="now", variables={"a": "my_ts_external_id"}, aggregate='interpolation', granularity='1m')
        """
        if limit is None or limit == -1:
            limit = cast(int, float("inf"))

        tasks = []
        expressions_to_iterate = (
            expressions if (isinstance(expressions, Sequence) and not isinstance(expressions, str)) else [expressions]
        )

        for exp in expressions_to_iterate:
            expression, short_expression = self._build_expression(exp, variables, aggregate, granularity)
            query = {"expression": expression, "start": timestamp_to_ms(start), "end": timestamp_to_ms(end)}
            values: list[float] = []  # mypy
            query_datapoints = Datapoints(value=values, error=[])
            query_datapoints.external_id = short_expression

            tasks.append((query, query_datapoints, limit))

        datapoints_summary = execute_tasks(self._fetch_datapoints, tasks, max_workers=self._config.max_workers)
        datapoints_summary.raise_first_encountered_exception()

        return (
            DatapointsList(datapoints_summary.results, cognite_client=self._cognite_client)
            if isinstance(expressions, list)
            else datapoints_summary.results[0]
        )

    def _fetch_datapoints(self, query: dict[str, Any], datapoints: Datapoints, limit: int) -> Datapoints:
        while True:
            query["limit"] = min(limit, self._DPS_LIMIT_SYNTH)
            resp = self._post(url_path=self._RESOURCE_PATH + "/query", json={"items": [query]})
            data = resp.json()["items"][0]
            datapoints._extend(Datapoints._load(data, expected_fields=["value", "error"]))
            limit -= len(data["datapoints"])
            if len(data["datapoints"]) < self._DPS_LIMIT_SYNTH or limit <= 0:
                break
            query["start"] = data["datapoints"][-1]["timestamp"] + 1
        return datapoints

    @staticmethod
    def _build_expression(
        expression: str | sympy.Expr,
        variables: dict[str, Any] | None = None,
        aggregate: str | None = None,
        granularity: str | None = None,
    ) -> tuple[str, str]:
        if expression.__class__.__module__.startswith("sympy."):
            expression_str = SyntheticDatapointsAPI._sympy_to_sts(expression)
            if not variables:
                raise ValueError(
                    "sympy expressions are only supported in combination with the `variables` parameter to map symbols to time series."
                )
        else:
            expression_str = cast(str, expression)
        if aggregate and granularity:
            aggregate_str = f",aggregate:'{aggregate}',granularity:'{granularity}'"
        else:
            aggregate_str = ""
        expression_with_ts: str = expression_str
        if variables:
            for k, v in variables.items():
                if isinstance(v, TimeSeries):
                    v = v.external_id
                expression_with_ts = re.sub(
                    re.compile(rf"\b{k}\b"), f"ts{{externalId:'{v}'{aggregate_str}}}", expression_with_ts
                )
        return expression_with_ts, expression_str

    @staticmethod
    def _sympy_to_sts(expression: str | sympy.Expr) -> str:
        sympy_module = cast(Any, local_import("sympy"))

        infix_ops = {sympy_module.Add: "+", sympy_module.Mul: "*"}
        functions = {
            sympy_module.cos: "cos",
            sympy_module.sin: "sin",
            sympy_module.sqrt: "sqrt",
            sympy_module.log: "ln",
            sympy_module.exp: "exp",
            sympy_module.Abs: "abs",
        }

        def process_symbol(sym: Any) -> str:
            if isinstance(sym, sympy_module.AtomicExpr):
                if isinstance(sym, sympy_module.NumberSymbol):
                    return str(sym.evalf(15))
                else:
                    return str(sym)

            infixop = infix_ops.get(sym.__class__)
            if infixop:
                return "(" + infixop.join(process_symbol(s) for s in sym.args) + ")"

            if isinstance(sym, sympy_module.Pow):
                if sym.args[1] == -1:
                    return f"(1/{process_symbol(sym.args[0])})"
                return f"pow({','.join(map(process_symbol, sym.args))})"

            if funop := functions.get(sym.__class__):
                return f"{funop}({','.join(map(process_symbol, sym.args))})"
            raise ValueError(f"Unsupported sympy class {sym.__class__} encountered in expression")

        return process_symbol(expression)
