import re
from datetime import datetime
from typing import Dict, List, Union

import cognite.client.utils._time
from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import Datapoints, DatapointsList, TimeSeries
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._experimental_warning import experimental_api


class SyntheticDatapointsAPI(APIClient):
    _RESOURCE_PATH = "/timeseries/synthetic"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._DPS_LIMIT = 10000

    def query(
        self,
        expressions: Union[str, "sympy.Expr", List[Union[str, "sympy.Expr"]]],
        start: Union[int, str, datetime],
        end: Union[int, str, datetime],
        limit: int = None,
        variables: Dict[str, Union[str, TimeSeries]] = None,
        aggregate: str = None,
        granularity: str = None,
    ) -> Union[Datapoints, DatapointsList]:
        """`Calculate the result of a function on time series. <https://docs.cognite.com/api/v1/#operation/querySyntheticTimeseries>`_

        Args:
            expressions (Union[str, "sympy.Expr", List[Union[str, "sympy.Expr"]]]): Functions to be calculated. Supports both strings and sympy expressions. Strings can have either the API `ts{}` syntax, or contain variable names to be replaced using the `variables` parameter.
            start (Union[int, str, datetime]): Inclusive start.
            end (Union[int, str, datetime]): Exclusive end
            limit (int): Number of datapoints per expression to retrieve.
            variables (Dict[str,Union[str,TimeSeries]]): An optional map of symbol replacements.
            aggregate (str): use this aggregate when replacing entries from `variables`, does not affect time series given in the `ts{}` syntax.
            granularity (str): use this granularity with the aggregate.

        Returns:
            Union[Datapoints, DatapointsList]: A DatapointsList object containing the calculated data.

        Examples:

            Request a synthetic time series query with direct syntax

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> dps = c.datapoints.synthetic.query(expressions="TS{id:123} + TS{externalId:'abc'}", start="2w-ago", end="now")

            Use variables to re-use an expression:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> vars = {"A": "my_ts_external_id", "B": client.time_series.retrieve(id=1)}
                >>> dps = c.datapoints.synthetic.query(expressions="A+B", start="2w-ago", end="now", variables=vars)

            Use sympy to build complex expressions:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> from sympy import symbols, cos, sin
                >>> a = symbols('a')
                >>> dps = c.datapoints.synthetic.query([sin(a), cos(a)], start="2w-ago", end="now", variables={"a": "my_ts_external_id"}, aggregate='interpolation', granularity='1m')
        """
        if limit is None or limit == -1:
            limit = float("inf")

        tasks = []
        expressions_to_iterate = expressions if isinstance(expressions, List) else [expressions]

        for i in range(len(expressions_to_iterate)):
            expression, short_expression = SyntheticDatapointsAPI._build_expression(
                expressions_to_iterate[i], variables, aggregate, granularity
            )
            query = {
                "expression": expression,
                "start": cognite.client.utils._time.timestamp_to_ms(start),
                "end": cognite.client.utils._time.timestamp_to_ms(end),
            }
            query_datapoints = Datapoints(value=[], error=[])
            query_datapoints.external_id = short_expression

            tasks.append((query, query_datapoints, limit))

        datapoints_summary = utils._concurrency.execute_tasks_concurrently(
            self._fetch_datapoints, tasks, max_workers=self._config.max_workers
        )

        if datapoints_summary.exceptions:
            raise datapoints_summary.exceptions[0]

        return (
            DatapointsList(datapoints_summary.results, cognite_client=self._cognite_client)
            if isinstance(expressions, List)
            else datapoints_summary.results[0]
        )

    def _fetch_datapoints(self, query, datapoints, limit):
        while True:
            query["limit"] = min(limit, self._DPS_LIMIT)
            resp = self._post(url_path=self._RESOURCE_PATH + "/query", json={"items": [query]})
            data = resp.json()["items"][0]
            datapoints._extend(Datapoints._load(data, expected_fields=["value", "error"]))
            limit -= len(data["datapoints"])
            if len(data["datapoints"]) < self._DPS_LIMIT or limit <= 0:
                break
            query["start"] = data["datapoints"][-1]["timestamp"] + 1
        return datapoints

    @staticmethod
    def _build_expression(expression, variables=None, aggregate=None, granularity=None):
        if expression.__class__.__module__.startswith("sympy."):
            expression = SyntheticDatapointsAPI._sympy_to_sts(expression)
            if not variables:
                raise ValueError(
                    "sympy expressions are only supported in combination with the `variables` parameter to map symbols to time series."
                )
        if aggregate and granularity:
            aggregate_str = ",aggregate:'{}',granularity:'{}'".format(aggregate, granularity)
        else:
            aggregate_str = ""
        expression_with_ts = expression
        if variables:
            for k, v in variables.items():
                if isinstance(v, TimeSeries):
                    v = v.external_id
                expression_with_ts = re.sub(
                    re.compile(r"\b%s\b" % k), "ts{externalId:'%s'%s}" % (v, aggregate_str), expression_with_ts
                )
        return expression_with_ts, expression

    @staticmethod
    def _sympy_to_sts(expression):
        sympy = utils._auxiliary.local_import("sympy")

        infix_ops = {sympy.Add: "+", sympy.Mul: "*"}
        functions = {
            sympy.cos: "cos",
            sympy.sin: "sin",
            sympy.sqrt: "sqrt",
            sympy.log: "ln",
            sympy.exp: "exp",
            sympy.Abs: "abs",
        }

        def process_symbol(sym):
            if isinstance(sym, sympy.AtomicExpr):
                if isinstance(sym, sympy.NumberSymbol):
                    return str(sym.evalf(15))
                else:
                    return str(sym)

            infixop = infix_ops.get(sym.__class__)
            if infixop:
                return "(" + infixop.join(process_symbol(s) for s in sym.args) + ")"
            if isinstance(sym, sympy.Pow):
                if sym.args[1] == -1:
                    return "(1/{})".format(process_symbol(sym.args[0]))
                return "pow({},{})".format(*[process_symbol(x) for x in sym.args])
            funop = functions.get(sym.__class__)
            if funop:
                return "{}({})".format(funop, ",".join(process_symbol(x) for x in sym.args))
            raise ValueError("Unsupported sympy class {} encountered in expression".format(sym.__class__))

        return process_symbol(expression)
