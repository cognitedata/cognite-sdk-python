import inspect
import json
import re
from collections import defaultdict
from doctest import DocTestParser, Example

from cognite.client import ClientConfig
from cognite.client._api_client import APIClient
from cognite.client.beta import CogniteClient
from cognite.client.credentials import Token


def collect_apis(obj, done):
    if done.get(obj.__class__):
        return []
    done[obj.__class__] = True
    apis = inspect.getmembers(obj, lambda m: isinstance(m, APIClient) and not done.get(m.__class__))
    sub = [(n + "." + sn, sa) for n, c in apis for sn, sa in collect_apis(c, done)]
    return apis + sub


client = CogniteClient(ClientConfig(project="_", client_name="_", credentials=Token("_")))
parser = DocTestParser()

apis = collect_apis(client, {})

snippets = {"language": "Python", "label": "Python SDK", "operations": defaultdict(str)}
filter_out = ["from cognite.client import CogniteClient", "client = CogniteClient()", ""]

duplicate_operations = {
    "listAssets": "getAssets",
    "advancedListEvents": "listEvents",
    "advancedListFiles": "listFiles",
    "advancedListSequences": "listSequences",
    "listTimeSeries": "getTimeSeries",
}

for api_name, api in apis:
    for fun_name, fun in inspect.getmembers(api, predicate=inspect.ismethod):
        docstring = fun.__doc__ or ""
        match_link_openapi = re.match("`.* <.*?/operation/(.*)>`_", docstring.strip().split("\n")[0])
        if api_name[0] != "_" and fun_name[0] != "_" and match_link_openapi:
            openapi_ident = match_link_openapi[1]
            parsed_lines = parser.parse(fun.__doc__)
            endpoint_snippets = []
            current_snippet = ""
            for ex in [*parsed_lines, "<end>"]:
                if isinstance(ex, Example):
                    if ex.source.strip() not in filter_out:
                        current_snippet += ex.source.rstrip() + "\n"
                elif ex != "" and current_snippet:
                    endpoint_snippets.append(current_snippet)
                    current_snippet = ""

            code = "\n".join(endpoint_snippets)
            snippets["operations"][openapi_ident] += code
            if openapi_ident in duplicate_operations:
                snippets["operations"][duplicate_operations[openapi_ident]] += code

print(json.dumps(snippets, indent=2))
