import inspect
import json
import re
from collections import defaultdict
from doctest import DocTestParser, Example

from cognite.client._api_client import APIClient
from cognite.client.experimental import CogniteClient


def collect_apis(obj, done):
    if done.get(obj.__class__):
        return []
    done[obj.__class__] = True
    apis = inspect.getmembers(obj, lambda m: isinstance(m, APIClient) and not done.get(m.__class__))
    sub = [(n + "." + sn, sa) for n, c in apis for sn, sa in collect_apis(c, done)]
    return apis + sub


client = CogniteClient(project="_", api_key="_", client_name="_")
parser = DocTestParser()

apis = collect_apis(client, {})

snippets = {"language": "Python", "label": "Python SDK", "operations": defaultdict(str)}
filter_out = ["from cognite.client import CogniteClient", "c = CogniteClient()"]

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
        match_link_openapi = re.match("`.* <.*?#operation/(.*)>`_", docstring.strip().split("\n")[0])
        if api_name[0] != "_" and fun_name[0] != "_" and match_link_openapi:
            openapi_ident = match_link_openapi[1]
            parsed_lines = parser.parse(fun.__doc__)
            snippet_lines = [
                re.sub("(= |in |^)c.", "\\1client.", ex.source)
                if isinstance(ex, Example) and ex.source.strip() not in filter_out
                else "\n"
                for ex in parsed_lines
            ]
            code = re.sub("\n{2,}", "\n\n", "".join(snippet_lines)).strip()
            snippets["operations"][openapi_ident] += code
            if openapi_ident in duplicate_operations:
                snippets["operations"][duplicate_operations[openapi_ident]] += code

filename = "python-sdk-examples.json"
with open(filename, "w+") as f:
    json.dump(snippets, f, indent=2)
print("JS code snippets saved to: " + filename)
