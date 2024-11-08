import inspect

from docutils.parsers.rst import directives
from sphinx.ext.autosummary import Autosummary


class AutoClassSummary(Autosummary):
    """Add custom functionality to autosummary directive.

    This adds three extra options to the autosummary directive:
    * classes: using this together with 'custom-automodule-template.rst' automatically generates the autosummary for all classes in a module.
    * methods: using this together with 'custom-automethods-template.rst' automatically generates the autosummary for all methods in a class.
    * attributes: using this together with 'custom-automethods-template.rst' automatically generates the autosummary for all attributes in a class.

    The methods and attributes options can be combined.
    """

    option_spec = {
        "methods": directives.unchanged,
        "attributes": directives.unchanged,
        "classes": directives.unchanged,
        "template": directives.unchanged,
        "toctree": directives.unchanged,
        "caption": directives.unchanged_required,
        "nosignatures": directives.flag,
        "recursive": directives.flag,
    }

    def run(self):
        if "classes" in self.options:
            content = []
            for name in self.content:
                prefixes: list[str | None] = [None]
                currmodule = self.env.ref_context.get("py:module")
                if currmodule:
                    prefixes.insert(0, currmodule)
                obj_module, obj, _, _ = self.import_by_name(name, prefixes=prefixes)
                classes = [
                    clas
                    for clas, clas_obj in inspect.getmembers(obj, inspect.isclass)
                    if obj_module == clas_obj.__module__ and not clas.startswith("_")
                ]
                content.extend([f"~{name}.{clas}" for clas in classes])
            self.content = content
        if "methods" in self.options or "attributes" in self.options:
            content = []
            for name in self.content:
                prefixes: list[str | None] = [None]
                currmodule = self.env.ref_context.get("py:module")
                if currmodule:
                    prefixes.insert(0, currmodule)
                _, obj, _, _ = self.import_by_name(name, prefixes=prefixes)
                attributes = [attribute for attribute in dir(obj) if not isinstance(getattr(obj, attribute), type)]
                if "methods" in self.options:
                    methods = [
                        method_name
                        for method_name in attributes
                        if callable(getattr(obj, method_name)) and not method_name.startswith("_")
                    ]
                    content.extend([f"{name}.{method}" for method in methods])
                if "attributes" in self.options:
                    attribs = [
                        attrib_name
                        for attrib_name in attributes
                        if not callable(getattr(obj, attrib_name)) and not attrib_name.startswith("_")
                    ]
                    content.extend([f"{name}.{attrib}" for attrib in attribs if not attrib.startswith("_")])
            self.content = content
        return super().run()


def setup(app):
    app.add_directive("autosummary", AutoClassSummary, override=True)

    return {"parallel_read_safe": True}
