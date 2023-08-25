:orphan:

{{ objname | escape | underline}}

.. currentmodule:: {{ module }}

{% block methods %}
{% if methods %}
.. autosummary::
    :toctree:
    :template: custom-accessor-template.rst
{% for item in methods %}
{%- if item != "__init__" %}
    {{ name }}.{{ item }}
{%- endif %}
{%- endfor %}
{% endif %}
{% endblock %}

{% block attributes %}
{% if attributes %}
.. rubric:: {{ _('Attributes') }}

.. autosummary::
{% for item in attributes %}
    ~{{ name }}.{{ item }}
{%- endfor %}
{% endif %}
{% endblock %}