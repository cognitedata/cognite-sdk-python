:orphan:

{{ objname | escape | underline}}

.. currentmodule:: {{ module }}

{% block methods %}
{% if methods %}
.. autosummary::
    :toctree:
    :template: custom-accessor-template.rst

{% if "__call__" in members %}
    {{ name }}.__call__
{%  endif %}
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