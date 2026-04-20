{% if objname in headings %}
{{headings[objname] | escape | underline }}
{% else %}
{{ objname.split('.')[-1].replace('_', ' ').title() | escape | underline }}
{% endif %}

.. currentmodule:: {{ module }}

.. autoaccessormethod:: {{ objname }}