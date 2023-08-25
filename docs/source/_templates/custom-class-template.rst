..
   Source: https://stackoverflow.com/a/62613202

{{ objname | escape | underline}}

.. currentmodule:: {{ module }}

.. autoclass:: {{ objname }}
   :show-inheritance:

   {% block methods %}

   {% if methods %}
   .. rubric:: {{ _('Methods') }}

   .. autosummary::
      :toctree:
      :template: custom-accessor-template.rst
   {% for item in methods %}
   {%- if item != "__init__" %}
      ~{{ name }}.{{ item }}
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