:orphan:

{% block classes %}
{% if classes %}
.. autosummary::
    :toctree:
    :template: custom-class-template.rst
{% for item in classes %}
    {{ fullname }}.{{ item }}
{%- endfor %}
{% endif %}
{% endblock %}

{% block exceptions %}
{% if exceptions %}
.. autosummary::
    :toctree:
    :template: custom-class-template.rst
{% for item in exceptions %}
    {{ fullname }}.{{ item }}
{%- endfor %}
{% endif %}
{% endblock %}