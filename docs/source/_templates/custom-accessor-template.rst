{{ objname.split('.')[1:] | join('.') | escape | underline }}

.. currentmodule:: {{ module.split('.')[0] }}

.. autoaccessormethod:: {{ (module.split('.')[1:] + [objname]) | join('.') }}