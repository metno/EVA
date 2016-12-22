Configuration directives
========================

[section]
---------

Starts a new configuration section.

There are a few special section names. Other than these names, you may define
your own configuration section names.

  * The section ``eva`` is used for defining global program options.
  * Variables under the ``DEFAULT`` section applies to every configuration section.
  * The section name ``defaults.<ROOT>`` is always loaded regardless of include
    directives. The name ``ROOT`` refers to the first part of a dotted name,
    e.g. ``adapter.foo.bar`` will automatically load the section
    ``defaults.adapter``.

.. code-block:: ini

   [defaults.my]
   ...

   [my.configuration.section]
   ...


abstract
--------

Specifies that a section should ONLY be used for inheritance, and not
instantiated when reading the configuration. You *MUST* define either ``class``
or ``abstract`` when writing a configuration section, otherwise EVA will refuse
to start.

.. code-block:: ini

   [adapter.base.foo]
   abstract = true
   input_product = foo


class
-----

Full Python class name, in dotted notation, referring to a class derived from
:class:`~eva.config.ConfigurableObject`.

.. code-block:: ini

   [adapter.foo]
   class = eva.adapter.FooAdapter


include
-------

Use this directive to join another configuration section into the current
configuration section.

For example, the following configuration file...

.. code-block:: ini

   [adapter.base.foo]
   abstract = true
   class = eva.adapter.FooAdapter

   [adapter.base.bar]
   abstract = true
   input_product = bar

   [adapter.foobar]
   include = adapter.base.foo, adapter.base.bar

... will be rendered as this:

.. code-block:: ini

   [adapter.foobar]
   class = eva.adapter.FooAdapter
   input_product = bar
