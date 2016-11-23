Configuration
=============

Event Adapter is configured using INI configuration files. There is no implicit
configuration file location, so it must be specified using the command line:

.. code-block:: bash

   python -m eva --config <PATH> [--config <PATH>] [...]

`PATH` can be either a file or a directory. In case `PATH` is a directory, the
entire directory is recursively scanned for configuration files. All
configuration files must end with `.ini`, all other files are ignored.

When all the configuration file paths have been resolved, they are merged
internally into one big configuration file. This implies that section names
must be unique, but grants you the ability to add references to configuration
sections that are present only in another file.


Configuration file format
-------------------------

Let's start with the most basic configuration that EVA will accept.
Configuration variables are grouped in sections, and sections can be
cross-referenced where applicable.

.. code-block:: ini
   :caption: eva-config/base.ini

   [eva]
   listeners = listener.productstatus
   mailer = mailer.default
   productstatus = productstatus.localhost
   statsd = 127.0.0.1:8125
   zookeeper = 127.0.0.1:2181/eva

   [productstatus.localhost]
   class = eva.incubator.Productstatus
   url = http://127.0.0.1:8000

   [mailer.default]
   class = eva.mail.Mailer
   enabled = NO
   smtp_host = 127.0.0.1
   recipients = me@example.com
   from = eva@example.com

   [listener.productstatus]
   class = eva.listener.ProductstatusListener

First, we define the ``[eva]`` section. This is the top-level configuration,
and requires a definition of global clients that should be instantiated upon
startup.

  * ``listeners``: a section reference to one or more ``Listener`` classes.
  * ``mailer``: a section reference to a ``Mailer`` class.
  * ``productstatus``: a section reference to a ``Productstatus`` class.
  * ``statsd``: comma-delimited ``HOST:PORT`` pairs to StatsD_ servers.
  * ``zookeeper``: ZooKeeper connection string.

Here's a more advanced example. Note that the configuration from the previous
example is assumed to be automatically included in this example. This example
uses section inheritance and default parameters.

.. code-block:: ini
   :caption: eva-config/advanced.ini

   [executor.grid_engine]
   class = eva.executor.GridEngineExecutor
   ssh_host = example.com
   ssh_key_file = /home/user/.ssh/id_rsa
   ssh_user = user

   [adapter.defaults]
   abstract = true
   executor = executor.grid_engine

   [include.adapter.a]
   abstract = true
   input_service_backend = backend-a
   output_service_backend = backend-a

   [include.adapter.b]
   abstract = true
   input_service_backend = backend-b
   output_service_backend = backend-b

   [adapter.foo.a]
   class = eva.adapter.ExampleAdapter
   include = include.adapter.a
   input_file_format = grib

   [adapter.foo.b]
   class = eva.adapter.ExampleAdapter
   include = include.adapter.b
   input_file_format = netcdf

Here, we define two adapters that should be run, ``adapter.foo.a`` and
``adapter.foo.b``. Both of these adapters will receive all events on the wire,
and generate their own sets of jobs based on the events. We use default values
and section inheritance to avoid duplicating configuration when many parameters
are similar.

The rendered configuration that EVA will see, looks like this:

.. code-block:: ini

   [executor.grid_engine]
   class = eva.executor.GridEngineExecutor
   ssh_host = example.com
   ssh_key_file = /home/user/.ssh/id_rsa
   ssh_user = user

   [adapter.foo.a]
   class = eva.adapter.ExampleAdapter
   executor = executor.grid_engine
   input_file_format = grib
   input_service_backend = backend-a
   output_service_backend = backend-a

   [adapter.foo.b]
   class = eva.adapter.ExampleAdapter
   executor = executor.grid_engine
   input_file_format = netcdf
   input_service_backend = backend-b
   output_service_backend = backend-b

Some notes on syntax:

  * ``abstract``: specifies that a section should ONLY be used for inheritance,
    and not instantiated when reading the configuration. You *MUST* define
    either ``class`` or ``abstract`` when writing a configuration section,
    otherwise EVA will refuse to start.

  * ``class``: full Python class name, in dotted notation, referring to a class
    derived from :class:`eva.config.ConfigurableObject`.


.. _StatsD: https://github.com/influxdata/telegraf/tree/master/plugins/inputs/statsd
