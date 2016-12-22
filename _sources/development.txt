Development
===========

This section is important to you if you want to run Event Adapter on your local
machine, either because you are writing custom adapters, or because you are
developing EVA itself.

Setting up a development environment
------------------------------------

EVA is a Python 3 program. It is recommended to set up a virtualenv_ to contain
dependencies, so that you do not clobber up your system-wide Python package
directory.

Check out the `EVA source code`_, then proceed to set up your virtualenv_:

.. code-block:: bash

   git clone https://github.com/metno/eva.git
   cd eva/
   sudo apt-get install python-pip python-virtualenv python-dev
   virtualenv deps
   source deps/bin/activate
   pip install -e .

Running
-------

.. code-block:: bash

   python -m eva --help

Docker container
----------------

Building the Docker container:

.. code-block:: bash

   make eva

Uploading the finished Docker container to the Docker registry:

.. code-block:: bash

   make upload-eva

Running tests and lint
----------------------

.. code-block:: bash

   source deps/bin/activate
   make test
   make lint

Generating documentation
------------------------

.. code-block:: bash

   make doc



.. _virtualenv: https://virtualenv.pypa.io/en/stable/
.. _`EVA source code`: https://github.com/metno/eva
