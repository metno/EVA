Tutorial
========

This tutorial will show you how to write an adapter for Event Adapter. You will
be using Python 3 for the logic that needs to go inside EVA. The tutorial
assumes that you have read the :doc:`intro`, set up your development
environment according to the :doc:`development` section, and created basic
configuration files as described in :doc:`configuration`.

First, a note on data flow. EVA is data-driven, using a message queue system
for event delivery. This means that every time metadata is updated in
Productstatus_, a message is generated, and sent to everyone who is interested.
EVA listens to this message queue, and loads the metadata (`Resource`)
associated with the event from Productstatus. This `Resource` is then passed on
to your Adapter class for validation, and if it is validated, your adapter
should generate a job that will be scheduled to run in a shell on a computing
node.


Creating an adapter
-------------------

In this tutorial, we are going to create an adapter that computes checksums for
processed files, and stores the result in Productstatus.  Your adapter can live
either inside or outside of the main EVA repository. For simplicity's sake, in
this tutorial we create an adapter that lives inside the main repository.

Create a skeleton file in the ``eva/adapters/`` directory:

.. code-block:: python
   :caption: ``eva/adapters/checksum.py``

   import eva.base.adapter

   class ChecksumAdapter(eva.base.adapter.BaseAdapter):
       def create_job(self, job):
           pass

       def finish_job(self, job):
           pass

       def generate_resources(self, job, resources):
           pass

This is the bare minimum required in order to load and run your adapter. The
three functions you need to override are documented here:

  * :meth:`eva.base.adapter.BaseAdapter.create_job`
  * :meth:`eva.base.adapter.BaseAdapter.finish_job`
  * :meth:`eva.base.adapter.BaseAdapter.generate_resources`


Loading the adapter
-------------------

In order to make your adapter run, you'll need to create a configuration
section for your adapter instance:

.. code-block:: ini
   :caption: ``eva-config/checksumadapter.ini``

   [adapter.test.checksum]
   class = eva.adapter.checksum.ChecksumAdapter

Now start EVA, loading the configuration directory (and, thus, your newly
created configuration file). You should see evidence of your adapter being
loaded::

    $ python -m eva --config /path/to/eva-config/
    (INFO) Starting EVA: the EVent Adapter.
    ...
    (INFO) Instantiating 'eva.adapter.checksum.ChecksumAdapter' from configuration section 'adapter.test.checksum'.
    ...
    (INFO) Initializing '<ChecksumAdapter: adapter.test.checksum>'...
    (WARNING) [adapter.test.checksum] Posting to Productstatus is DISABLED due to insufficient configuration.
    ...
    (INFO) Configured adapter: <ChecksumAdapter: adapter.test.checksum>
    ...

The adapter is loaded into the program. Right now, your adapter will accept all
events passed to it, but do nothing. Next, we'll take a look at how to make it
do something meaningful.


Adding configuration variables
------------------------------

We want our adapter to be re-usable so that we do not have to change the source
code each time we want to change the hashing algorithm. EVA supports custom
configuration variables, which can be configured in the INI files. We define
our configuration variables in the :attr:`eva.config.ConfigurableObject.CONFIG`
dictionary, and mark its inclusion in the INI file as optional:

.. code-block:: python
   :caption: ``eva/adapters/checksum.py``

   class ChecksumAdapter(eva.base.adapter.BaseAdapter):
       CONFIG = {
           'algorithm': {
               'type': 'string',
               'default': 'md5',
               'help': 'Hashing algorithm used to create a checksum.'
           },
       }

       OPTIONAL_CONFIG = [
           'algorithm',
       ]

Now, the variable is loaded from the INI file. If omitted, it will fall back on
the default value of ``md5``.

.. code-block:: ini
   :caption: ``eva-config/checksumadapter.ini``

   [adapter.test.checksum]
   class = eva.adapter.checksum.ChecksumAdapter
   algorithm = sha256

You can access the variable from within your code, like this:

.. code-block:: python
   :caption: ``eva/adapters/checksum.py``

   class ChecksumAdapter(eva.base.adapter.BaseAdapter):
       def create_job(self, job):
           assert self.env['algorithm'] == 'sha256'  # True


Adding executable shell code
----------------------------

Now that we have the configuration option in place, let's generate a job that
will be executed on the computing infrastructure. The
:meth:`eva.base.adapter.create_job` method is expected to set the
:attr:`eva.job.Job.command` variable to a string that will be put into a shell
script and executed.

.. code-block:: python
   :caption: ``eva/adapters/checksum.py``

   import eva
   import eva.exceptions

   class ChecksumAdapter(eva.base.adapter.BaseAdapter):
       def adapter_init(self):
           if self.env['algorithm'] == 'sha256':
               self.hash_command = "sha256sum %(path)s | awk '{print $1}'"
           elif selv.env['algorithm'] == 'md5':
               self.hash_command = "md5sum %(path)s | awk '{print $1}'"
           else:
               raise eva.exceptions.InvalidConfigurationException(
                   "Hashing algorithm '%s' in not supported." %
                   self.env['algorithm']
               )

       def create_job(self, job):
           params = {
               'path': eva.url_to_filename(job.resource.url),
           }
           job.command = '\n'.join([
               '#!/bin/sh',
               self.hash_command % params,
           ])

Here, we do two things. First, we run a check upon adapter initialization,
verifying that the configuration have parameters that our adapter supports. We
cache the command line we are going to use for later. If the adapter is
mis-configured, it is neccessary to raise
:exc:`eva.exceptions.InvalidConfigurationException` so that EVA can
complain about it to the user, and then terminate. Second, when a event arrives
on the wire, a :class:`eva.job.Job` object is created, containing the
`Resource` metadata in the :attr:`job.resource` attribute. We use this
information to retrieve the path of the file that was updated, and create a
shell script that will be executed on a compute node.

.. code-block:: sh
   :caption: Generated shell script

   #!/bin/sh
   sha256sum /path/to/source/file.nc | awk '{print $1}'


Shell code execution
--------------------

Your adapter is not concerned with the actual execution of the shell code. EVA
will handle this part of the job for you, and execute the
:func:`eva.base.adapter.finish_job` when the job has finished (regardless
whether it failed or not.)


Automatically restarting jobs
-----------------------------

EVA supports automatically retrying jobs when they fail. Jobs may fail due to a
excruciating number of reasons, so this is probably something you want. To
enable this functionality, you must raise a
:exc:`eva.exceptions.RetryException` in the :func:`finish_job` function.

.. code-block:: python
   :caption: ``eva/adapters/checksum.py``

   import eva.exceptions

   class ChecksumAdapter(eva.base.adapter.BaseAdapter):
       def finish_job(self, job):
           if not job.complete():
               raise eva.exceptions.RetryException(
                   "This job has to run, at all costs!"
               )


Writing metadata to Productstatus
---------------------------------

After the :func:`finish_job` function has run, and not generating any
exceptions, EVA will call the :func:`eva.base.adapter.generate_resources`
function. This function may populate the :attr:`resources` parameter with
Productstatus resources that should be permanently persisted in the database.

.. code-block:: python
   :caption: ``eva/adapters/checksum.py``

   class ChecksumAdapter(eva.base.adapter.BaseAdapter):
       def generate_resources(self, job, resources):
           job.resource.hash_type = self.env['algorithm']
           job.resource.hash = ''.join(job.stdout).strip()
           resources['datainstance'] += [job.resource]

The :attr:`job.stdout` attribute contains the standard output from the executed
shell script. We modify the original :class:`productstatus.api.Resource` object
with new hash and hash type attributes, and populate the :attr:`resources`
parameter to specify that this resource should be saved back to Productstatus.


.. _Productstatus: https://github.com/metno/productstatus
