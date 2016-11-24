Introduction to Event Adapter
=============================

Event Adapter, or *EVA*, is a robust job scheduling system, utilizing
Productstatus_ for event triggering and metadata storage, and Kafka_ for
transporting events over a highly available message queue.

Anatomy of EVA
--------------

EVA is structured as three main, logical parts. These parts are:

  * Events

    Events are small, chronographically ordered messages, that ultimately
    instructs EVA to execute specific tasks. Currently, only messages coming
    directly from Productstatus_ make sense to EVA.

  * Adapters

    Adapters are Python classes that receives events, and creates jobs that
    should be processed on an Executor. They are configured to accept
    Productstatus_ events with specific properties, such as a specific input
    file format or a specific *Product*.

  * Executors

    Executors are Python classes that provide an asynchronous interface to job
    processing. Executors make sure that shell commands run on the correct
    infrastructure, and that the exit code, standard output, and standard error
    is available for log output and further processing.


.. _Productstatus: https://github.com/metno/productstatus
.. _Kafka: https://kafka.apache.org/
