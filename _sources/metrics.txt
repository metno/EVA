Metrics
=======

EVA regularly reports metrics to a StatsD server. See the :doc:`configuration`
section for how to set up configuration for StatsD.

.. table::

   =======================================  ==============  ======================  ===========
   Metric                                   Type            Tags                    Description
   =======================================  ==============  ======================  ===========
   eva_adapter_count                        gauge                                   Adapters that are configured to run, and accepting events.
   eva_event_queue_count                    gauge                                   Events present in the event queue, either pending processing, or having jobs that are being processed.
   eva_job_status_count                     gauge           status                  Jobs that are instantiated.
   eva_zk_*_count                           gauge                                   Items stored as ZooKeeper serialized data, for the specific variable.
   eva_zk_*_size                            gauge                                   Bytes stored in ZooKeeper, for the specific variable.
   eva_deleted_datainstances                counter                                 DataInstance resources marked as *deleted* by :class:`~eva.adapter.delete.DeleteAdapter`.
   eva_event_accepted                       counter                                 Events that resulted in one or more jobs being generated.
   eva_event_duplicate                      counter                                 Events received that was already present in the event queue at the time of arrival.
   eva_event_expired                        counter                                 Productstatus *expired* events, delivering a list of DataInstance resources that needs deletion.
   eva_event_heartbeat                      counter                                 Heartbeat events received.
   eva_event_productstatus                  counter                                 Productstatus events received.
   eva_event_received                       counter                                 Events received, regardless of type.
   eva_event_rejected                       counter         adapter                 Events that did not result in any job generation.
   eva_event_too_old                        counter                                 Events that were older than the *message timestamp threshold*.
   eva_event_version_unsupported            counter                                 Events discarded because of a mismatch in message protocol version.
   eva_grid_engine_run_time                 timing          adapter,                How long it took the executor to run a job from a specific adapter.
                                                            grid_engine_qname,
                                                            grid_engine_hostname
   eva_grid_engine_qsub_delay               timing          adapter,                How long a submitted Grid Engine job had to wait to be run, from the moment qsub was called.
                                                            grid_engine_qname,
                                                            grid_engine_hostname
   eva_grid_engine_ru_stime                 timing          adapter,                Amount of system CPU time consumed by a job of the specific adapter.
                                                            grid_engine_qname,
                                                            grid_engine_hostname
   eva_grid_engine_ru_utime                 timing          adapter,                Amount of userland CPU time consumed by a job of the specific adapter.
                                                            grid_engine_qname,
                                                            grid_engine_hostname
   eva_job_failures                         counter                                 Jobs that had some kind of processing failure.
   eva_job_status_change                    counter         status,                 Status changes for a job belonging to a specific ``adapter``, to the status reported in ``status``.
                                                            adapter
   eva_kafka_commit_failed                  counter                                 Kafka message queue position commit errors.
   eva_kafka_no_brokers_available           counter                                 Connection loss against all Kafka message queue brokers.
   eva_md5sum_fail                          counter                                 Files on filesystem that had a mis-match with their md5sum counterpart. Reported by :class:`~eva.adapter.checksum.ChecksumVerificationAdapter`.
   eva_recoverable_exceptions               counter                                 Times job processing was aborted internally in some way because of a network or other transient error.
   eva_requeue_rejected                     counter         adapter                 Jobs that was attempted re-queued, but rejected for re-processing by the owning adapter.
   eva_requeued_jobs                        counter         adapter                 Jobs that was re-queued after a previous failure.
   eva_resource_object_version_too_old      counter                                 Events that was rejected because the Productstatus resource was updated again before EVA could process the original resource.
   eva_restored_corrupt                     counter                                 Events that was attempted restored from ZooKeeper cache, but were corrupt.
   eva_restored_events                      counter                                 Events restored from the ZooKeeper cache upon starting EVA from a crashed state.
   eva_restored_jobs                        counter                                 Jobs restored from the ZooKeeper cache upon starting EVA from a crashed state.
   eva_shutdown                             counter                                 EVA program shutdowns.
   eva_start                                counter                                 EVA program startups.
   eva_zookeeper_connection_loss            counter                                 Times the ZooKeeper connection was irrecoverably lost.
   =======================================  ==============  ======================  ===========
