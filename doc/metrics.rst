Metrics
=======

EVA regularly reports metrics to a StatsD server. See the :doc:`configuration`
section for how to set up configuration for StatsD.

.. table:: Metrics reported by EVA

   =======================================  ==============  ==============  ===========
   Metric                                   Type            Tags            Description
   =======================================  ==============  ==============  ===========
   eva_adapter_count                        gauge                           How many adapters that are configured to run, and accepting events.
   eva_event_queue_count                    gauge                           How many events present in the event queue, either pending processing, or having jobs that are being processed.
   eva_job_status_count                     gauge           status          How many jobs that are instantiated.
   eva_zk_*_count                           gauge                           How many items stored as ZooKeeper serialized data, for the specific variable.
   eva_zk_*_size                            gauge                           How many bytes stored in ZooKeeper, for the specific variable.
   eva_deleted_datainstances                counter                         Number of DataInstance resources marked as *deleted* by :class:`~eva.adapter.delete.DeleteAdapter`.
   eva_event_accepted                       counter                         Number of events that resulted in one or more jobs being generated.
   eva_event_duplicate                      counter                         Number of events received that was already present in the event queue at the time of arrival.
   eva_event_heartbeat                      counter                         Number of heartbeat events received.
   eva_event_productstatus                  counter                         Number of Productstatus events received.
   eva_event_received                       counter                         Number of events received, regardless of type.
   eva_event_rejected                       counter         adapter         Number of events that did not result in any job generation.
   eva_event_too_old                        counter                         Number of events that were older than the *message timestamp threshold*.
   eva_event_version_unsupported            counter                         Number of events discarded because of a mismatch in message protocol version.
   eva_job_failures                         counter                         Number of jobs that had some kind of processing failure.
   eva_job_status_change                    counter         status,         Number of status changes for a job belonging to a specific ``adapter``, to the status reported in ``status``.
                                                            adapter
   eva_kafka_commit_failed                  counter                         Number of Kafka message queue position commit errors.
   eva_md5sum_fail                          counter                         Number of files on filesystem that had a mis-match with their md5sum counterpart. Reported by :class:`~eva.adapter.checksum.ChecksumVerificationAdapter`.
   eva_recoverable_exceptions               counter                         Number of times job processing was aborted internally in some way because of a network or other transient error.
   eva_requeue_rejected                     counter         adapter         Number of jobs that was attempted re-queued, but rejected for re-processing by the owning adapter.
   eva_requeued_jobs                        counter         adapter         Number of jobs that was re-queued after a previous failure.
   eva_resource_object_version_too_old      counter                         Number of events that was rejected because the Productstatus resource was updated again before EVA could process the original resource.
   eva_restored_events                      counter
   eva_restored_jobs                        counter
   eva_shutdown                             counter
   eva_start                                counter
   eva_zookeeper_connection_loss            counter
   =======================================  ==============  ==============  ===========
