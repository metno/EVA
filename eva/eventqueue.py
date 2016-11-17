import collections
import os

import eva.event
import eva.exceptions
import eva.globe
import eva.job
import eva.zk


class EventQueueItem(object):
    """!
    @brief Manages jobs for a specific event.

    This class is a wrapper around the eva.event.Event object, maintaining an
    ordered list of eva.job.Job objects. The class can be iterated to retrieve
    a list of the job objects.
    """
    def __init__(self, event):
        # Dictionary of Job objects, indexed by the adapter configuration key.
        self.jobs = collections.OrderedDict()
        assert isinstance(event, eva.event.Event)
        self.event = event

    def id(self):
        return self.event.id()

    def add_job(self, job):
        assert isinstance(job, eva.job.Job)
        self.jobs[job.id] = job

    def remove_job(self, job_id):
        del self.jobs[job_id]

    def empty(self):
        return len(self.jobs) == 0

    def job_keys(self):
        return list(self.jobs.keys())

    def failed_jobs(self):
        return [job for key, job in self.jobs.items() if job.failed()]

    def finished(self):
        if len(self.jobs) == 0:
            raise RuntimeError('finished() should not be called on an EventQueueItem with zero jobs')
        for job in self:
            if not job.finished():
                return False
        return True

    def serialize(self):
        serialized = {}
        serialized['message'] = self.event.message
        serialized['job_keys'] = self.job_keys()
        serialized['jobs'] = {}
        for key, job in self.jobs.items():
            serialized['jobs'][key] = {
                'status': job.status,
                'adapter': job.adapter.config_id,
            }
        return serialized

    def __iter__(self):
        return iter(self.jobs.values())

    def __len__(self):
        return len(self.jobs)

    def __repr__(self):
        return '<EventQueueItem: event.id=%s>' % self.id()


class EventQueue(eva.globe.GlobalMixin):
    """!
    @brief Manages events under processing in a queue mirrored to ZooKeeper.

    ZooKeeper paths:

    * /events
        A list of event IDs referring to individual ZooKeeper paths. This list
        exists only in order to detect which events are in the processing list.
    * /events/<EVENT_UUID>
        A list of jobs IDs, referring to jobs that have been initialized from
        this particular event. This list exists only in order to detect which
        jobs are in the processing list.
    * /events/<EVENT_UUID>/message
        The raw message string for an event, as received on the wire. Used for
        reconstructing event objects in case of a program crash or adapter
        failure.
    * /events/<EVENT_UUID>/<JOB_UUID>/status
        The job status of this job.
    * /events/<EVENT_UUID>/<JOB_UUID>/adapter
        The adapter that owns this job.
    """

    def init(self):
        # Dictionary of EventQueueItem objects, indexed by the event id.
        self.items = collections.OrderedDict()
        self.zk_base_path = os.path.join(self.zookeeper.EVA_BASE_PATH, 'events')
        self.zookeeper.ensure_path(self.zk_base_path)

    def add_event(self, event):
        assert isinstance(event, eva.event.Event)
        id = event.id()
        if id in self.items:
            raise eva.exceptions.DuplicateEventException('Event %s already exists in the event queue.', id)
        item = EventQueueItem(event)
        self.items[id] = item
        self.store_item(item)
        self.logger.debug('Event added to event queue: %s', event)
        return item
        #try:
        #except eva.exceptions.ZooKeeperDataTooLargeException as e:
            #self.logger.warning(str(e))
            #return False
        #except kazoo.exceptions.ZooKeeperError as e:
            #self.logger.warning(str(e))
            #return False
        return item

    def active_jobs_in_adapter(self, adapter):
        active = 0
        for item in self:
            for job in item:
                if job.adapter != adapter:
                    continue
                if not job.started():
                    continue
                active += 1
        return active

    def status_count(self):
        """!
        @brief Return a hash with status codes and the total number of jobs in
        the event queue having that specific status.
        """
        status_map = dict(zip(eva.job.ALL_STATUSES, [0] * len(eva.job.ALL_STATUSES)))
        for item in self:
            for job in item:
                status_map[job.status] += 1
        return status_map

    def remove_item(self, item):
        assert isinstance(item, EventQueueItem)
        id = item.id()
        assert id in self.items
        text = 'Event removed from event queue: %s' % item.event
        del self.items[id]
        self.delete_stored_item(id)
        self.logger.debug(text)

    def item_keys(self):
        return list(self.items.keys())

    def store_list(self):
        self.store_serialized_data(self.zk_base_path, self.item_keys(), metric_base='event_queue')

    def store_item(self, item):
        assert isinstance(item, EventQueueItem)
        base_path = os.path.join(self.zk_base_path, item.id())
        self.zookeeper.ensure_path(base_path)
        serialized = item.serialize()
        self.store_serialized_data(os.path.join(base_path, 'message'), serialized['message'])
        self.store_serialized_data(os.path.join(base_path, 'jobs'), serialized['job_keys'])
        for key, job in serialized['jobs'].items():
            path = os.path.join(base_path, 'jobs', key)
            self.zookeeper.ensure_path(path)
            self.store_serialized_data(os.path.join(path, 'adapter'), job['adapter'])
            self.store_serialized_data(os.path.join(path, 'status'), job['status'])
        self.store_list()

    def delete_stored_item(self, item_id):
        assert isinstance(item_id, str)
        path = os.path.join(self.zk_base_path, item_id)
        self.zookeeper.delete(path, recursive=True)
        self.logger.debug('Recursively deleted ZooKeeper path: %s', path)
        self.store_list()

    def store_serialized_data(self, path, data, metric_base=None):
        """!
        @brief Store structured data in ZooKeeper.
        @throws kazoo.exceptions.ZooKeeperError on failure
        """
        count, size = eva.zk.store_serialized_data(self.zookeeper, path, data)
        self.logger.debug('Stored %d items of total %d bytes at ZooKeeper path %s', count, size, path)
        if not metric_base:
            return
        self.statsd.gauge('eva_' + metric_base + '_count', count)
        self.statsd.gauge('eva_' + metric_base + '_size', size)

    def empty(self):
        """!
        @brief Returns True if the event queue list is empty.
        """
        return len(self.items) == 0

    def __iter__(self):
        return iter(self.items.values())

    def __len__(self):
        return len(self.items)
