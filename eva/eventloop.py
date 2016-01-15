import logging

import eva.job
import eva.executor
import eva.executor.gridengine
import eva.adapter
import eva.checkpoint

import productstatus.event
import productstatus.exceptions


class Eventloop(object):
    """
    The main loop.
    """

    def __init__(self,
                 productstatus_api,
                 event_listener,
                 adapters,
                 executor,
                 checkpoint,
                 environment_variables,
                 ):
        self.jobs = []
        self.event_listener = event_listener
        self.productstatus_api = productstatus_api
        self.adapters = adapters
        self.executor = executor
        self.checkpoint = checkpoint
        self.env = environment_variables

    def load_state(self):
        """
        Load state from previous run before starting eventloop().
        Gets eva.job.Job and productstatus.event.Message objects from Checkpoint.
        """
        logging.info("Load persistent checkpoint state.")
        objects = self.checkpoint.load()

        for object_ in objects:
            if isinstance(object_, eva.job.Job):
                logging.info('[%s] loading job from checkpoint.' % object.id)
                self.jobs += object_
            if isinstance(object, productstatus.event.Message):
                logging.info('Loading event from checkpoint: %s' % object.uri)
                self.add_jobs_from_event(object_)

    def add_jobs_from_event(self, event):
        """
        @brief Add a Job to the queue if any Adapter classes matches the event
        @param event Productstatus event
        """
        resource = self.productstatus_api[event.uri]
        logging.info('Checking for matching adapters to handle event...')
        for adapter in self.adapters:
            job = adapter.match(event, resource)
            if not job:
                continue
            logging.info('Adapter %s matches event, adding new job [%s] to queue.' % (job.adapter.__class__, job.id))
            self.checkpoint.set(job)
            self.jobs += [job]

        logging.info('Finished checking for event adapters.')
        self.checkpoint.delete(event)

    def iterate_get_event_add_jobs(self):
        """
        @brief Check for Productstatus events, and generate Jobs from them.
        """
        try:
            event = self.event_listener.get_next_event()
            self.checkpoint.set(event)
            logging.info('Received Productstatus event for resource URI %s' % event.uri)
            self.add_jobs_from_event(event)
        except productstatus.exceptions.EventTimeoutException:
            pass

    def iterate_job_execution(self):
        """
        @brief Run through the job queue. Start any jobs that are not started,
               and check status on jobs that are running.
        """
        job_count = len(self.jobs)
        if not job_count:
            return
        logging.info('Job queue has %d jobs; executing and checking status...' % job_count)
        for job in self.jobs:
            if job.status == eva.job.PREPARED:
                self.executor.execute_async(job)
            elif job.status == eva.job.STARTED:
                self.executor.update_status(job)

        logging.info('Finished executing and checking jobs.')

    def iterate_job_finish(self):
        """
        @brief Run through the job queue. Run the finish routine on any jobs
               that have completed or failed.
        """
        job_count = len(self.jobs)
        if not job_count:
            return
        logging.info('Job queue has %d jobs; checking for completed and failed jobs...' % job_count)
        for job in self.jobs:
            if job.status in [eva.job.COMPLETE, eva.job.FAILED]:
                job.adapter.finish(job)
                logging.info('[%s] Removing job from queue.', job.id)
                self.checkpoint.delete(job)
                self.jobs.remove(job)

        logging.info('Finished checking jobs.')

    def __call__(self):
        """
        @brief Main loop. Iterates through event handling and job queue handling.
        """
        logging.info('Starting main loop.')
        while True:
            self.iterate_get_event_add_jobs()
            self.iterate_job_execution()
            self.iterate_job_finish()
