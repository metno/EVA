import eva.job
import eva.executor
import eva.adapter

import productstatus.event
import productstatus.exceptions


class Eventloop(object):
    """
    The main loop.
    """

    def __init__(self, productstatus_api, event_listener, adapters, executor):
        self.jobs = []
        self.event_listener = event_listener
        self.productstatus_api = productstatus_api
        self.adapters = adapters
        self.executor = executor

    def add_jobs_from_event(self, event):
        """
        @brief Add a Job to the queue if any Adapter classes matches the event
        @param event Productstatus event
        """
        for adapter in self.adapters:
            resource = self.productstatus_api[event.uri]
            job = adapter.match(event, resource)
            if not job:
                self.jobs += [job]

    def iterate_get_event_add_jobs(self):
        """
        @brief Check for Productstatus events, and generate Jobs from them.
        """
        try:
            event = self.event_listener.get_next_event()
            self.add_jobs_from_event(event)
        except productstatus.exceptions.EventTimeoutException:
            pass

    def iterate_job_execution(self):
        """
        @brief Run through the job queue. Start any jobs that are not started,
               and check status on jobs that are running.
        """
        for job in self.jobs:
            if job.status == eva.job.PREPARED:
                self.executor.execute_async(job)
            elif job.status == eva.job.STARTED:
                self.executor.update_status(job)

    def iterate_job_finish(self):
        """
        @brief Run through the job queue. Run the finish routine on any jobs
               that have completed or failed.
        """
        for job in self.jobs:
            if job.status in [eva.job.COMPLETE, eva.job.FAILED]:
                job.adapter.finish(job)

    def __call__(self):
        """
        @brief Main loop. Iterates through event handling and job queue handling.
        """
        while True:
            self.iterate_get_event_add_jobs()
            self.iterate_job_execution()
            self.iterate_job_finish()
