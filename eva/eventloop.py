import logging

import eva.job
import eva.executor
import eva.executor.gridengine
import eva.adapter
import eva.checkpoint

import productstatus.event
import productstatus.exceptions


class AdapterJobs(object):
    """
    Helper class for Eventloop, maintains two lists of jobs for an adapter
    """

    def __init__(self, adapter):
        self.adapter = adapter
        self.started_jobs = []
        self.pending_jobs = []

    def add_pending_jobs(self, jobs):
        for job in jobs:
            self.pending_jobs.append(job)

    def get_state(self):
        return (self.adapter.get_state(), self.started_jobs, self.pending_jobs)

    def set_state(self, state):
        if state is None:
            return
        adapter_state, self.started_jobs, self.pending_jobs = state
        self.adapter.set_state(adapter_state)


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
        self.event_listener = event_listener
        self.productstatus_api = productstatus_api
        self.adapter_jobs = [AdapterJobs(a) for a in adapters]
        self.executor = executor
        self.checkpoint = checkpoint
        self.env = environment_variables

    def load_state(self):
        """
        Load state from previous run before starting eventloop().
        Gets eva.job.Job and productstatus.event.Message objects from Checkpoint.
        """
        logging.info("Load persistent checkpoint state.")

        self.executor.set_state(self.checkpoint.get("executor"))
        for aj in self.adapter_jobs:
            aj.set_state(self.checkpoint.get(aj.adapter.id))

    def save_all_adapter_states(self):
        for aj in self.adapter_jobs:
            self.checkpoint.set(aj.adapter.id, aj.get_state())

    def save_adapter_state(self, aj):
        self.checkpoint.set(aj.adapter.id, aj.get_state())

    def start_new_jobs(self):
        self.save_all_adapter_states()
        for aj in self.adapter_jobs:
            for job in aj.pending_jobs:
                self.executor.execute_async(job)
                aj.started_jobs.append(job)
                aj.pending_jobs.remove(job)
                self.checkpoint.set("executor", self.executor.get_state())
                self.save_adapter_state(aj)

    def add_jobs_from_event(self, event):
        """
        @brief Add a Job to the queue if any Adapter classes matches the event
        @param event Productstatus event
        """
        self.checkpoint.set("event", event)

        if event is not None:
            resource = self.productstatus_api[event.uri]
        logging.info('Checking for matching adapters to handle event...')
        for aj in self.adapter_jobs:
            if event is not None:
                jobs = aj.adapter.match_event(event, resource)
            else:
                jobs = aj.adapter.match_timeout()
            aj.add_pending_jobs(jobs)
        logging.info('Finished checking for event adapters.')
        self.checkpoint.set("event", None)
        self.save_all_adapter_states()

        self.start_new_jobs()

    def iterate_event(self):
        """
        @brief Check for Productstatus events, and generate Jobs from them.
        """
        try:
            event = self.event_listener.get_next_event()
            logging.info('Received Productstatus event for resource URI %s' % event.uri)
            self.add_jobs_from_event(event)
        except productstatus.exceptions.EventTimeoutException:
            self.add_jobs_from_event(None)

    def iterate_status(self):
        """
        @brief Run through the job queue. Start any jobs that are not started,
               and check status on jobs that are running.
        """
        for aj in self.adapter_jobs:
            adapter = aj.adapter
            finished_jobs = []
            for job in aj.started_jobs:
                self.executor.update_status(job)
                if job.status in [eva.job.COMPLETE, eva.job.FAILED]:
                    logging.debug("[%s] job finished", job.id)
                    finished_jobs.append(job)
                    aj.started_jobs.remove(job)
                    self.checkpoint.set("executor", self.executor.get_state())
            if len(finished_jobs) > 0:
                aj.add_pending_jobs(adapter.finished_jobs(finished_jobs))
                self.save_adapter_state(aj)

        self.start_new_jobs()

    def __call__(self):
        """
        @brief Main loop. Iterates through event handling and job queue handling.
        """
        logging.info('Starting main loop.')
        while True:
            self.iterate_event()
            self.iterate_status()
